#!/usr/bin/env bash
# Run the docs-drift eval suite end to end:
#   1. rebuild fixtures from origin/main (setup-fixtures.sh)
#   2. per eval: run `claude -p` headless with the skill against the fixture
#   3. per eval: grade the report against evals.json assertions with a
#      second `claude -p` judge call
#   4. print a pass/fail summary; non-zero exit if any assertion failed
#
# Each headless run reads SKILL.md and follows it — the same mechanics
# a future CI job would use. Results land under results/<timestamp>/
# (gitignored).
#
# Usage: run-evals.sh [--model <model>]   (default: session default model)
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
EVALS_DIR="$REPO_ROOT/.claude/skills/docs-drift/evals"
SKILL_MD="$REPO_ROOT/.claude/skills/docs-drift/SKILL.md"
FIXDEST=/tmp/artel-eval
# ${MODEL_ARGS[@]+...} guards at the call sites: macOS bash 3.2 treats
# expanding an empty array under `set -u` as an unbound-variable error.
MODEL_ARGS=()
[ "${1:-}" = "--model" ] && MODEL_ARGS=(--model "$2")

STAMP=$(date +%Y%m%d-%H%M%S)
RESULTS="$EVALS_DIR/results/$STAMP"
mkdir -p "$RESULTS"

"$EVALS_DIR/setup-fixtures.sh" "$FIXDEST"

# eval id -> fixture dir (order matches evals.json ids)
FIXTURE_DIRS=(drift-variant clean-tests rustdoc-stale)

FAILED=0
COUNT=$(jq '.evals | length' "$EVALS_DIR/evals.json")

for i in $(seq 0 $((COUNT - 1))); do
    NAME=$(jq -r ".evals[$i].eval_name" "$EVALS_DIR/evals.json")
    PROMPT=$(jq -r ".evals[$i].prompt" "$EVALS_DIR/evals.json")
    FIXTURE="$FIXDEST/${FIXTURE_DIRS[$i]}"
    OUT="$RESULTS/$NAME"
    mkdir -p "$OUT"
    echo "=== eval $i: $NAME"

    # Run the skill headless against the fixture. --add-dir grants the
    # fixture worktree; cwd is the fixture so git commands hit it.
    (cd "$FIXTURE" && claude -p \
        "Read and follow the skill instructions in $SKILL_MD exactly, \
as if the user had invoked /docs-drift. The user's request: \"$PROMPT\" \
Work only in this directory. Do not edit any files — report only." \
        ${MODEL_ARGS[@]+"${MODEL_ARGS[@]}"} \
        --allowedTools "Bash(git *) Bash(grep *) Bash(rg *) Read Glob Grep" \
        > "$OUT/report.md" 2> "$OUT/stderr.log") || {
        echo "  RUN FAILED (see $OUT/stderr.log)"; FAILED=1; continue; }

    # Grade: a judge call scores the report against the assertions.
    jq -c ".evals[$i].assertions" "$EVALS_DIR/evals.json" > "$OUT/assertions.json"
    claude -p \
        "You are grading an automated docs-audit report against assertions. \
Assertions (JSON array): $(cat "$OUT/assertions.json") \
Report to grade follows between markers. Judge each assertion strictly \
against the report text only. Output ONLY a JSON array, one object per \
assertion, shape {\"text\": ..., \"passed\": true|false, \"evidence\": \
\"<quote or reason>\"}. No prose outside the JSON. \
---REPORT--- $(cat "$OUT/report.md") ---END---" \
        ${MODEL_ARGS[@]+"${MODEL_ARGS[@]}"} --allowedTools "" \
        > "$OUT/grading.raw" 2>> "$OUT/stderr.log" || {
        echo "  GRADING FAILED"; FAILED=1; continue; }
    # The judge sometimes wraps its output in a ```json fence despite
    # instructions; strip any fence lines before parsing.
    sed '/^```/d' "$OUT/grading.raw" > "$OUT/grading.json"

    PASS=$(jq '[.[] | select(.passed)] | length' "$OUT/grading.json")
    TOTAL=$(jq 'length' "$OUT/grading.json")
    echo "  $PASS/$TOTAL assertions passed"
    if [ "$PASS" != "$TOTAL" ]; then
        FAILED=1
        jq -r '.[] | select(.passed | not) | "  FAIL: \(.text)\n        \(.evidence)"' \
            "$OUT/grading.json"
    fi
done

echo
echo "results: $RESULTS"
[ "$FAILED" = 0 ] && echo "ALL EVALS PASSED" || echo "SOME EVALS FAILED"
exit "$FAILED"
