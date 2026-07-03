#!/usr/bin/env bash
# Rebuild the docs-drift eval fixture worktrees from current origin/main.
#
# Each eval in evals.json runs against a git worktree with a planted
# change (see fixtures/*.patch). Fixtures 0 and 1 commit the change on
# an eval/ branch; fixture 2 leaves it uncommitted on purpose — that
# eval exists to prove working-tree-only changes count as "in flight".
#
# Patches apply onto *moving* main by design: the skill's subject is
# drift against the current repo, so the evals should track it. If a
# refactor breaks application, `git apply` fails loudly here — refresh
# the patch against the new code and re-run.
#
# Usage: setup-fixtures.sh [dest-dir]   (default: /tmp/artel-eval)
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
FIXTURES="$REPO_ROOT/.claude/skills/docs-drift/evals/fixtures"
DEST=${1:-/tmp/artel-eval}

cd "$REPO_ROOT"
git fetch origin main --quiet

make_fixture() {
    local name=$1 patch=$2 commit_msg=$3   # empty msg => leave uncommitted
    local dir="$DEST/$name" branch="eval/$name"
    if [ -e "$dir" ]; then
        git worktree remove --force "$dir" 2>/dev/null || rm -rf "$dir"
    fi
    git branch -D "$branch" 2>/dev/null || true
    git worktree add --quiet -b "$branch" "$dir" origin/main
    git -C "$dir" apply --verbose "$FIXTURES/$patch"
    if [ -n "$commit_msg" ]; then
        git -C "$dir" add -A
        git -C "$dir" commit --quiet -m "$commit_msg"
    fi
    echo "fixture ready: $dir"
}

make_fixture drift-variant 0-new-event-variant.patch \
    "feat(artel-fs): emit ScanCompleted when the initial bulk sweep finishes"
make_fixture clean-tests 1-clean-test-only.patch \
    "test(artel-fs): cover uppercase .SWP non-skip and deep-nested hardcoded components"
make_fixture rustdoc-stale 2-uncommitted-rustdoc.patch ""

echo "all fixtures ready under $DEST"
