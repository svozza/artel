#!/usr/bin/env bash
# Host-side staging for the devcontainer (wired as initializeCommand,
# so it runs on the HOST before every container create/start).
#
# Two jobs, both writing into .devcontainer/.local/ (gitignored, and
# self-ignored via the .gitignore written below), which is
# bind-mounted into the container at /opt/host:
#
# 1. AWS credentials, if the developer routes Claude through an AWS
#    profile (e.g. Bedrock). The profile name is read from the `env`
#    block of the developer's own ~/.claude/settings.json
#    (AWS_PROFILE), overridable via CLAUDE_BEDROCK_PROFILE. Host
#    profiles often resolve credentials through host-only tooling
#    (credential_process binaries, SSO helpers) that cannot run
#    inside the Linux container — so we export short-lived static
#    credentials here and give the container an AWS config whose
#    credential_process is just `cat` of the exported JSON. The JSON
#    carries an Expiration timestamp; AWS SDKs re-invoke the process
#    when it lapses, so re-running this script on the host refreshes
#    a LIVE container — no restart:
#
#        bash .devcontainer/host-init.sh
#
# 2. A one-time seed copy of ~/.claude.json (CLI state: onboarding,
#    project trust, user-scoped MCP servers). The container keeps its
#    own copy under the mounted ~/.claude (via CLAUDE_CONFIG_DIR) so
#    host and container never fight over the same file.
#
# Everything is best-effort: contributors with no AWS profile in
# their Claude settings, no AWS CLI, or no Claude at all still get a
# working container — the relevant step is just skipped.

set -uo pipefail

STAGE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.local"
mkdir -p "$STAGE/aws"

# Never let staged credentials become committable, even if the
# repo-level .gitignore entry is ever lost.
printf '*\n' > "$STAGE/.gitignore"

# Guarantee bind-mount sources exist so container start never fails
# on a missing host path.
mkdir -p "$HOME/.claude"

# Resolve the AWS profile: explicit override first, then the env
# block of the developer's own Claude settings.
PROFILE="${CLAUDE_BEDROCK_PROFILE:-}"
REGION="${AWS_REGION:-}"
SETTINGS="$HOME/.claude/settings.json"
if [ -z "$PROFILE" ] && [ -f "$SETTINGS" ] && command -v python3 > /dev/null 2>&1; then
  PROFILE="$(python3 -c "
import json, sys
try:
    env = json.load(open('$SETTINGS')).get('env', {})
except Exception:
    sys.exit(0)
print(env.get('AWS_PROFILE', ''))
" 2> /dev/null)"
  if [ -z "$REGION" ]; then
    REGION="$(python3 -c "
import json, sys
try:
    env = json.load(open('$SETTINGS')).get('env', {})
except Exception:
    sys.exit(0)
print(env.get('AWS_REGION', ''))
" 2> /dev/null)"
  fi
fi

if [ -n "$PROFILE" ] && command -v aws > /dev/null 2>&1 \
    && aws configure list-profiles 2> /dev/null | grep -qx "$PROFILE"; then
  if aws configure export-credentials --profile "$PROFILE" --format process \
      > "$STAGE/aws/bedrock-creds.json.tmp" 2> /dev/null; then
    mv "$STAGE/aws/bedrock-creds.json.tmp" "$STAGE/aws/bedrock-creds.json"
    chmod 600 "$STAGE/aws/bedrock-creds.json"
    cat > "$STAGE/aws/config" << EOF
[profile $PROFILE]
${REGION:+region = $REGION
}output = json
credential_process = cat /opt/host/aws/bedrock-creds.json
EOF
    echo "host-init: exported credentials for profile '$PROFILE'" >&2
  else
    rm -f "$STAGE/aws/bedrock-creds.json.tmp"
    echo "host-init: WARNING: export-credentials failed for '$PROFILE'" >&2
    echo "host-init: (auth expired? re-auth on the host, then rerun this script)" >&2
  fi
else
  echo "host-init: no AWS profile configured; skipping credential export" >&2
fi

if [ -f "$HOME/.claude.json" ]; then
  cp "$HOME/.claude.json" "$STAGE/claude-state.json"
fi

exit 0
