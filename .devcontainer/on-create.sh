#!/usr/bin/env bash
# Runs INSIDE the container once, at create time (onCreateCommand).
set -euo pipefail

WS="${1:?usage: on-create.sh <container-workspace-folder>}"

# The named volumes are created root-owned by Docker; hand them to
# the vscode user. Each independently — one missing mount must not
# abort the seeding below.
for vol in /usr/local/cargo/registry "$WS/target" "$HOME/.claude"; do
  [ -d "$vol" ] && sudo chown -R vscode:vscode "$vol"
done

# Seed the container's Claude setup from the host snapshot staged by
# host-init.sh (mounted read-only at /opt/host). The container's
# ~/.claude is a named volume, deliberately not a bind mount of the
# host's — copy-in/never-out keeps permission-bypassed agent sessions
# from reaching host config (see host-init.sh). Seeding only when the
# volume is fresh preserves container-side state (trust answers,
# memory, login credentials) across container rebuilds; to re-seed
# from the host, remove the volume and recreate the container.
CFG="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"
if [ ! -e "$CFG/.seeded" ]; then
  if [ -d /opt/host/claude-config ]; then
    cp -R /opt/host/claude-config/. "$CFG/"
    echo "on-create: seeded Claude config from host snapshot" >&2
  fi
  if [ ! -f "$CFG/.claude.json" ] && [ -f /opt/host/claude-state.json ]; then
    cp /opt/host/claude-state.json "$CFG/.claude.json"
    echo "on-create: seeded Claude CLI state from host" >&2
  fi
  date -u +"%Y-%m-%dT%H:%M:%SZ" > "$CFG/.seeded"
fi
