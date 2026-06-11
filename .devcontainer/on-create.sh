#!/usr/bin/env bash
# Runs INSIDE the container once, at create time (onCreateCommand).
set -euo pipefail

WS="${1:?usage: on-create.sh <container-workspace-folder>}"

# The named target/registry volumes are created root-owned by Docker;
# hand them to the vscode user.
sudo chown -R vscode:vscode /usr/local/cargo/registry "$WS/target"

# Seed the container's Claude CLI state from the host's ~/.claude.json
# (staged by host-init.sh) if this is a fresh config dir. Done once —
# after that the container's copy evolves independently inside the
# mounted ~/.claude, so it survives container rebuilds.
CFG="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"
if [ ! -f "$CFG/.claude.json" ] && [ -f /opt/host/claude-state.json ]; then
  cp /opt/host/claude-state.json "$CFG/.claude.json"
  echo "on-create: seeded Claude CLI state from host" >&2
fi
