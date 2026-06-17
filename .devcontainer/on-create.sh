#!/usr/bin/env bash
# Runs INSIDE the container at create time (onCreateCommand). Re-runs
# whenever the container is recreated (the named volumes persist; this
# script does not).
set -euo pipefail

WS="${1:?usage: on-create.sh <container-workspace-folder>}"

# The named volumes are created root-owned by Docker; hand them to
# the vscode user. Each independently — one missing mount must not
# abort the seeding below.
for vol in /usr/local/cargo/registry "$WS/target" "$HOME/.claude"; do
  [ -d "$vol" ] && sudo chown -R vscode:vscode "$vol"
done

CFG="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"

# The container's ~/.claude is a named volume, deliberately not a bind
# mount of the host's — copy-in/never-out keeps permission-bypassed
# agent sessions from reaching host config (see host-init.sh). Two
# classes of content, synced differently:
#
# 1. Host-authoritative config (skills, agents, settings, ...): the
#    host is the source of truth, so RE-SYNC every (re)create. This is
#    what lets a skill you add on the host show up after a container
#    rebuild. rm+cp per item so host-side deletions propagate too;
#    container-side edits to these are intentionally discarded.
# 2. Container-private state (CLI state, project memory): SEED ONCE,
#    so trust answers / logins / memory written inside survive a
#    rebuild. Gated on the .seeded marker.
if [ -d /opt/host/claude-config ]; then
  for item in CLAUDE.md settings.json keybindings.json agents skills statusline; do
    src="/opt/host/claude-config/$item"
    [ -e "$src" ] || continue
    rm -rf "${CFG:?}/$item"
    cp -R "$src" "$CFG/$item"
  done
  echo "on-create: re-synced host-authoritative Claude config" >&2
fi

if [ ! -e "$CFG/.seeded" ]; then
  # Plugins carry a container-written cache, and project memory +
  # CLI state are container-private — seed them only on a fresh volume.
  if [ -e /opt/host/claude-config/plugins ]; then
    cp -R /opt/host/claude-config/plugins "$CFG/plugins"
  fi
  if [ -d /opt/host/claude-config/projects ]; then
    cp -R /opt/host/claude-config/projects "$CFG/projects"
  fi
  if [ ! -f "$CFG/.claude.json" ] && [ -f /opt/host/claude-state.json ]; then
    cp /opt/host/claude-state.json "$CFG/.claude.json"
  fi
  date -u +"%Y-%m-%dT%H:%M:%SZ" > "$CFG/.seeded"
  echo "on-create: seeded container-private state (fresh volume)" >&2
fi
