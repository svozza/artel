# Plan: artel-daemon adversarial-review follow-ups

date: 2026-07-15
status: DONE (2026-07-16) — all seven findings fixed and merged to main:
  - #2 gossip frame-size mismatch → PR #25 (`0c42b47`)
  - #5 joiner epoch watermark ordering → PR #26 (`cf775cc`)
  - #3 unbounded task spawn (DoS) → PR #27 (`2ef3df7`)
  - #8 shutdown hang on stalled IPC client → PR #28 (`234fc16`)
  - #4 concurrent HostSession creation race → PR #29 (`377f332`)
  - #6 concurrent-join rollback race → PR #30 (`176bd0e`)
  - #7 send() TOCTOU → PR #31 (`ece609b`)
relates-to:
  - `docs/roadmap.md` ("Gossip-lurker capability leak CLOSED" entry — tracks findings #9/#10 below, already on the roadmap, not duplicated here)
  - PR #24 (fix/session-id-reuse-replay) — closed finding #1, the highest-severity item from the same review

## Context

An adversarial code review of `artel-daemon` (via `codex`, cross-verified by hand
against the actual source) produced 10 findings. Finding #1
(session-ID-reuse `SessionClosed` replay) was the most severe and has been
fixed and merged (PR #24, commit `acc6444`). Findings #9 and #10 (the
`delivered_from`-keyed `Replay` gate, and live gossip chatter staying visible
to topic lurkers) were found to already be tracked in `docs/roadmap.md` under
the "Gossip-lurker capability leak CLOSED" entry, with an explicit "MUST
become a signed, B.5-style requester identity... as part of the P2P
causal-DAG rethink" note — no new action needed there beyond what's already
planned.

This doc tracks the remaining 7 findings (#2–#8), ranked by severity/fix
priority as originally assessed. Each entry gives the current file:line,
the concrete failure scenario, and a candidate fix direction — but fix
approach has NOT been through a design pass yet for any of these (unlike
finding #1, which had an explicit user sign-off on "full fix" scope before
implementation started).

## Findings

### #2 — Gossip frame-size mismatch (silent data loss, no attacker required)

**Where:** `crates/artel-daemon/src/server.rs:1351` (`Gossip::builder().spawn(...)`
with no `.max_message_size()` override) vs. `crates/artel-daemon/src/store/fs.rs:50`
(`MAX_FRAME_SIZE = 16 * 1024 * 1024`).

**Problem:** iroh-gossip's default `max_message_size` is 4096 bytes
(`iroh_gossip::proto::DEFAULT_MAX_MESSAGE_SIZE`), confirmed by reading the
iroh-gossip 0.101.0 source directly. The daemon's store and IPC transport both
accept payloads up to 16 MiB. A host `send()` with a payload of a few KB
persists locally and IPC-acks success to the caller, then the gossip broadcast
silently fails somewhere inside iroh-gossip's connection loop —
`GossipBridge::publish_message`'s `Result` only covers the actor-channel
handoff, not the eventual wire write. No error path in `artel-daemon` ever
surfaces this. Result: permanent, silent host/mirror divergence with no
attacker involved — an ordinary chat message over ~4KB triggers it.

**Fix direction:** either raise `.max_message_size()` on the `Gossip::builder()`
call to match `MAX_FRAME_SIZE` (bytes cost scales with mesh fanout, so this
isn't free), or clamp the app-level payload size accepted at the IPC/store
boundary to whatever gossip can actually carry, with a clear
`ProtocolError` at send time rather than silent loss. Needs a decision on
which constant is the real ceiling — probably the smaller, gossip-side one,
since raising iroh-gossip's frame size has network-wide cost per broadcast.

**Severity:** high-priority to fix next — no attacker required, silent, and
easy to trigger by accident.

---

### #3 — Unbounded task spawn before signature verification (DoS)

**Where:** `crates/artel-daemon/src/session.rs:1316-1333` (`mirror_on_message`),
feeding into `apply_inbound_mirror_message` (dedup → author-sig → host-seq-sig,
in that order, inside the spawned task).

**Problem:** every inbound `GossipBody::Message` frame on a joiner's mirror
spawns a fresh `tokio::spawn` task *before* any verification runs — dedup,
author signature check, and host seq-sig check all happen inside the spawned
task, not before spawning it. Combined with finding #9 (topic subscription is
unauthenticated — already tracked on the roadmap), any peer that can derive
the topic ID (deterministic from session id) can flood unique-payload frames
to spawn unbounded tasks, each retaining a full `SessionMessage` until its
task runs. Bounded per-frame by iroh-gossip's message-size cap (see #2), but
unbounded in aggregate task/memory count.

**Fix direction:** move dedup (cheap, no crypto) to run synchronously in the
gossip forwarder loop, before spawning — a duplicate seq should never spawn a
task at all. Signature verification is heavier; consider a bounded
worker-pool/semaphore for the spawned verification tasks so an attacker can
inflate queue depth but not unbounded concurrent task count.

**Severity:** medium — real DoS vector, but requires network access to a
session's topic id, which is deterministic but not broadcast outside ticket
holders.

---

### #4 — Concurrent `HostSession(Some(id))` creation isn't atomic

**Where:** `crates/artel-daemon/src/session.rs`, the create path starting at
`// Create path. Either no...` (~line 1004) inside `Registry::host`.

**Problem:** the create path does read-check (session not in `self.sessions`)
→ `await self.store.epoch_floor(...)` → `await self.store.create(...)` →
write-insert into `self.sessions`, with no lock held across the whole
sequence. Two racing `HostSession(Some(same_new_id))` calls can both pass the
initial check, both build independent `Session`/ticket-ledger objects, and the
losing caller's in-memory state and just-minted ticket get silently discarded
by the final `HashMap::insert`'s overwrite — while the two `store.create()`
calls (writing `meta.json`, `tickets.json` for the same session dir) can
interleave on disk.

**Fix direction:** `claim_remote_mirror` (same file, `~line 1589`) already
solves the equivalent race for the joiner side — read-check and write-insert
share one lock hold, with a "loser hands back the winner's arc" contract. The
host-create path needs the same shape: claim the registry slot under one lock
hold before doing the async store write, and detect-and-reuse on a losing
race rather than silently overwriting.

**Severity:** medium-low — narrow window, same-daemon-only race, but a real
correctness bug (ticket loss, possible disk corruption on the ledger file).

---

### #5 — Epoch-persistence ordering on the joiner side can regress the watermark after restart

**Where:** `crates/artel-daemon/src/session.rs:2213-2242`
(`advance_host_epoch_watermark`), called from the `EpochBeacon` arm in
`crates/artel-daemon/src/gossip_bridge.rs`.

**Problem:** the joiner's live in-memory watermark (`AtomicU64`, via
`fetch_max`) advances *before* the call to persist it via `bump_host_epoch`.
If the disk write fails (or the daemon crashes between the two), a later
restart rehydrates the stale (lower) on-disk value — briefly reopening the
epoch-floor gap that finding #1's fix (PR #24) closed for the *host* side.
This is a narrower, restart-triggered variant of the same class of bug, on
the joiner side specifically. There's also a related lost-update risk:
`bump_host_epoch` and the log-append path's `meta.head` update are
independent read-modify-write cycles against the same `meta.json` file with
no shared lock at the store layer (session-level lock is held by the caller
in each individual case, but the two call sites don't coordinate with each
other).

**Fix direction:** either persist the watermark to disk before advancing the
in-memory atomic (mirroring the "store-before-memory" discipline used
throughout the rest of `session.rs`), or accept the current order but make
`bump_host_epoch`'s failure retry-until-durable rather than fire-and-forget
(`session.rs`'s call site currently just warns and drops the error). Worth
revisiting once #1's epoch-floor mechanism has been in production for a bit —
the two are related enough that a combined fix (e.g. extending the store-side
epoch floor to also gate the joiner's watermark rehydration) might be cleaner
than patching this in isolation.

**Severity:** medium — same failure class as the now-fixed #1, but requires a
disk-write failure or crash at a specific narrow window to trigger, not just
network eavesdropping.

---

### #6 — Concurrent-join rollback can invalidate an already-returned "success"

**Where:** `crates/artel-daemon/src/session.rs:1589` (`claim_remote_mirror`)
and its caller `materialise_remote_session` (`~line 1405`), specifically the
`rollback_remote_mirror` path taken when `bridge.join_session` fails.

**Problem:** Join A claims the mirror slot via `claim_remote_mirror` and
starts the (slow, up-to-15s) `bridge.join_session` subscribe. Join B, racing
on the same daemon for the same session id, sees the slot already claimed
(`fresh = false`) and adopts the *same* arc — and can return success to its
caller before Join A's subscribe finishes. If Join A's subscribe then times
out, `rollback_remote_mirror` unconditionally deletes both the in-memory
entry and the persisted record — invalidating the success B's caller already
received.

**Fix direction:** track a refcount or a "waiters" list on the slot so
rollback only fires if no other join adopted the mirror in the meantime, or
have B's adoption path wait on A's subscribe outcome (via a shared
oneshot/notify) rather than returning immediately on `fresh = false`. The
`SlotReservation` machinery in `gossip_bridge.rs` (reserve → finalize → forget,
built for the H7/H8 races on the bridge side) is a reasonable model to mirror
here.

**Severity:** low-medium — narrow window (requires two racing joins for the
same session id on the same daemon, plus a slow/failing subscribe), and the
caller does at least get a wrong-but-not-corrupting result (a `JoinSession`
success for a session that then vanishes) rather than silent data loss.

---

### #7 — TOCTOU on membership/capability checks in `send()`

**Where:** `crates/artel-daemon/src/session.rs:659` (`ensure_can_write`) and
`crates/artel-daemon/src/session.rs:2261` (`Registry::send`) — the sequence of
lock-acquire/check/release cycles between the membership check, the
`ensure_can_write` capability check, and the final append.

**Problem:** `send()` checks membership, drops the lock, checks write
capability under a separate lock acquisition, drops it again, then
reacquires to build+append the message. `resolve_authoring` (the step between
the capability check and the final lock reacquisition) is synchronous — no
`.await` — so the actual window is narrow: one async lock-drop-then-reacquire,
not a wide multi-await gap. A capability revoke landing in that exact window
lets at most one already-in-flight message slip through after the revoke
commits.

**Fix direction:** collapse the membership check, capability check, and
append into a single lock hold (one `s.lock().await` around all three steps)
now that `resolve_authoring` has no async work to do outside the lock — this
was likely split up historically for a reason that may no longer apply; worth
checking whether `resolve_authoring`'s signature-verification call
(`author_remote`, sync CPU-bound ed25519) is cheap enough to hold the lock
across, or whether it's deliberately kept out from under the lock to avoid
blocking other session operations during verification.

**Severity:** low — self-limiting (at most one message per revoke), and the
fix is a small refactor, but worth closing since the "drop-before-append" O1
invariant documented elsewhere in the file implies this window shouldn't
exist at all.

---

### #8 — Graceful shutdown can hang on a stalled IPC client

**Where:** `crates/artel-daemon/src/server.rs:449`
(`while connections.join_next().await.is_some() {}`, no timeout) and
`crates/artel-daemon/src/server.rs:1564` (`send_frame`, blocking
`guard.send(frame).await` with no cancellation-aware `select!` against the
shutdown token).

**Problem:** a client that requests a large replay and then stops reading
(e.g. a stuck consumer process) will eventually fill the Unix socket send
buffer. `send_frame`'s blocking send then never returns, and the connection
task never exits. `Daemon::run`'s shutdown drain loop
(`connections.join_next().await.is_some()`) has no timeout, so it waits
forever on that one stuck task — meaning `Daemon::run()` never returns, the
daemon never reaches the iroh teardown / PID-file cleanup that follows the
drain loop, and a service manager sees the process fail to exit within its
stop grace period.

**Fix direction:** add a bounded timeout around the drain loop (e.g.
`tokio::time::timeout` around the `while` loop, or a deadline passed through
to each spawned connection task via the existing `ShutdownToken`) so a stuck
client gets forcibly aborted rather than blocking shutdown indefinitely. Since
IPC is a local, trusted Unix socket (not attacker-facing in the threat
model — memory `project_headless_first_class` notes headless/systemd
operation is first-class), this is an operational robustness fix, not a
security fix: it matters for graceful daemon restarts/upgrades under
`systemd`/`launchd`, where a stuck workspace client shouldn't be able to make
`stop` hang.

**Severity:** low-medium — real operational bug (blocks graceful restart),
zero security impact given the trust boundary, moderate fix complexity
(needs to reach into the per-connection task's send path, not just the outer
drain loop).

## Suggested fix order

1. **#2** (gossip frame-size mismatch) — silent data loss today, no attacker,
   cheap fix once the size-ceiling decision is made.
2. **#5** (joiner-side epoch persistence ordering) — same bug class as the
   now-merged #1; worth doing while that context is still fresh, and might
   combine cleanly with extending the store-side epoch floor.
3. **#3** (unbounded task spawn) — real DoS vector; the dedup-before-spawn
   half is a small, self-contained change.
4. **#4** (concurrent HostSession creation race) — same fix shape as the
   already-solved `claim_remote_mirror` race; mostly a port of existing code.
5. **#8** (shutdown hang) — operational only, but affects every restart/
   upgrade path in production.
6. **#6** (concurrent-join rollback race) — narrow window, low blast radius.
7. **#7** (send() TOCTOU) — narrowest window of all seven, smallest fix.

Note this ordering is a judgment call from the original review pass, not a
user-confirmed priority — revisit before starting if priorities have shifted.
