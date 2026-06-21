# Spike learnings: headless artel driver (toward CI/CD agent-driving)

date: 2026-06-21
status: SEED — captured from a working spike. The spike itself lives in the
        **gitignored** `examples/chat-harness` (`src/scenario.rs` +
        `chat-harness scenario <name>`), so it is NOT committed; this doc is the
        durable record of what it taught us, since the code can't be the record.
relates-to:
  - ADR-003 (daemon stays namespace-agnostic)
  - CONTEXT.md "Namespace-agnostic daemon", "Evict"
  - project memory: evict-does-not-revoke-ticket
  - `docs/plans/2026-06-18-rw-redelivery.md` (the #2 fix the spike validates)

## What the spike was

A headless, self-asserting scenario driver bolted onto the throwaway chat
harness: `chat-harness scenario evict-drops-membership` spins up two in-process
daemons + workspaces, drives host → join → grant → write → evict through the
real `artel-fs` / `artel-protocol` APIs, and asserts the host's session
`peer_count` drops 2→1 after the evict (proving the `RemoveSessionMember` fix).
Exits 0/1. Runs in ~1.5s against a localhost relay.

Built as a deliberate first step toward **driving artel peers in CI/CD** (e.g.
running agents headless), not just to smoke-test one fix. These are the
learnings worth keeping for when headless driving becomes a real requirement.

## Learnings

### 1. The two-`EndpointSetup`-types seam is the first wall an external driver hits

`artel-daemon::EndpointSetup` and `artel-fs::EndpointSetup` are **distinct
types** (peer crates, neither depends on the other) that both wrap the same
`Arc<DnsPkarrServer>` for the `Testing` variant. Any driver standing up both a
daemon and a workspace must build *both* setups from one fixture — the
integration tests already do this with paired `daemon_testing_setup` /
`testing_setup` helpers (`crates/artel-fs/tests/common/mod.rs`). The spike had
to re-derive it (and hit an E0308 first). **A shared test/driver-fixture helper
crate** that hands back both setups from one `DnsPkarrServer` would remove this
friction for every future external driver.

### 2. A CI driver must NOT use the public relay; `test-utils` is the lever

The TUI path uses `EndpointSetup::Production` (public n0 relay), which carries
the noq-proto handshake flakiness the n0 suite documents — fine for a human at a
terminal, fatal for deterministic CI. The fix: enable the crates' `test-utils`
feature and run against a localhost `DnsPkarrServer` (`EndpointSetup::Testing`),
exactly like the integration harness. So **`test-utils` is effectively the
"headless/CI-driveable" feature** today. If headless driving becomes
first-class, consider promoting a localhost-relay setup out from behind
`test-utils` (it's currently framed as test-only) so a driver binary doesn't
have to pull a test feature in a release build.

### 3. `peer_count` via `ListSessions` is a clean machine-observable membership signal

The spike asserts on `SessionSummary.peer_count` (derived from the durable
`members` set, NOT live connections). It's the cleanest substrate-level
observable for "did membership change" — no log parsing, no event-stream race.
Good primitive for a future driver API. Note it's membership-backed: it reflects
`RemoveSessionMember` / `leave` / admission, not transient connectivity.

### 4. The harness's command handlers are coupled to the TUI; the substrate APIs are not

The spike deliberately did NOT reuse the harness's `/evict` etc. handlers —
those take `&mut EventLoopCtx` and push to `AppState` (TUI state). Driving the
**substrate APIs directly** (`Workspace::host_with`/`join_with`,
`Request::Send` with a `CapabilityAction`, `Request::ListSessions`) was cleaner
and is the right altitude for a driver. Lesson for a real driver: target the
substrate surface, not a consumer's UI-coupled glue.

### 5. Write-then-observe needs unique content + poll-a-fixed-path

Same hazards as the real-n0 tests: the echo-guard suppresses identical
re-writes (so a fixed-content poll authors only once and can race a namespace
swap), and a write needs a round-trip (so reading back in the same tick loses).
The `wait_until` helper writes unique content each tick and polls the host's
mirror for arrival. Any driver doing eventually-consistent assertions needs this
shape; worth extracting into the hypothetical fixture crate.

## If this becomes real

The `Peer` abstraction + `wait_until` + scenario-catalogue shape is the kernel.
The path to "real" is to **extract a committed test-driver crate** (out of the
gitignored harness) that:

- bundles the paired-`EndpointSetup`-from-one-`DnsPkarrServer` fixture (learning #1),
- exposes a localhost-relay setup without requiring a test-only feature in a
  release driver (learning #2),
- offers `peer_count`-style membership observables (learning #3) and the
  unique-write/poll helper (learning #5),
- targets the substrate APIs, not consumer UI glue (learning #4).

Until then, the spike stays in the harness as a proof-of-shape, and this doc is
its memory.
