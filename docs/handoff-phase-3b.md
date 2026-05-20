# Handoff: Phase 3b (artel-fs hardening)

Written 2026-05-19 right after Phase 3a (the artel-fs MVP) landed.
Phase 3a shipped a working `Workspace::host` / `join` / `run` over
in-memory `iroh-docs` + `iroh-blobs::store::mem::MemStore`. Phase 3b
turns it into something a real consumer can rely on — disk-backed
storage, stable identity, crash safety, and a configurable filter.

The MVP doc (`docs/handoff-phase-3-mvp.md`) was deleted when 3a
landed; if you want the original architectural rationale, look at
`docs/adr/001-collab-substrate-platform.md` § "Doc handles across
IPC" and `docs/roadmap.md` § "Phase 3 — Slice 3a".

This doc is the **concrete plan** for 3b. Delete it once 3b is in.

## Where we are

279 tests passing. fmt + clippy clean both feature modes. Phase 3a's
`artel-fs` crate ships:

- `Workspace::host` — own iroh node, fresh in-mem doc, scan + publish
  existing files, broadcast `DocTicket` as a `MessageKind::System`
  message with action `workspace.ticket`.
- `Workspace::join` — own iroh node, subscribe + drain for the
  ticket, `import_and_subscribe`, wait for `SyncFinished` +
  `PendingContentReady`, bulk-export to disk.
- `Workspace::run` — spawns watcher (`notify-debouncer-full`,
  300 ms) + applier (`Doc::subscribe`, handles `InsertRemote` +
  `ContentReady` with 250 ms echo-guard release).
- Tests: `iroh_docs_smoke`, `host_publishes_ticket`,
  `join_bulk_export`, `live_edit`, `delete_propagates`, `round_trip`
  (5 consecutive runs).

What's missing: anything the workspace knows about disappears when
the host process exits. Restart the workspace and the doc starts
from zero; joiners that joined the *previous* doc see the old
ticket but it points at a `NamespaceId` that no longer exists on the
host. That's the gap 3b closes.

## Architectural shape (resolved)

Read this first. Some choices below are subtle.

### Disk layout per workspace

```
<state_dir>/
  iroh.key             # 32 bytes, mode 0600 — workspace's own iroh secret
  author.key           # 32 bytes, mode 0600 — author id stamping our writes
  doc-id               # 32 bytes — NamespaceId of the published doc (host only)
  docs/                # iroh-docs persistent store dir (Docs::persistent)
    docs.redb
    default-author     # iroh-docs's own per-store default author
  blobs/               # iroh-blobs FsStore
    blobs.db
    ...
```

- `state_dir` is **per workspace**, not shared with the daemon.
  Concretely: a `WorkspaceConfig::state_dir` arg defaulting to
  `<root>/.artel-fs/`. Yes, that lives *inside* the workspace dir;
  the existing hardcoded filter already skips dotfiles starting
  with `.` only when they match the explicit list, so we add
  `.artel-fs` to that list (next to `.git`, `target`, etc.) — see
  filter changes below.
- `iroh.key` and `author.key` are **distinct**: the iroh secret
  is the *network* identity (NodeId / EndpointAddr), the author
  key is the *doc-attribution* identity. Both are 32-byte ed25519
  secrets, both persisted with the same `load_or_create` shape
  the daemon already uses for its iroh key
  (`crates/artel-daemon/src/iroh_key.rs`). Steal that file —
  literally — into `crates/artel-fs/src/keystore.rs` and
  generalise the path arg.
- `doc-id` is the host-only marker that says "the doc this
  workspace already published". On restart the host *reuses*
  the same `NamespaceId` (open it via `Docs::open(id)`), so the
  ticket it emits is identical to last time and any joiner with
  the old ticket can re-sync. Without this, every host restart
  invalidates every outstanding ticket.

### Host vs joiner persistence asymmetry

Hosts persist three things: iroh key, author key, doc namespace
(plus the Doc/Blobs stores under it).

Joiners persist two: iroh key, author key. They don't store
`doc-id` — they get the namespace from the host's ticket every
time they join. This matches the existing flow: a joiner that
restarts but holds onto the old ticket can re-import it; if the
host's namespace is the same (via the host's `doc-id`
persistence), sync resumes. If the host has rotated the doc the
joiner sees an `InvalidTicket`-shaped failure on `import` and the
caller has to grab a fresh ticket out of band.

Joiners *do* keep their `Docs::persistent` store across
restarts — once they've imported a namespace, the store
remembers the entries they synced, so a joiner that comes back
online with no host reachable still has the last-known state on
disk. That matters for ADR-001 § "Reconnect from anywhere".

### iroh identity stability

`iroh.key` has a less obvious benefit: NAT-traversal hints other
peers learned about you (relay URL, observed direct addrs) are
keyed by `EndpointId`, which is derived from the secret. Rotate
the key on every restart and every reconnection blows up. This
is the same reason `artel-daemon` persists *its* iroh key.

### What happens to the in-memory MVP

Delete the memory paths. We do **not** keep both backends.
"Two impls from day one or none" applies to traits, not to
configuration. Memory storage is fine for tests; the disk-backed
production path is the only shipped variant. (If you really want
a memory variant for tests later, add a `WorkspaceConfig::storage:
Storage::Memory | Storage::Disk { ... }` enum — but not now.)

### Doc-ticket reuse vs re-publish on host restart

Two options:

1. **Reuse the namespace** (proposed). On restart, host opens the
   same Doc, re-runs `scan_and_publish_existing` (idempotent: same
   key, same bytes, echo-guarded), re-broadcasts the same
   `DocTicket` over the artel session.
2. Rotate the namespace and tell joiners via a new system message
   action (`workspace.ticket-rotated`).

Option (1) keeps existing tickets valid and matches the user's
mental model ("the workspace at /foo is the same workspace
across restarts"). Go with (1).

There's a subtle edge case in (1): the persisted doc may already
contain entries for files that were deleted on disk while the
workspace was down. On restart, before broadcasting the ticket,
we need a **reconcile step**: walk the doc, for each entry whose
key has no corresponding file on disk, emit a `Doc::del`
(tombstone). Then `scan_and_publish_existing` writes anything
that's on disk but missing from the doc. Net result: the doc
matches the current filesystem.

Reconcile order matters: tombstone first, publish second.
Otherwise a file that was deleted offline + recreated with
different bytes during downtime would tombstone *after* the
republish and we'd lose the data.

## Engineering principles (non-negotiable)

Same as before. Brief reminder:

- **Two impls from day one or none.** Don't introduce a
  `Workspace` trait. There's still one impl. (See above re:
  not adding a memory backend — it's not a trait, it's just
  removing a code path.)
- **Persistence-first.** Every mutation in the daemon already
  follows this; for `artel-fs`, "persistence" is iroh-docs +
  iroh-blobs writing to redb / `blobs.db`. We don't add our
  own state file beyond `iroh.key` / `author.key` / `doc-id`,
  all of which are write-once-on-create.
- **Tests for every mutation.** Disk-backed storage is itself
  a mutation in the test sense — every persisted thing needs
  a "host writes X, restart, X is still there" e2e. See test
  list below.
- **Postcard wire enums must be externally tagged.** Not
  applicable here (no new wire types).
- **Unix-only.** notify works cross-platform; we still
  `#[cfg(unix)]`-gate any 0600 mode bits we touch.
- **No speculative abstractions.** The reconcile step is
  one helper function, not a `ReconcileStrategy` trait.

## Slice 3b-1 — Disk-backed storage (this slice's focus)

The workspace persists its iroh identity, author identity, and
the doc/blob stores so a host restart resumes the same workspace
and joiners with old tickets can keep syncing.

### Concrete steps, in order

#### 3b-1-a — `WorkspaceConfig` + `keystore` module

1. Add `WorkspaceConfig { state_dir: Option<PathBuf> }`. `None`
   defaults to `<root>/.artel-fs/`. Resolved once at
   construction time, stored on `Workspace`.
2. Port `crates/artel-daemon/src/iroh_key.rs` into
   `crates/artel-fs/src/keystore.rs`. Generalise the path arg
   (it's already path-driven). Export
   `load_or_create_secret(path) -> ed25519 SecretKey` — used by
   both `iroh.key` and `author.key`. (The daemon's version is
   iroh-flavoured; we want the underlying ed25519 secret. Open
   the file, parse 32 bytes, that's it.)
3. Mode 0600 on Unix; `#[cfg(unix)]`-gate the chmod call.
4. Tests in `keystore.rs`:
   - generate-then-load round-trip
   - persists across re-open (hash the bytes)
   - mode is 0600 (Unix-only test)
   - parent dir created if missing (matches `iroh_key.rs`)

The point of porting rather than depending on the daemon's
module: `artel-fs` has no dep on `artel-daemon` and shouldn't.
Two unrelated callers of the same trivial helper is fine.

#### 3b-1-b — Switch `WorkspaceNode` to disk-backed Docs/Blobs

In `crates/artel-fs/src/node.rs`:

1. Take `state_dir: &Path` as an argument to `WorkspaceNode::spawn`.
2. Replace `MemStore::new()` with
   `iroh_blobs::store::fs::FsStore::load(state_dir.join("blobs"))`.
3. Replace `Docs::memory()` with
   `Docs::persistent(state_dir.join("docs"))`.
4. Replace the random `SecretKey::generate()` for the iroh
   endpoint with `load_or_create_secret(state_dir.join("iroh.key"))`.

Both `FsStore::load` and `Docs::persistent(...).spawn(...)` are
async and return `Result`. Map errors into `WorkspaceError::Iroh`
with context.

The `endpoint`, `docs`, and `blobs` fields keep the same types.
`BlobsProtocol::new(&fs_store, None)` works the same way it did
for `MemStore` (both deref to `Store`).

Add `state_dir: PathBuf` to `WorkspaceNode` so `Workspace`
shutdown can do anything that needs the path later (it doesn't
yet — but having the path on the runtime saves threading later).

#### 3b-1-c — Persist `doc-id` (host) and reuse it on restart

In `Workspace::host`:

1. After `WorkspaceNode::spawn`, check for `state_dir/doc-id`.
   If it exists, parse 32 bytes as a `NamespaceId` and call
   `node.docs.open(id).await?.ok_or(WorkspaceError::Doc(...))?`
   instead of `node.docs.create()`.
2. If `doc-id` does not exist, `node.docs.create()` (fresh
   workspace) and atomically write the new namespace's bytes
   to `state_dir/doc-id` (mode 0600 not required — namespace
   ids aren't secret).
3. Atomic write = "write to `doc-id.tmp`, fsync, rename" —
   match the pattern the daemon's `iroh_key.rs` uses.

The author id:

4. Same shape: `state_dir/author.key`.
   `load_or_create_secret(...)` returns 32 bytes; build an
   `iroh_docs::Author` from them via
   `Author::from_bytes(...)`. Use `node.docs.author_import` to
   register it with the docs store. **OR** — simpler — let
   iroh-docs manage author identity itself: when we use
   `Docs::persistent(...)`, it already loads / persists a
   default author at `state_dir/docs/default-author`. Use
   `node.docs.author_default()`. This avoids a duplicate
   author-key file we don't need.

   **Decision: use iroh-docs's built-in default-author
   persistence.** Drop `author.key` from the disk layout
   above. Adjust the doc layout section accordingly when you
   write code: `docs/default-author` is the author-id
   persistence file, owned by iroh-docs.

This means the disk layout simplifies to:

```
<state_dir>/
  iroh.key         # workspace's iroh endpoint identity
  doc-id           # host only: NamespaceId of the published doc
  docs/            # Docs::persistent: includes default-author
  blobs/           # FsStore
```

#### 3b-1-d — Reconcile on host restart

Before `Workspace::host` returns:

1. After `Docs::open(id)` succeeds (returning-host path only;
   for a fresh-host this is a no-op), enumerate the doc:
   `doc.get_many(Query::all())`.
2. For every entry with non-zero `content_len`, check if a
   file exists at `key_to_path(root, entry.key())`. If not,
   `doc.del(author, entry.key()).await` to tombstone it.
3. Then run `scan_and_publish_existing` as before.

Two subtleties:

- The reconcile pass must run **before** the watcher /
  applier are spawned. Otherwise a tombstone we emit could
  race a peer-driven InsertRemote that's mid-sync.
- The reconcile pass must run **before** the ticket is
  re-broadcast. A joiner picking up the old ticket and
  syncing while we're mid-reconcile would see flapping
  state.

Tests:
- "host publishes a.txt + b.txt, shutdown, delete b.txt on
  disk, restart host, the doc no longer contains b.txt"
- "host publishes a.txt, shutdown, modify a.txt on disk,
  restart, the doc has the new bytes" (covered by
  scan_and_publish_existing's idempotent-publish path —
  echo guard records the last-published hash so the watcher
  doesn't double-publish, but the *initial scan* does
  re-publish if the bytes differ).

#### 3b-1-e — `Workspace::shutdown` must let stores flush

`Docs::persistent` writes to redb in the background.
`FsStore` similarly. Today `Workspace::shutdown` calls
`router.shutdown().await` which is supposed to drain the
protocol handlers, but the *store* shutdown is separate:

1. Look at iroh-docs `protocol::Docs::shutdown` impl —
   confirms it calls `Engine::shutdown` which flushes the
   replica store. Good.
2. `BlobsProtocol::shutdown` calls `Store::shutdown()` —
   for `FsStore` that drains writes. Good.
3. The `Router::shutdown` already invokes both. So the
   existing teardown is correct in shape; just verify
   empirically with a "write a file, shutdown, restart,
   read the file from disk via a fresh node" test.

If the test fails (writes lost), we've found a real bug in
either the iroh shutdown path or our teardown order;
diagnose before adding workarounds.

#### 3b-1-f — Disk-resume e2e test

`crates/artel-fs/tests/disk_resume.rs`:

```text
1. Alice hosts a workspace at <dir_a>/wstate, dir contains a.txt.
2. Wait for the ticket message; capture the ticket.
3. Bob joins via the ticket at <dir_b>/wstate, dir contains nothing.
4. Bob's dir gains a.txt (sanity).
5. Both shutdown gracefully.
6. Spin up two fresh daemons (could even be the same state
   dirs, but easier: the daemons are unrelated to fs state).
7. Alice re-hosts pointing at <dir_a> + same wstate.
8. Capture the new ticket; assert it equals the old one
   (NamespaceId stable, plus the host's NodeId stable
   because iroh.key was persisted, plus relay URL stable —
   so the ticket bytes should be byte-identical).
9. Bob re-joins via the OLD ticket. Bob's dir already has
   a.txt from before; assert the workspace stands up
   without errors.
10. Alice writes b.txt; Bob sees it. (Live sync resumed.)
11. Bob writes c.txt; Alice sees it.
12. Alice deletes a.txt; Bob's a.txt disappears (delete
    propagation across restart).
```

If step 8 fails ("ticket changed across restart"), find out
which bit changed: NamespaceId? NodeId? Relay URL list?
That tells you which of iroh.key / doc-id / iroh's address
discovery is the regression.

Also worth: a focused "Alice's ticket is byte-identical
across restart" unit test, captured from `Workspace::host`'s
return value.

#### 3b-1-g — Filter additions

Add `.artel-fs` to the hardcoded skip list in
`crates/artel-fs/src/filter.rs::is_hardcoded_skip`. Single
line change; add to `skips_swp_and_tmp_and_ds_store` test
(or add a dedicated test).

Without this, the watcher would see writes to
`<root>/.artel-fs/blobs/blobs.db` and try to publish them
into the doc — which would then cause an infinite-loop-ish
mess where the doc contains pointers to the doc.

**This is load-bearing.** Don't skip it.

### Definition of done for 3b-1

- `cargo test --workspace --all-features` green; new tests
  in 3b-1-a (keystore unit tests) and 3b-1-f (disk_resume
  e2e) pass.
- `cargo clippy` clean both feature modes.
- `cargo fmt --all` clean.
- Manually verified: kill `-9` a workspace process, restart,
  state survives. (Crash-recovery test in 3b-3 below
  formalises this; the 3b-1 sanity is "graceful shutdown
  preserves state".)

### Risks / unknowns

1. **`Docs::open(id)` returning `None` on a redb that
   exists but doesn't contain the namespace.** This shouldn't
   happen if we wrote `doc-id` and `docs/` together, but
   storage corruption / partial-writes could surface. Surface
   it as a clear `WorkspaceError::Doc("doc-id refers to
   a namespace not in the store")` and let the caller decide
   to delete + restart fresh.
2. **Author drift across restart.** iroh-docs persists the
   default author in `docs/default-author`, but if a user
   nukes that file and not the rest, the next restart
   creates a new author and writes it stamps will differ.
   We don't attribute writes anywhere user-visible today, so
   this is cosmetic — but if 3b-2 (persistent author
   identity, see below) wants stronger guarantees we'll need
   to take ownership of the author file ourselves.
3. **iroh-docs `Docs::persistent` thread + tokio runtime
   semantics.** The MVP smoke test on `MemStore` ran cleanly;
   `FsStore` brings up its own tokio runtime
   (`build_multi_thread`) for store I/O. Should be fine
   inside an existing tokio context, but verify the smoke
   test still passes after the swap. If it deadlocks under
   tokio's runtime nesting rules, fall back to wrapping
   `FsStore::load` in `tokio::task::spawn_blocking`.
4. **Disk write-amplification.** Every `set_bytes` writes
   to redb + blobs.db. For a workspace of N small files
   this is fine; for large repos the watcher's "write a
   file, debounce, publish" cycle could thrash. Out of
   scope for 3b-1; revisit if a real user complains.

## Slice 3b-2 — Persistent author identity (sketch)

**Goal:** the bytes attributing each doc entry to "this peer"
are stable across `Workspace` restarts. Today they're not —
on restart we get a fresh `AuthorId` from
`docs/default-author` *if* iroh-docs persisted it (which it
does for `Docs::persistent`), so 3b-1 mostly solves this for
free. But:

- We don't currently *expose* the author id anywhere user-
  visible (no `WorkspaceEvent::PeerWrote` carries it). If
  ever we want to attribute changes ("Alice deleted X"),
  we need a stable mapping from `AuthorId` to a peer
  identifier the app understands.
- A user who blows away `state_dir/docs/default-author`
  but keeps the rest of the doc state would suddenly start
  writing under a new author, which iroh-docs treats as a
  separate writer. This breaks if peers ever count distinct
  authors (they don't today).

**Probable shape:** take ownership of `state_dir/author.key`
(re-add to the disk layout), use
`load_or_create_secret(...)` + `Author::from_bytes(...)` +
`docs.author_import(...)` instead of leaning on
`author_default()`. Add a `Workspace::author_id() -> AuthorId`
getter. Plumb it into `WorkspaceEvent` if a real consumer
needs it.

**Tests to add:**
- "author id stable across restart"
- "deleted author.key surfaces an error rather than silently
  rotating"

**When to do this:** when 3b-1 is in and a real consumer
(harness, presumably) actually wants per-author attribution.
Don't pre-empt.

## Slice 3b-3 — Crash recovery test (sketch)

**Goal:** prove that a `kill -9` mid-write doesn't corrupt
the workspace state. iroh-docs uses redb (ACID) and
iroh-blobs uses an append-only blob store with redb metadata,
so the underlying primitives are designed for this — but
*our* code layered on top might not be.

**Probable shape:**
- New integration test `crates/artel-fs/tests/crash_recovery.rs`.
- Scenario: spawn a workspace as a child process (via a tiny
  test binary that just sits in `Workspace::run`), wait for
  it to publish a few files, send `SIGKILL` mid-write, restart
  the workspace, assert the doc + disk state agree (everything
  on disk is in the doc; everything in the doc is on disk;
  no dangling tombstones, no orphan entries).
- Particularly nasty case: kill mid `scan_and_publish_existing`
  during host startup (some files in the doc, others not).
  The reconcile step from 3b-1-d should make this self-healing
  on next start.

**Why a separate slice:** subprocess test harness is its own
chunk of code (cargo workspace tricks for the child binary,
process supervision, deterministic timing). Doesn't gate
3b-1.

**Risks:**
- Test-side flakiness around process timing. Use
  `WorkspaceEvent::PeerWrote` / `Error` events to drive
  test progress rather than sleeping.
- macOS `SIGKILL` semantics differ subtly from Linux around
  fsync; tolerate that the test takes a few seconds longer.

## Slice 3b-4 — Configurable filter (sketch)

**Goal:** apps can extend or override the hardcoded skip
list (`.git`, `target`, `node_modules`, `.DS_Store`,
`*.swp`, `*.tmp`).

**Probable shape:**
- `WorkspaceConfig::filter: FilterRules` (new). Default
  matches today's hardcoded list verbatim, *plus*
  `.artel-fs` (added in 3b-1-g).
- `FilterRules` exposes `extend(&[&str])`, `replace_skip_list(&[&str])`,
  `add_extension(&str)` — keep the API minimal until a real
  consumer asks for more.
- `WorkspaceFilter::new` takes `&FilterRules` instead of
  having the list baked in.

**Tests to add:**
- "default filter matches today's hardcoded behaviour"
  (regression guard so the refactor is invisible to existing
  tests).
- "extend with a custom path causes that path to be skipped".
- "filter changes only take effect at workspace construction
  time" (don't try to make it dynamic — too much complexity
  for no proven need).

**When to do this:** when a consumer (harness, again
probably) hits a real "I want to sync `.git` for a
collaborative-git-replay use case" moment, or "stop syncing
my Cargo.lock". Until then it's speculative.

## How to start (fresh-agent instructions)

1. Read `docs/roadmap.md` § "Phase 3 — Slice 3a" and
   § "Slice 3b — open follow-ups" for context on what
   shipped and what's left.
2. Read this doc for the 3b plan.
3. Read `docs/adr/001-collab-substrate-platform.md` § "Doc
   handles across IPC" for the ADR's reasoning on
   ticket-handout (now-shipped).
4. Read `crates/artel-daemon/src/iroh_key.rs` — the keystore
   module you're porting.
5. Read `crates/artel-fs/src/node.rs` — the iroh node setup
   you're swapping from in-mem to disk.
6. Read `crates/artel-fs/src/workspace.rs::Workspace::host` —
   the codepath you're adding `Docs::open` + reconcile to.
7. Start at slice **3b-1-a** (keystore module). Each
   sub-slice has tests; don't skip them.
8. When 3b-1 lands, stop. 3b-2/3/4 are sketches, not
   prescriptions — re-plan them with fresh context.

When in doubt: small slices, tests at every layer, e2e last.
The pattern that worked for 3a will work for 3b.
