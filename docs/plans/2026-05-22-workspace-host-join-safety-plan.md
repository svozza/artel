# Workspace host/join safety + configurable policy — implementation plan

Source brainstorm: `docs/brainstorms/2026-05-22-workspace-host-join-safety-brainstorm.md`. The brainstorm picks Approach B (per-path role-blind rules), ticket-bound `PathRules`, no-default `AttachPolicy`, `ReadWrite`/`ReadOnly` only. This plan is *how*, not *what*.

## Sub-slice ordering — validated

Brainstorm proposed:
- (a) ticket v3 + `PathRules` plumbing
- (b) `AttachPolicy` + wrong-dir guards
- (c) watcher rule consultation

**Recommended re-ordering: (b) → (a) → (c).**

Rationale: `AttachPolicy` is a self-contained safety guard with zero wire-format risk. It closes the actually-reported hazard (almost-published-home-dir, 2026-05-20) on its own and is the smallest landable unit. (a) introduces a wire-version bump and a forward-incompatibility decision — better to land that with the *consumer* of the new wire field already in place (rules just being parsed and stored, no enforcement yet). (c) needs both: it consults rules per-event and is the largest behavioural change.

If "land safety first" is deemed less important than "land wire change first while it's small", (a) → (b) → (c) is the brainstorm's order and is also fine. Plan below documents (b) → (a) → (c).

Each sub-slice ends with green tests, fmt/clippy clean both feature modes, and is independently mergeable.

---

## Sub-slice 1 — `AttachPolicy` + wrong-dir guards

**Goal:** Every `Workspace::host`/`host_with`/`join`/`join_with` call requires an explicit `AttachPolicy`. The constructors enforce the policy *before* spawning the iroh node, *before* any disk write.

### Files touched

- `crates/artel-fs/src/workspace.rs` — add `AttachPolicy` enum, change `host`/`join`/`host_with`/`join_with` signatures to take an `AttachPolicy`, add `enforce_attach_policy(...)` helper called at the top of `host_with`/`join_with` (after `canonicalise`, before `ensure_state_dir` / `WorkspaceNode::spawn`).
- `crates/artel-fs/src/error.rs` — add `WorkspaceError::Policy(PolicyViolation)` variant carrying a structured reason (see "Error type placement").
- `crates/artel-fs/src/filter.rs` — expose the hardcoded-skip names so the emptiness check shares the source of truth (e.g. lift `is_hardcoded_skip` to `pub(crate)` + add a `WorkspaceFilter::is_hardcoded_skip(path)` thin wrapper). Avoid duplicating the skip list.
- `crates/artel-fs/src/lib.rs` — re-export `AttachPolicy`, `PolicyViolation`.
- All test files under `crates/artel-fs/tests/` — every existing call to `Workspace::host(...)` / `Workspace::join(...)` / `host_with(...)` / `join_with(...)` needs to pass an `AttachPolicy`. Use `AttachPolicy::AllowExisting` for tests that pre-seed the dir, `AttachPolicy::RequireEmpty` for tests that start empty.
- `crates/artel-fs/tests/bin/crash_child.rs` — same update.

### Public API additions

```rust
// in artel-fs::workspace (re-exported from lib.rs)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachPolicy {
    RequireEmpty,
    AllowExisting,
    InitFromExisting, // originate-only; on join, rejected
}

// New constructor signatures (replace the old ones — no defaults):
impl Workspace {
    pub async fn host(
        client: &Client,
        session: SessionId,
        root: PathBuf,
        policy: AttachPolicy,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError>;

    pub async fn join(
        client: &Client,
        session: SessionId,
        root: PathBuf,
        policy: AttachPolicy,
    ) -> Result<(Self, mpsc::Receiver<WorkspaceEvent>), WorkspaceError>;

    // host_with / join_with: same plus a WorkspaceConfig.
    // The policy stays a positional arg (not in WorkspaceConfig) to make
    // it visible at every call site — that's the whole point.
}
```

`WorkspaceConfig` does **not** gain a `policy` field. Brainstorm: "no default — every caller passes an `AttachPolicy` explicitly." Putting it in the config would let it default-initialise via `WorkspaceConfig::default()`. Keep it positional.

### Emptiness semantics for `RequireEmpty`

A directory counts as "empty" iff every entry in `read_dir(root)` either:
1. has a name equal to `DEFAULT_STATE_SUBDIR` (`.artel-fs`) **or** the resolved `state_dir` (when overridden), **or**
2. would be filtered out by `WorkspaceFilter::is_hardcoded_skip` (`.git`, `target`, `node_modules`, `.DS_Store`, `*.swp`, `*.tmp`).

Implementation: top-level `read_dir` only — *not* a deep walk. A non-empty `src/` is non-empty regardless of contents. Symlinks at the top level count as non-empty (cheap conservatism: the brainstorm says "wrong-dir hazard" and a symlink we'd refuse to follow shouldn't make a dir look "empty").

### `InitFromExisting` semantics

- **On `host`:** identical to `AllowExisting` for now. The current `scan_and_publish_existing` call already adopts the dir's contents; there's no `host`-time difference between the two today. Documented as "intentional adoption opt-in"; will diverge from `AllowExisting` once a future slice introduces a snapshot/init step that's distinct from the live scan.
- **On `join`:** rejected with `PolicyViolation::InitFromExistingNotMeaningfulOnJoin`. Brainstorm leans this way; joiners don't have a canonical tree to seed from, and silently treating it as `AllowExisting` would let consumers stop noticing the host/join distinction at exactly the boundary where it matters.

### Test additions (required by `feedback_extensive_unit_tests`)

Unit tests in `workspace.rs`:
- `enforce_attach_policy_require_empty_accepts_truly_empty_dir`
- `enforce_attach_policy_require_empty_accepts_dir_with_only_artel_fs`
- `enforce_attach_policy_require_empty_accepts_dir_with_only_filtered_paths` (`.git/`, `target/`, `.DS_Store`, `foo.swp`)
- `enforce_attach_policy_require_empty_accepts_dir_with_overridden_state_dir`
- `enforce_attach_policy_require_empty_rejects_dir_with_user_file`
- `enforce_attach_policy_require_empty_rejects_dir_with_subdirectory`
- `enforce_attach_policy_require_empty_rejects_top_level_symlink`
- `enforce_attach_policy_allow_existing_passes_anything`
- `enforce_attach_policy_init_from_existing_passes_on_host`
- `enforce_attach_policy_init_from_existing_rejected_on_join`

New integration tests under `crates/artel-fs/tests/`:
- `attach_policy_host.rs` — host into populated dir with `RequireEmpty` returns `Policy(...)` error and does NOT spawn the iroh node (assert `state_dir/iroh.key` absent post-fail). Same call with `AllowExisting` succeeds and publishes contents.
- `attach_policy_join.rs` — join into populated dir with `RequireEmpty` returns `Policy(...)` error before bulk-export; with `AllowExisting`, bulk-export proceeds (current behaviour). With `InitFromExisting`, returns `Policy(...)`.
- `attach_policy_state_dir_only.rs` — `RequireEmpty` succeeds when the dir contains only an existing `.artel-fs/` (the resume case).

### Definition of done

1. `Workspace::host`/`join` signatures require `AttachPolicy`; no public default exposed.
2. Policy check runs before iroh node spawn (no leftover `iroh.key` / `doc-id` on a `Policy` error).
3. All existing test files updated; all tests still pass.
4. New unit + integration tests above exist and pass.
5. fmt + clippy clean both feature modes.
6. `cargo doc` builds; `AttachPolicy` doc-comments name each variant's safety implication.

---

## Sub-slice 2 — Workspace ticket envelope + `PathRules` plumbing

**Goal:** `PathRules` rides in the workspace.ticket payload, is bound at originate-time, decoded at join-time, stored on the `Workspace` struct. **No enforcement yet** — that's sub-slice 3. This slice only proves end-to-end that rules round-trip through the wire.

Two things named "ticket" in this codebase:
- `artel-protocol::ticket::SessionTicket` (artel-session ticket, currently v2).
- `iroh_docs::DocTicket` (the per-workspace doc ticket, broadcast as a `workspace.ticket` system message).

The brainstorm phrasing ("Bumps `TICKET_VERSION` 2→3") refers to the workspace.ticket payload, not the artel-session ticket. Concretely, the workspace.ticket payload is today `ticket.to_string().into_bytes()`. We extend the wire shape to a postcard-encoded envelope:

```rust
// new type in a new module: artel-fs::ticket
#[derive(Serialize, Deserialize)]
struct WorkspaceTicketEnvelope {
    version: u8,            // = 1; this is a fresh envelope, not bumping any existing version
    doc_ticket: String,     // DocTicket::to_string()
    rules: PathRules,
}
```

The artel-session-level `TICKET_VERSION` (`artel-protocol::ticket`) is **not** touched — leaving it at v2 keeps Phase 2c work stable. The brainstorm's "TICKET_VERSION 2→3" wording should be re-read as "introduces a versioned `WorkspaceTicketEnvelope` v1 around the existing `DocTicket` payload."

### Files touched

- `crates/artel-fs/src/rules.rs` — **new module.** Defines `PathRules`, `PathRule`, `Mode`, plus `PathRules::mode_for(rel_path: &Path) -> Mode` (first-match-wins; falls through to `default`). All glob matching uses the `globset` crate (already a transitive dep via `ignore`; verify in `Cargo.toml`). Globs are workspace-relative, forward-slash only (matches the doc-key shape; we're Unix-only).
- `crates/artel-fs/src/ticket.rs` — **new module.** Defines `WorkspaceTicketEnvelope { version, doc_ticket, rules }`, plus `encode(env) -> Vec<u8>` / `decode(bytes) -> Result<Envelope, TicketEnvelopeError>`. Postcard encoding. v1 only; future versions extend via the version byte.
- `crates/artel-fs/src/workspace.rs`:
  - `WorkspaceConfig` gains `pub rules: PathRules` (originator side; defaults to `PathRules { default: Mode::ReadWrite, rules: vec![] }` — see "v2-ticket compat" below).
  - `Workspace` struct gains `pub(crate) rules: Arc<PathRules>` (Arc so the watcher and applier can borrow without cloning the vec).
  - `host_with` serializes `WorkspaceConfig::rules` into the envelope and broadcasts that as the workspace.ticket payload.
  - `join_with` parses the envelope, extracts `doc_ticket` and `rules`, stores `rules` on the resulting `Workspace`.
  - `publish_ticket` signature changes to take `(client, session, doc_ticket, rules)` and encode the envelope internally.
  - `wait_for_ticket` returns the parsed envelope (not raw bytes).
- `crates/artel-fs/src/error.rs` — add `WorkspaceError::TicketEnvelope(TicketEnvelopeError)`.
- `crates/artel-fs/src/lib.rs` — `pub mod rules; pub mod ticket;` and re-exports for `PathRules`, `PathRule`, `Mode`.

### Public API additions

```rust
// artel-fs::rules
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mode { ReadWrite, ReadOnly }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRule { pub glob: String, pub mode: Mode }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRules {
    pub default: Mode,
    pub rules: Vec<PathRule>,
}

impl PathRules {
    pub fn read_write() -> Self { /* default Mode::ReadWrite, no rules */ }
    pub fn read_only() -> Self { /* default Mode::ReadOnly, no rules */ }
    pub fn mode_for(&self, rel: &Path) -> Mode { /* first-match-wins, falls through to default */ }
    pub fn validate(&self) -> Result<(), PathRulesError> {
        // Reject empty globs, globs with absolute prefixes, globs containing ".."
    }
}
```

### Postcard encoding for `PathRules`

Externally-tagged `Mode` (default for `serde::Serialize` on a unit-variant enum) — already postcard-compatible. `PathRule` and `PathRules` are plain structs. No `#[serde(tag, content)]` anywhere — honoured.

Encoded size: each rule is ~`len(glob) + 1 byte mode + ~2 bytes length prefix` ≈ 20-50 bytes. 100 rules ≈ 2-5 KiB postcard. The workspace.ticket message is iroh-gossip-bound; current gossip frames can be MiBs. No practical ceiling for the workspace.ticket payload (it's a session message, not a base32-encoded URL-safe string). Document this in the module-doc.

### v2-ticket compatibility decision (brainstorm open question)

Brainstorm: "leaning hard-reject — pre-1.0, no external tickets in the wild." **Recommendation: hard-reject.**

In practice this means the workspace.ticket payload is *now* always a `WorkspaceTicketEnvelope`. Joiners that try to decode the payload as a raw `DocTicket::from_str` (the old shape) will fail. Joiners running new code against an old host (which broadcast a raw `DocTicket` string) get a clean `TicketEnvelopeError::Malformed` and bail.

This is a hard cutover. Justification:
1. No external consumers pre-1.0; no tickets in the wild.
2. Silent fallback (old shape → "default-permissive rules") is the exact hazard pattern this slice closes — a `~`-published-by-accident host whose ticket was issued under the old code shouldn't be silently honoured by a new joiner.
3. The cost is "rebuild both sides together"; identical to the gossip-frame versioning story in the roadmap's "Future" section.

The mechanism: `decode` returns `TicketEnvelopeError::UnsupportedVersion(v)` for any non-1 version byte, and `TicketEnvelopeError::Malformed(_)` for any bytes that don't postcard-decode as the envelope. Old `DocTicket` strings (pure base32) won't postcard-decode → `Malformed`. The error message names "workspace ticket envelope" so confused upgraders can grep.

### Test additions

Unit tests in `crates/artel-fs/src/rules.rs`:
- `mode_for_no_rules_returns_default`
- `mode_for_first_match_wins` (rule order matters; second matching rule never reached)
- `mode_for_no_match_falls_through_to_default`
- `mode_for_glob_matches_relative_path`
- `mode_for_glob_does_not_match_paths_outside_workspace` (defensive)
- `mode_for_unicode_path_normalisation_consistent` (NFC, mirrors `keys.rs`)
- `validate_rejects_empty_glob`
- `validate_rejects_absolute_glob`
- `validate_rejects_parent_traversal_glob`
- `validate_accepts_typical_globs` (`"docs/**"`, `"*.lock"`, `"src/**/*.rs"`)
- `path_rules_round_trip_postcard` (encode + decode, equality)

Unit tests in `crates/artel-fs/src/ticket.rs`:
- `envelope_round_trips_with_empty_rules`
- `envelope_round_trips_with_dozen_rules`
- `envelope_decode_rejects_malformed_bytes`
- `envelope_decode_rejects_wrong_version_byte`
- `envelope_decode_rejects_raw_doc_ticket_string` (hard-reject pre-existing v2 shape)

Integration tests:
- `crates/artel-fs/tests/ticket_envelope_round_trip.rs` — Alice hosts with `PathRules { default: ReadOnly, rules: vec![PathRule { glob: "shared/**", mode: ReadWrite }] }`. Bob joins, asserts `bob_ws.rules()` (new pub accessor) deep-equals what Alice handed in. Verify rules ride the wire intact.
- `crates/artel-fs/tests/ticket_envelope_rejects_old_shape.rs` — manually broadcast a raw `DocTicket::to_string().into_bytes()` payload on the workspace.ticket action; assert the joiner's `wait_for_ticket` returns a `TicketEnvelope` error and does not bulk-export.

Update existing tests that capture the workspace.ticket payload (`disk_resume.rs`, `host_publishes_ticket.rs`):
- `capture_ticket` helper now decodes the envelope, returns `(DocTicket, PathRules)`.
- `disk_resume.rs`'s `phase1_ticket.capability.id() == phase2_ticket.capability.id()` assertion stays — uses the inner `DocTicket`.

### Definition of done

1. `WorkspaceTicketEnvelope` v1 lives in `artel-fs::ticket`; postcard encoded, version-byte-prefixed.
2. `PathRules` round-trips through host → workspace.ticket → joiner; joiner stores them on `Workspace`.
3. **No enforcement yet.** The watcher and applier still treat all paths as `ReadWrite`. (Sub-slice 3.)
4. `Workspace::rules() -> &PathRules` exists for tests and consumers to inspect.
5. All sub-slice-1 tests still pass after this slice's wire changes.
6. New unit + integration tests above exist and pass.
7. fmt + clippy clean both feature modes.

---

## Sub-slice 3 — Watcher and applier rule consultation

**Goal:** Wire `Workspace::rules` into the watcher and applier so `ReadOnly`-classed paths don't publish outward (watcher) and don't get written to disk inbound (applier).

### Files touched

- `crates/artel-fs/src/watcher.rs`:
  - `on_modified`: after `filter.check(...) == Include`, consult `workspace.rules.mode_for(rel_path)`. If `ReadOnly`, drop the local change *and* surface a `WorkspaceEvent::SkippedReadOnly { path, direction: Outgoing }`. Do not publish.
  - `on_removed`: same — `ReadOnly` paths don't get tombstones written by this peer.
  - `rel_path` here is `path.strip_prefix(&workspace.root)`. Compute it once with `strip_prefix`.
- `crates/artel-fs/src/applier.rs`:
  - `handle_entry`: after `filter.check(...) == Include`, consult `workspace.rules.mode_for(rel_path)`. If `ReadOnly`, **do not write** to disk. Surface `WorkspaceEvent::SkippedReadOnly { path, direction: Incoming }`.
  - Same for tombstones (zero-length entries): `ReadOnly` means we don't `remove_file` either.
  - Same handling in `handle_content_ready` — the underlying `handle_entry` call covers it.
- `crates/artel-fs/src/workspace.rs`:
  - `scan_and_publish_existing`: same rule consultation. Originator-side: a host with a rule like `default: ReadOnly, "shared/**": ReadWrite` should not publish the read-only zones either. Surfaces `SkippedReadOnly { Outgoing }`.
  - `bulk_export`: same. **Recommendation: yes, honour `ReadOnly` during bulk-export.** `ReadOnly` in role-blind v1 means "the originator declared this path-class is not subject to peer-driven mutation." Initial bulk-export *is* peer-driven mutation of the joiner's disk. Honouring it during bulk-export means a `ReadOnly` rule has no surprise carve-out for the join boundary.

  Edge case: this means a `default: ReadOnly` workspace bulk-exports nothing on join. That's the consistent behaviour. Consumers can opt into per-class `ReadWrite` rules; the default-deny case is correct for "I want to publish state outward without anyone modifying it."

- `crates/artel-fs/src/workspace.rs`: add `WorkspaceEvent::SkippedReadOnly { path: PathBuf, direction: Direction }` variant, with `pub enum Direction { Incoming, Outgoing }`.

### Public API additions

```rust
pub enum Direction { Incoming, Outgoing }

pub enum WorkspaceEvent {
    // ...existing variants...
    SkippedReadOnly { path: PathBuf, direction: Direction },
}
```

`Workspace::rules() -> &PathRules` (added in sub-slice 2) becomes load-bearing for tests.

### Test additions

Integration tests under `crates/artel-fs/tests/`:
- `read_only_outgoing_blocks_publish.rs` — Alice hosts with `default: ReadWrite, "secret/**": ReadOnly`. Alice writes `secret/key.txt` locally. Bob joins. Assert: Bob never sees `secret/key.txt` (use a sentinel-after-write to drive timing, mirror of `round_trip.rs`'s target/junk pattern). Also assert: Alice's doc has *no* entry for `secret/key.txt` (defense in depth).
- `read_only_incoming_blocks_apply.rs` — Alice hosts with `default: ReadOnly`. Alice has a pre-existing `a.txt` and the rules block its initial publish on Alice's side. Bob joins; bulk-export honours rules (joiner-side `mode_for` says `ReadOnly` for `a.txt`); Bob's disk stays empty. Assert `WorkspaceEvent::SkippedReadOnly { Incoming }` fires.
- `read_only_post_join_live_blocks.rs` — Alice and Bob both up with `default: ReadWrite, "locked/**": ReadOnly`. Alice writes `locked/x.txt` after both are running. Verify Alice's watcher drops it, Bob never gets it, and neither side publishes a doc entry.
- `mixed_rules_first_match_wins.rs` — `rules: [{ glob: "docs/**", mode: ReadWrite }, { glob: "docs/secret/**", mode: ReadOnly }]`. The second rule is unreachable (first-match-wins on `docs/secret/foo.txt` → `ReadWrite`). Assert the second rule is honoured *only* if it precedes the first.
- `default_read_write_unchanged_behaviour.rs` — `default: ReadWrite, rules: vec![]`. Run a subset of the existing `round_trip.rs` checks. Confirms zero behavioural drift for the default case.

Update existing tests:
- `round_trip.rs` etc. now construct workspaces with `PathRules::read_write()`. Most tests get a one-line addition.

### Definition of done

1. `ReadOnly` paths are never published outward by `scan_and_publish_existing` or the watcher (verified via doc inspection, not just disk state).
2. `ReadOnly` paths are never written inward by `bulk_export` or the applier.
3. First-match-wins ordering verified.
4. `WorkspaceEvent::SkippedReadOnly` fires in both directions.
5. All existing tests still pass.
6. New integration tests above exist and pass.
7. fmt + clippy clean both feature modes.

---

## Cross-cutting concerns

### v2-ticket compatibility

**Recommendation: hard-reject** old `DocTicket`-string-only payloads. The brainstorm leans this way; endorsing it. Mechanism is `WorkspaceTicketEnvelope` decode failing with `Malformed` on non-envelope bytes. Pre-1.0, no consumers. Silent fallback re-introduces the very hazard this slice closes.

### Error type placement

**Recommendation: extend `WorkspaceError`, no new top-level error type.** Two new variants:

```rust
WorkspaceError::Policy(PolicyViolation),       // sub-slice 1
WorkspaceError::TicketEnvelope(TicketEnvelopeError), // sub-slice 2
```

Where `PolicyViolation` and `TicketEnvelopeError` are sibling enums in `artel-fs::error` (or `artel-fs::workspace` and `artel-fs::ticket` respectively, re-exported). Inline structured reasons rather than stringly-typed:

```rust
pub enum PolicyViolation {
    DirNotEmpty { root: PathBuf, offending_entries: Vec<PathBuf> }, // top 5 max
    InitFromExistingNotMeaningfulOnJoin,
}

pub enum TicketEnvelopeError {
    Malformed(String),
    UnsupportedVersion(u8),
}
```

`PolicyViolation` carries up to 5 offending entries so the user error is actionable: "refused to host /Users/svozza: contains Documents/, Pictures/, .ssh/, ...". This is the actual UX win of `RequireEmpty`.

### Postcard encoding for `PathRules`

Plain-struct round-trip via `postcard::to_stdvec` / `postcard::from_bytes`. `Mode` is unit-variant enum (externally tagged by default, postcard-compatible). No `#[serde(tag, content)]` anywhere. Honoured.

`PathRules::validate()` runs at originate-time (inside `host_with`, before envelope encode) and at decode-time (inside the joiner's envelope parse). Belt-and-braces: a corrupt host would refuse to publish bad rules; a corrupt wire would be rejected at decode. Both surfaces the same `PathRulesError`.

### Persistence-first constraint

Sub-slice 1's `enforce_attach_policy` runs *before* `WorkspaceNode::spawn`, so a `RequireEmpty` failure leaves zero on-disk state. Already aligned with the project memory.

Sub-slice 2 doesn't persist `PathRules` to disk on the host — they're regenerated from the in-memory `WorkspaceConfig::rules` on every host start. **Open question:** should the host persist them so a restart with a different `WorkspaceConfig` doesn't silently change rules for existing joiners? Brainstorm punted "rule changes after creation" out of scope. Treat `WorkspaceConfig::rules` as *the* source of truth on the host; document loudly that "rules are bound at originate-time and the host promises not to change them across restarts; ensure your `WorkspaceConfig::rules` is identical on resume." Persisting them adds a second source of truth and a "what if they diverge" question we don't need to answer in this slice.

### Two-impls-or-none

`AttachPolicy` is an enum, not a trait — fine.
`Mode` is an enum, not a trait — fine.
`PathRules` is a concrete struct, not a trait — fine.
No new traits introduced. Honoured.

---

## Risks and unknowns (separate from the brainstorm's open questions)

1. **`globset` matching semantics on doc-keys.** Doc keys are NFC-normalised forward-slash strings (`keys.rs` line 36-46). `PathRules::mode_for` should match against the *workspace-relative path* (forward-slash, NFC) for consistency. Verify `globset` matches forward-slash on Unix and that NFC composition pre-applies. Risk: a glob like `"café/**"` written in NFC matches a path that arrived in NFD. Mitigation: do the NFC normalisation once on the input path *and* on the glob at construction time inside `PathRules::matches`. Mirror `keys::path_to_key`'s normalisation.

2. **`canonicalise(root)` happens before `enforce_attach_policy`.** On macOS `/var/foo` becomes `/private/var/foo`. The error message in `PolicyViolation::DirNotEmpty { root, ... }` will show the canonicalised path. That's correct (the tests already deal with this in `round_trip.rs` line 178) but worth a passing comment in the error doc. Low-risk; document.

3. **`AttachPolicy` is always positional, never in `WorkspaceConfig`.** Brainstorm rationale: forces consumers to think at every call site. *But* `WorkspaceConfig` is `Clone`able and the natural way to share configuration. If a consumer keeps a `WorkspaceConfig` around and threads it through both `host_with` and `join_with` calls, they'll thread the policy separately. That's the design. Risk: future consumers might wrap their own config struct that combines them, partially defeating the purpose. Acceptable.

4. **`PathRules` validation surface area.** `validate()` rejects empty/absolute/`..` globs, but it doesn't reject *useless* globs (e.g. a glob that matches nothing meaningful, or one with overlapping rules where the second is unreachable). The `mixed_rules_first_match_wins` test demonstrates first-match-wins as a feature, but consumers hand-writing rules might write order-dependent rules without realising it. Out of scope to detect; mention in `PathRules::rules` doc-comment.

5. **`WorkspaceTicketEnvelope` is a new module under `artel-fs`, not `artel-protocol`.** Locating it in `artel-fs` keeps it close to the consumers and avoids dragging postcard-rule machinery into the protocol crate. But `artel-protocol::ticket` (session ticket) and `artel-fs::ticket` (workspace ticket envelope) being two different "ticket" types is a naming collision. Mitigation: name the new module `workspace_ticket` or the new struct `WorkspaceTicketEnvelope` (proposed) so it's never just "ticket" in conversation. The `TICKET_ACTION` constant (`workspace.ticket`) stays unchanged — that's the wire-action name, not the type name.

6. **macOS FSEvents `Modify(Metadata)` post-unlink path.** `watcher.rs` already handles this. Sub-slice 3 adds rule consultation in `on_removed`. Make sure rule consultation runs *after* the FSEvents-unlink-disguised-as-modify path so a `ReadOnly` file deleted off a `ReadWrite` host doesn't get a tombstone published.

7. **Existing-test surface area is large.** Sub-slice 1 touches many test files. Worth a single mechanical pass plus careful review to make sure `AllowExisting` is used only where the test intentionally pre-seeds and `RequireEmpty` is the default for clean-start tests. Risk of accidentally hiding regressions if the wrong policy gets set everywhere.

8. **`Doc::del` semantics under `ReadOnly` with multiple peers.** If peer A has `ReadWrite` for `secret/x.txt` (via local override or differing rule order — though rules ride the ticket so they're identical, but a corrupt/forked peer is possible), peer A could publish a tombstone. Peer B's `ReadOnly` rule means the applier ignores the tombstone — B's disk keeps the file. This is the cooperative-trust v1 model; flagged in the brainstorm. Document explicitly in the `Mode::ReadOnly` doc-comment that "ReadOnly is honoured by well-behaved peers; cryptographic enforcement is deferred (ADR-001 capabilities)."

---

## Critical files for implementation

- `crates/artel-fs/src/workspace.rs`
- `crates/artel-fs/src/error.rs`
- `crates/artel-fs/src/watcher.rs`
- `crates/artel-fs/src/applier.rs`
- `crates/artel-fs/src/lib.rs`

(New files this plan introduces: `crates/artel-fs/src/rules.rs` and `crates/artel-fs/src/ticket.rs`. Also new test files under `crates/artel-fs/tests/` per each sub-slice.)
