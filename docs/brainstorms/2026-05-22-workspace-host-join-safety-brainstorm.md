---
date: 2026-05-22
topic: workspace-host-join-safety
---

# Workspace host/join safety + configurable policy

## What We're Building

Replace the current implicit "shared bucket, scan-and-publish-whatever-is-here"
behavior of `artel-fs::Workspace` with a configurable policy model that:

1. Prevents the wrong-dir-publish hazard surfaced 2026-05-20 (nearly published a
   home dir during smoke testing).
2. Lets consumers express which paths in a workspace are read-only vs.
   read-write to non-originating peers.
3. Stays neutral on the host/joiner distinction so a future symmetric-P2P
   evolution doesn't require breaking changes to the workspace policy surface.

`artel-fs` remains a broad primitive. No consumer-specific (e.g. agent-harness)
semantics leak in.

## Why This Approach

Considered three approaches for the policy surface:

- **A. Single workspace-wide mode** — too narrow; the obvious near-term consumer
  (agent chat harness) has three distinct path-classes (chat, user inputs,
  agent outputs) that each need different treatment.
- **B. Per-path rules, role-blind** *(chosen)* — covers the chat-harness case
  via path globs without pulling identity machinery into `artel-fs`.
  Role-based distinctions live one layer up in the consumer.
- **C. Per-path + role-aware** — pulls ADR-001's deferred capabilities/auth
  work forward. Over-builds before any consumer needs it.

Trust model is **cooperative trust v1, extensible later**: rules are honored
by well-behaved peers; cryptographic enforcement waits for ADR-001
capabilities. The API surface should make capabilities a non-breaking add.

The bootstrap asymmetry (`Workspace::host` vs. `Workspace::join`) is kept as
named constructors because someone has to be first and bootstrap mechanics
genuinely differ. But post-bootstrap behavior is symmetric, and the policy
surface treats every peer the same — explicitly to keep a path open to
ADR-001 § "Future evolution" symmetric P2P without a breaking change.

## Key Decisions

- **Policy rides in the ticket.** Originator serializes `PathRules` into the
  ticket payload; joiner deserializes at construction and is bound by them
  before the watcher/applier loops start. Bumps `TICKET_VERSION` 2→3; v2
  tickets continue to decode but produce a workspace with the existing
  default-permissive behavior (or are rejected — decide during planning).
  Rationale: tickets are already the "this is what you're joining" object;
  consumers think of join-time policy as ticket-shaped. Inspectable from the
  ticket alone (no live workspace needed). Symmetric-P2P-neutral — a ticket
  in a future symmetric model is just a workspace identifier any peer can
  hand out, and rules-in-ticket doesn't make it more host-shaped than it
  already is.

  Considered doc-bound (originator writes `.artel-fs/rules` at creation,
  joiner reads after sync): more architecturally pure for symmetric P2P
  but introduces a chicken-and-egg window during initial sync, makes
  inspection from outside a live workspace harder, and adds an
  originator-only-writes asymmetry inside the doc anyway. The ticket-bound
  trade-offs landed cleaner once "rules" became concrete.

  Considered independent-each-side: rejected. Silent divergence between
  host and joiner configs is exactly the wrong-dir-class hazard pattern
  repeating at the policy layer.

- **Two-mode policy for v1: `ReadWrite` and `ReadOnly`.** No `Append`. iroh-docs
  is key-latest-value; multi-writer append needs a separate primitive
  (per-peer key prefix is the most likely shape) and belongs in its own slice,
  probably as an `AppendLog` sibling to `Workspace`. Rationale: confirmed with
  user that artel session host-as-sequencer is at a different layer than
  `artel-fs`; sessions sequence chat, the doc does not sequence fs writes.

- **Per-path rules, first-match-wins, with a default.** Rough shape:

  ```rust
  pub struct PathRules {
      pub default: Mode,
      pub rules: Vec<PathRule>, // first-match-wins
  }
  pub struct PathRule { pub glob: String, pub mode: Mode }
  pub enum Mode { ReadWrite, ReadOnly }
  ```

  Watcher consults rules to decide whether to publish a local change outward.
  Applier consults rules to decide whether to write inbound changes to disk.
  The pre-existing exclusion filter (`.git`, `target`, `node_modules`, etc.)
  remains separate — those paths don't sync at all, regardless of rules.

- **Local-dir attachment policy is per-peer, not host-vs-joiner.** Both
  constructors take the same enum, working name:

  ```rust
  pub enum AttachPolicy {
      RequireEmpty,      // refuse if target dir is non-empty
      AllowExisting,     // proceed; bulk-export may overwrite
      InitFromExisting,  // adopt the dir's current contents into the workspace
                         // (only meaningful at originate-time; on join,
                         //  treat as AllowExisting or reject — TBD in plan)
  }
  ```

  Empty `.artel-fs/` directory and filtered paths do **not** count toward
  "non-empty" for `RequireEmpty`. **No default — every caller passes an
  `AttachPolicy` explicitly.** Forces consumers to think about wrong-dir
  risk at every host/join site. Pre-1.0, no external consumers; if always
  specifying proves annoying we add a default (likely `RequireEmpty`)
  later. Cheap to revisit; easier to relax than to tighten.

- **`Workspace::host` and `Workspace::join` stay as the public constructors.**
  They differ in bootstrap (host creates the namespace + writes initial rules;
  join consumes a ticket + reads rules from the doc) but their post-bootstrap
  watcher/applier loops are already symmetric and stay that way.

- **Rule changes after creation: out of scope for this slice.** Rules are
  written once at originate-time. Mutating them later is a separate design
  question (who can change them? does the change propagate atomically? is
  there a transition window?). Defer until a real consumer asks.

## Open Questions

- v2-ticket compatibility: do existing v2 tickets decode into a workspace
  with default-permissive rules (back-compat, but means existing tickets
  silently get the old hazard-prone behavior), or do we hard-reject them
  and require re-issuing? Leaning hard-reject — pre-1.0, no external
  tickets in the wild, and silent fallback re-introduces the very hazard
  this slice is closing.
- Serialization for `PathRules` inside the ticket — postcard for consistency
  with the rest of the ticket payload. Confirm the encoded size stays under
  whatever practical ticket-length ceiling we care about; globs are short
  but a workspace with hundreds of rules could push it.
- `AttachPolicy::InitFromExisting` semantics on join: is this even meaningful,
  or should it be originate-only? Leaning originate-only (joiners don't have
  a canonical tree to seed from), but worth a sentence in the plan.
- Error type shape: do we add new variants to existing error enums or
  introduce a `PolicyError`? Falls out naturally during implementation.

## Deferred (explicitly not in this slice)

- **Append semantics** — separate primitive, likely `artel-fs::AppendLog` with
  per-peer key prefixes. Roadmap entry.
- **Capability/identity enforcement** — `PathRule` gains a `peers:
  PeerSelector` field when ADR-001 capabilities land. Designed-for, not built.
- **Rule mutation after creation** — see open questions.
- **Symmetric P2P session layer** — ADR-001 § "Future evolution". This slice
  only ensures `artel-fs` doesn't make that harder.

## Next Steps

→ `/workflows:plan` for the implementation slicing (probably 2–3 sub-slices:
  ticket v3 + `PathRules` plumbing, `AttachPolicy` + wrong-dir guards,
  watcher rule consultation).
