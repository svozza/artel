# ADR-003: The daemon stays namespace-agnostic; all iroh-docs state lives in artel-fs

**Status**: Accepted
**Date**: 2026-06-17

## Context

Namespace rotation (the Tier-1 write-revocation fix, see ADR-002) spans two
processes. The `SessionId`, gossip topic, capability log, and issued-ticket
ledger are owned by `artel-daemon` (the foundational sequencing layer). The
`NamespaceId`, the iroh-docs document, the blobs, and the rotation itself are
owned by `artel-fs`. A naive implementation would add `current_namespace` and
`namespace_epoch` to the daemon's `SessionRecord` so the rotation state sits
next to the cap/ticket state that drives it.

Verified at decision time: **neither `artel-protocol` nor `artel-daemon`
depends on `iroh-docs`, and neither ever names a `NamespaceId`.** The daemon
handles the `NamespaceSecret` only as opaque `[u8; 32]` it couriers over the
upgrade unicast, and the `WorkspaceTicketEnvelope` as opaque bytes it
persists and forwards without decoding.

## Decision

**All namespace and rotation state lives in `artel-fs` / `state_dir`. The
daemon must not gain an `iroh-docs` dependency or learn what a `NamespaceId`
is.**

- Genesis namespace (`doc-id`, write-once), current namespace, and the rotation
  logic are `artel-fs` concerns.
- The daemon keeps couriering the opaque `[u8; 32]` secret over the existing
  `DeliveryFrame` and persisting the opaque `WorkspaceTicketEnvelope`.
- `namespace_epoch` travels *inside* the already-opaque envelope; the daemon
  persists and forwards it but never interprets it.

## Consequences

- Putting `NamespaceId` into `SessionRecord` is explicitly rejected — it would
  pull an iroh-docs dependency and a filesystem-layer concept below the
  foundational line, the exact scope-creep ADR-001 warns against ("a daemon
  that owns everything collaborative risks becoming a feature warehouse").
- The boundary is load-bearing and easy to erode by accident; this ADR exists
  so a future change that "just adds the namespace to the session record" is
  recognized as a deliberate line-crossing, not a convenience.
