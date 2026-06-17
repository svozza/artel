# ADR-002: No MLS for Tier-1 write-revocation; plain namespace rotation instead

**Status**: Accepted
**Date**: 2026-06-17

## Context

Write capability in artel *is* possession of the iroh-docs `NamespaceSecret`.
Revocation today blocks a peer's connection but does not take the secret away,
so a revoked peer keeps signing valid entries (the "write cut-off does not
hold" gap). The fix is to rotate the namespace so the revoked peer's copy is
worthless.

OpenMLS was the leading candidate: it offers audited member-removal with
post-compromise security, and our host is the sole sequencer that MLS's
delivery-service model requires (RFC 9420 §14), so it fits the host-centric
(Tier-1) shape. A scratch spike (openmls 0.8.1) confirmed the mechanics work:
the host can drive add/remove commits unilaterally and `export_secret` yields a
deterministic 32-byte value every current member — and only current members —
can derive.

## Decision

**Tier-1 rotation mints a fresh random `NamespaceSecret` and distributes it to
survivors over the existing host→peer unicast (`DeliveryFrame::Secret`). No
MLS.**

The spike's integration question — "how does an MLS group key relate to the
`NamespaceSecret`?" — resolved to: it can only ever be a *seed* stretched into
the ed25519 namespace key (the MLS exporter identifies group membership, not
message origin; the namespace key is a signing key whose public half makes
entries verifiable). MLS therefore sits strictly *above* the namespace key and
never replaces it.

Given that, MLS buys little in Tier-1:

- "Hand-rolled rotation" is just `NamespaceSecret::new()` (a fresh ed25519
  keypair — zero novel crypto) plus the unicast channel that **already exists**
  to distribute the secret. MLS removes a non-risk (key generation).
- The real risk in rotation is the *orchestration* (identity decoupling, the
  freeze-drain quiescence barrier). MLS does not help with any of it.
- MLS's headline benefit (post-compromise security / never-transmit-the-secret)
  protects the write *signing* key — but content lives in content-addressed
  blobs the namespace key never encrypts, so forward secrecy on it is near
  worthless.

## Consequences

- MLS is deferred, not rejected forever. It earns its weight only if a group
  key ever encrypts *content* (a real departure from today's model), or in
  Tier-2 P2P where there is no sequencer to distribute a freshly-minted secret
  and group keying without total order becomes necessary.
- No `openmls` dependency enters the workspace. The spike stays throwaway.
