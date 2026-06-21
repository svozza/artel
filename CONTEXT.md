# artel

A peer-to-peer collaborative filesystem substrate built on iroh. A **host**
shares a workspace directory; **joiners** mirror it. All inter-daemon traffic
rides iroh-gossip, with one sanctioned exception: host→peer unicast of
session-key material. The host is the sole sequencer for its sessions.

## Language

### Write authority

**NamespaceSecret**:
The iroh-docs document **write key** — an ed25519 *signing* key. Possession of
it *is* write capability: any holder can author valid, verifiable doc entries.
One symmetric secret shared by all RW peers. Its public half is the
`NamespaceId`. Revocation today does *not* take it away — that is the gap.
_Avoid_: write key (ambiguous), doc secret.

**NamespaceId**:
The public half of the `NamespaceSecret` (32 bytes) and the identity of an
iroh-docs document. Not secret. Changing the secret changes the id, i.e. it is
a *different document*. After rotation there are two of relevance — see
[[Genesis namespace]] and [[Current namespace]].
_Avoid_: doc id (overloaded with the on-disk `doc-id` file).

**Genesis namespace**:
The `NamespaceId` a session was *born* with, persisted write-once in the
`doc-id` file and **never rewritten on rotation**. It is the stable root the
`SessionId` derivation reads (`SessionId = session_id_for(genesis_ns)`,
recomputed every start — the pure derivation is preserved verbatim across
rotation). Distinct from the document currently holding content.

**Current namespace**:
The `NamespaceId` the workspace is *currently* writing to — a **mutable
session attribute** persisted separately from `doc-id`, advanced on each
rotation. Decoupled from `SessionId` so rotation never changes the session id,
gossip topic, or any issued ticket.

**AuthorId**:
A **per-node** ed25519 keypair that signs each individual entry a node writes
(`doc.set_bytes(author, …)`). Distinct from the `NamespaceSecret`: never
shared, not the revocation gap. Identifies *which node* authored an entry.
**Seeded from the same bytes as the workspace endpoint key**, so `AuthorId`
== `endpoint_id` and `peer_map` resolves `entry.author` → daemon `PeerId` with
no announcement. The key reuse is proven safe — a TLS `CertificateVerify`
payload (64 `0x20` bytes + context string) can never collide with an
`entry.to_vec()` (32-byte namespace pubkey prefix). Replaces iroh-docs'
`author_default()`.

**Author binding**:
The fact that `entry.author` resolves to the `PeerId` whose capability the
session tracks — established by same-seed (see `AuthorId`), not a maintained
map. Lets carry-forward and (future) ingest enforcement reason about *who*
authored a doc entry in cap terms.

**Namespace rotation**:
Minting a fresh `NamespaceSecret` (hence a new `NamespaceId`, hence a new
document), distributing it to survivors but never the revoked peer, and
re-pointing the `path→hash` map at it. Cheap because file bytes are
content-addressed in iroh-blobs and never move. The fix for write cut-off.
Driven by the host under a **freeze-drain barrier**: the host re-publishes the
quiesced old doc's latest-per-key snapshot under its **own author** into the
new namespace (the entry author signature covers the `NamespaceId`, so entries
cannot be copied across namespaces — they must be re-authored, and only a key's
holder can sign). The revoked peer's entries are filtered out at snapshot via
the **author binding**.

**Freeze-drain barrier**:
The rotation's quiescence guarantee. **v1 ships the epoch-gated (light)
shape**: there is no host-coordinated global freeze/ack handshake — the "freeze"
is each survivor's *own* reimport pausing its watcher (see [[Current
namespace]] / reimport). On Evict the host snapshots + rotates immediately and
ships `{new_secret, namespace_epoch}` over the existing `DeliverUpgrade`
unicast; each survivor, on seeing a higher epoch, reimports (pause watcher →
swap doc → respawn). The one re-publisher is the host, so there is no cross-doc
timestamp reconciliation. **Accepted loss**: a *trusted* survivor's in-flight
write in the sub-second window between the host's snapshot and that survivor's
reimport lands in the abandoned old doc — identical to a write made during a
momentary partition, which the system already recovers from via
`namespace_epoch` on reconnect. This is a durability edge on an honest peer, not
a security hole: the *evicted* peer is fully cut (never gets the new secret,
`PeerFilter` blocks it). The heavy host-coordinated freeze/ack barrier remains
an **additive future upgrade** (the epoch/secret wire is unchanged; only the
snapshot gets gated behind acks) if a forcing function appears — a workload
where continuous-writer survivors can't tolerate even sub-second loss.

**namespace_epoch**:
A monotonic counter carried (opaque) in the workspace ticket envelope, bumped
on each rotation, so a peer detects it is on a stale namespace and re-imports.

**Rejoin re-delivery** (offline-across-rotation recovery):
The recovery path for an RW member that was *offline* when a rotation happened
(so it missed the live-only secret unicast AND its mirror replays its own stale
genesis ticket). On the returning peer's `NODE_ID` re-announce, the host's
cap-listener re-delivers **both** the current `NamespaceSecret` (`publish_upgrade`)
**and** the current rotated Write ticket (`publish_rotate` → the joiner's
`SurvivorRotate`/`reimport_namespace` consumer), gated on `has_rw`, idempotent
(monotonic-epoch guard drops a no-op; inert at genesis epoch 0). Complemented by
a daemon-side lazy gossip re-subscribe: a reloaded `Remote` mirror re-subscribes
its topic on its first post-restart send, so the `NODE_ID` announce reaches the
host at all. Subsumes the "offline promotion" case (a peer promoted to RW while
offline holds no prior secret — only the host knows it is now RW). `NODE_ID` is
the ordering-safe trigger: a joiner emits it only *after* its cap-listener is
live, so the live-only re-delivery cannot outrun its receiver. Stays entirely in
`artel-fs` (daemon couriers opaque bytes) per [[Namespace-agnostic daemon]].

Two correctness conditions this path depends on:
- **Host-restart cell re-seed.** The rotated Write ticket the host re-delivers
  is read from a shared `(write_ticket, namespace_epoch)` cell, seeded on
  `host_with` and overwritten on each rotation. It MUST seed the epoch from the
  value recovered from disk (the persisted `namespace_epoch`), not a hard-coded
  0 — a returning host that had rotated otherwise re-delivers `epoch 0`, which a
  returning member's monotonic-epoch guard drops as stale, silently stranding it
  on the abandoned namespace.
- **De-storm.** `NODE_ID` is a *logged, replayed* message, so a host cap-listener
  restart replays every historical announce and would re-fan-out a unicast to
  every RW peer that ever announced. A per-peer "epoch already re-delivered"
  high-water mark suppresses the storm without suppressing a genuine recovery
  (claimed up front so concurrent replays collapse; rolled back on delivery
  failure so a transient failure never durably blocks a later retry).

### Layer boundary (invariant)

**Namespace-agnostic daemon**:
Neither `artel-protocol` nor `artel-daemon` depends on `iroh-docs`, and they
never name a `NamespaceId`. The daemon handles the `NamespaceSecret` only as
opaque `[u8; 32]` it couriers over the upgrade unicast, and the
`WorkspaceTicketEnvelope` as opaque bytes it persists/forwards. **All
iroh-docs concepts — namespaces, the doc, blobs, rotation — live in `artel-fs`
and `state_dir`.** Rotation must not push any of this below the fs line: the
`namespace_epoch` travels *inside* the already-opaque envelope; the daemon
never interprets it. This boundary is load-bearing — do not give the daemon an
`iroh-docs` dependency.

### Capability & revocation

**Capability**:
A peer's current authority in a session — `ReadWrite` or `Read`. Derived by
replaying host-signed `Capability` grant/revoke messages in seq order. Absent
peer ⇒ `Read` floor.

**Demote**:
A *cooperative* RW→Read downgrade of a **trusted** peer. Wire form
`Grant{peer, Read}` (peer stays connected as read-only). It is **not** a
cryptographic write cut-off — the peer keeps the live `NamespaceSecret`, has no
joiner-side write check, and its watcher would keep publishing. Enforcement is
*voluntary*: the **downgrade notification** is load-bearing — the demoted
daemon honours it by halting its own watcher. Cheap, no rotation; safe only
under a cooperative threat model.

**Evict**:
An *adversarial* removal. Wire form `Revoke{peer}` (`PeerFilter` blocks the
connection). The only true **cryptographic** write cut-off: `PeerFilter` block
+ freeze-drain namespace rotation, so the peer's retained `NamespaceSecret`
becomes worthless. Use when the peer is not trusted to self-halt. On a `Revoke`
the host also drops the peer from the daemon's durable session *membership* (a
host-authority `RemoveSessionMember` IPC — distinct from a peer's own
`LeaveSession`): membership is the gate on the gossip log **Replay**, and a
capability `Revoke` alone leaves membership intact, so without this an evicted
peer could still pull the session-log replay on an announce-less re-subscribe
(see [[Rejoin re-delivery]]). The daemon is told only to drop a member — the
"`Revoke` means drop membership" decision stays in `artel-fs`
([[Namespace-agnostic daemon]]). Chatter-only residual closed; reads/writes were
already denied by `PeerFilter` + the crypto cut. A later re-admission still
requires a fresh `JoinAnnouncement` with an admissible ticket.

**Revoke**:
The `Revoke{peer}` wire verb underlying **Evict**. Historically (pre-rotation)
it suspended *delivery* only, not *writing* — the gap rotation closes.

**Write cut-off**:
Stopping a revoked peer from *producing* valid state. Distinct from **read
cut-off** (stopping it pulling new state — already works) and **notification**
(telling it — today it is never told; the silent one-way partition).

**Downgrade notification**:
A host→peer unicast telling a peer it was demoted, mirroring the existing
RW-promotion `UPGRADE_ACTION`. **Load-bearing for [[Demote]]** — it is the
mechanism a cooperative demoted daemon uses to halt its own watcher (a
voluntary write-stop), not merely a UI signal. Independently shippable.

### Tiers

**Tier 1 (host-centric)**:
Revocation that leans on the host being the sole sequencer — the host drives
rotation and distributes the new secret. The pragmatic near-term path.

**Tier 2 (P2P / project-at-merge)**:
Sequencerless revocation: every peer rejects revoked authors at merge time via
a causal-DAG high-water mark. Requires an authority model, monotonic
project-at-merge, and convergence under partition. Supersedes Tier 1 but is the
research-frontier end-state.

### MLS (evaluated, deferred)

**MLS exporter secret**:
A deterministic per-epoch symmetric value every *current* MLS group member —
and only current members — can derive via `export_secret`. It identifies group
*membership*, not message *origin*, so it can only ever be a **seed** stretched
into a `NamespaceSecret`; it sits *above* the namespace key and never *is* it.
MLS member-removal rotates this on `remove_members`. Deferred in Tier 1 — see
`docs/adr/` — because its post-compromise security protects the write *signing*
key, an asset that guards content-addressed blobs the key never encrypts. It
earns its weight only if a group key ever encrypts *content*, or in Tier 2.
