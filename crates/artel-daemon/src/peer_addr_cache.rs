//! On-disk cache of peer addrs the daemon has learned, persisted
//! across daemon restarts.
//!
//! ## Why
//!
//! The daemon installs a [`iroh::address_lookup::memory::MemoryLookup`]
//! (`addr_hint`) at startup
//! and registers it in iroh's address-lookup chain. When a peer's
//! addr lands in `addr_hint`, iroh's resolver chain (consulted by
//! every dial — including iroh-docs's internal `LiveActor` after a
//! host restart) finds it synchronously without waiting on
//! pkarr/DNS propagation.
//!
//! Pre-fix, `addr_hint` was rebuilt fresh at every daemon startup,
//! so a host restart lost everything the daemon had learned. The
//! failing test was `host_restart_live_writes_n0` — alice's
//! post-restart writes never reached bob because iroh-docs read
//! id-only `EndpointAddr`s from its persistent doc store, skipped
//! its internal `memory_lookup` seeding (`engine/live.rs:472`), and
//! raced n0 pkarr/DNS to find bob. The race lost.
//!
//! See
//! `docs/brainstorms/2026-05-29-host-restart-peer-addr-cache-brainstorm.md`.
//!
//! ## Shape
//!
//! - **Source of truth**: snapshot iroh's per-peer state at
//!   graceful daemon shutdown via [`iroh::Endpoint::remote_info`].
//! - **Storage**: single per-daemon file (`peer_addrs.postcard`)
//!   alongside the daemon's `iroh.key`. Atomic write (tmp + fsync +
//!   rename) so a crash mid-write never corrupts the live file.
//! - **Format**: postcard, externally-tagged versioned envelope
//!   (`PeerAddrCache::V1(_)`). Future variants land cleanly without
//!   breaking forward-compat.
//! - **Pruning**: size-cap at write — only the most-recently-seen
//!   `MAX_ENTRIES` peers persist, ranked by snapshot timestamp.
//! - **Failure modes are non-fatal**: read errors at startup log and
//!   yield an empty cache; write errors at shutdown log and don't
//!   block daemon exit. Per the headless-first-class policy, the
//!   cache is a freshness optimisation, not a correctness primitive.

#![allow(clippy::redundant_pub_crate)]

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use artel_protocol::{PeerId, WireEndpointAddr};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Mode applied to the on-disk cache file. The cache contains peer
/// addr-shapes — not secrets, but no reason to make it world-readable
/// either; mirror the iroh.key convention living in the same dir.
const CACHE_MODE: u32 = 0o600;

/// Maximum entries persisted per snapshot. Keeps the file size and
/// `MemoryLookup` memory footprint bounded over a long-running daemon.
/// 256 covers a several-hundred-peer mesh comfortably; at write time
/// the most-recently-seen entries are kept and the oldest evicted
/// (LRU eviction).
const MAX_ENTRIES: usize = 256;

/// Versioned on-disk envelope. Externally-tagged so future variants
/// can be added without breaking forward-compat. See
/// `feedback_postcard_externally_tagged_enums` in memory.
#[derive(Debug, Serialize, Deserialize)]
enum PeerAddrCacheFile {
    V1(PeerAddrCacheV1),
}

#[derive(Debug, Serialize, Deserialize)]
struct PeerAddrCacheV1 {
    entries: Vec<CacheEntry>,
}

/// One peer's addr snapshot. The address fields mirror
/// [`WireEndpointAddr`] so the on-disk format stays iroh-free; we
/// convert to/from `iroh::EndpointAddr` at the cache boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CacheEntry {
    pub(crate) addr: WireEndpointAddr,
    /// Wall-clock time at snapshot. Used to rank entries when the
    /// file is over `MAX_ENTRIES` — most-recent kept, oldest dropped.
    pub(crate) last_seen_unix_secs: u64,
}

/// Handle to the on-disk peer-addr cache.
#[derive(Debug)]
pub(crate) struct PeerAddrCache {
    path: PathBuf,
    max_entries: usize,
}

impl PeerAddrCache {
    /// Create a handle pointing at `path`. The file does not need to
    /// exist; [`load`](Self::load) returns an empty list if missing.
    pub(crate) const fn new(path: PathBuf) -> Self {
        Self {
            path,
            max_entries: MAX_ENTRIES,
        }
    }

    /// Load entries from disk. Never errors; logs and returns an
    /// empty list on any failure (file missing, decode error,
    /// unknown version). The cache is a freshness optimisation — a
    /// load failure means the next dial falls back to pkarr/DNS,
    /// which is the existing behaviour pre-cache.
    pub(crate) fn load(&self) -> Vec<CacheEntry> {
        let bytes = match fs::read(&self.path) {
            Ok(b) => b,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                debug!(path = %self.path.display(), "peer_addr_cache: no file, starting empty");
                return Vec::new();
            }
            Err(err) => {
                warn!(path = %self.path.display(), error = %err, "peer_addr_cache: read failed");
                return Vec::new();
            }
        };
        match postcard::from_bytes::<PeerAddrCacheFile>(&bytes) {
            Ok(PeerAddrCacheFile::V1(v1)) => {
                debug!(
                    path = %self.path.display(),
                    count = v1.entries.len(),
                    "peer_addr_cache: loaded entries"
                );
                v1.entries
            }
            Err(err) => {
                warn!(
                    path = %self.path.display(),
                    error = %err,
                    "peer_addr_cache: decode failed; ignoring"
                );
                Vec::new()
            }
        }
    }

    /// Persist `entries` to disk. Never errors; logs on failure.
    /// Drops entries with no relay url AND no direct addrs (id-only
    /// — they provide no value to `MemoryLookup`) and caps the file at
    /// `MAX_ENTRIES`, keeping the most-recently-seen.
    pub(crate) fn save(&self, entries: Vec<CacheEntry>) {
        let mut filtered: Vec<CacheEntry> = entries
            .into_iter()
            .filter(|e| !e.addr.relay_url.is_empty() || !e.addr.direct_addrs.is_empty())
            .collect();
        // Newest first; truncate keeps the head.
        filtered.sort_by_key(|e| std::cmp::Reverse(e.last_seen_unix_secs));
        filtered.truncate(self.max_entries);

        let envelope = PeerAddrCacheFile::V1(PeerAddrCacheV1 { entries: filtered });
        let bytes = match postcard::to_stdvec(&envelope) {
            Ok(b) => b,
            Err(err) => {
                warn!(error = %err, "peer_addr_cache: encode failed");
                return;
            }
        };
        if let Err(err) = write_atomic(&self.path, &bytes) {
            warn!(path = %self.path.display(), error = %err, "peer_addr_cache: write failed");
            return;
        }
        let count = match postcard::from_bytes::<PeerAddrCacheFile>(&bytes) {
            Ok(PeerAddrCacheFile::V1(v1)) => v1.entries.len(),
            Err(_) => 0,
        };
        debug!(
            path = %self.path.display(),
            count,
            "peer_addr_cache: snapshot written"
        );
    }
}

/// Convert an `iroh::EndpointAddr` to a [`CacheEntry`] stamped at
/// the current wall-clock time.
pub(crate) fn entry_from_iroh(addr: &EndpointAddr) -> CacheEntry {
    let peer_id = PeerId::from_bytes(*addr.id.as_bytes());
    let relay_url = addr
        .relay_urls()
        .next()
        .map(ToString::to_string)
        .unwrap_or_default();
    let direct_addrs: BTreeSet<SocketAddr> = addr.ip_addrs().copied().collect();
    let last_seen_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    CacheEntry {
        addr: WireEndpointAddr {
            peer_id,
            relay_url,
            direct_addrs,
        },
        last_seen_unix_secs,
    }
}

/// Convert a [`CacheEntry`] back into an `iroh::EndpointAddr`
/// suitable for `MemoryLookup::add_endpoint_info`. Returns an error
/// only when the persisted addr is structurally invalid (bad id
/// bytes, malformed relay URL); callers should log and skip.
pub(crate) fn iroh_addr_from_entry(entry: &CacheEntry) -> Result<EndpointAddr, String> {
    let endpoint_id = iroh::EndpointId::from_bytes(entry.addr.peer_id.as_bytes())
        .map_err(|e| format!("peer id: {e}"))?;
    let mut iroh_addr = EndpointAddr::new(endpoint_id);
    if !entry.addr.relay_url.is_empty() {
        let url = iroh::RelayUrl::from_str(&entry.addr.relay_url)
            .map_err(|e| format!("relay_url: {e}"))?;
        iroh_addr = iroh_addr.with_relay_url(url);
    }
    for direct in &entry.addr.direct_addrs {
        iroh_addr = iroh_addr.with_ip_addr(*direct);
    }
    Ok(iroh_addr)
}

/// Atomic write — tmp + fsync + chmod 0600 + rename. Mirrors
/// `iroh_key.rs::write_atomic`: copying the pattern is intentional —
/// the helper there is private and the cache file lives in the same
/// dir with the same threat profile. Crash-safe: a partially-written
/// tmp file never replaces the live one.
fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        let mut perms = f.metadata()?.permissions();
        perms.set_mode(CACHE_MODE);
        fs::set_permissions(&tmp, perms)?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::os::unix::fs::MetadataExt;

    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    /// Generate a deterministic peer id from a single seed byte that
    /// is a VALID ed25519 public key (constant-byte ids like [0x42;32]
    /// don't satisfy the curve check). Done by deriving the pubkey
    /// from a 32-byte secret seed.
    fn valid_peer_id(seed: u8) -> PeerId {
        let secret = iroh::SecretKey::from_bytes(&[seed; 32]);
        let pk_bytes = *secret.public().as_bytes();
        PeerId::from_bytes(pk_bytes)
    }

    fn entry(peer_byte: u8, last_seen: u64) -> CacheEntry {
        CacheEntry {
            addr: WireEndpointAddr {
                peer_id: valid_peer_id(peer_byte),
                relay_url: format!("https://relay-{peer_byte}.example.com."),
                direct_addrs: {
                    let mut s = BTreeSet::new();
                    s.insert(format!("192.0.2.{peer_byte}:9999").parse().unwrap());
                    s
                },
            },
            last_seen_unix_secs: last_seen,
        }
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let dir = tempdir().unwrap();
        let cache = PeerAddrCache::new(dir.path().join("peer_addrs.postcard"));
        assert!(cache.load().is_empty());
    }

    #[test]
    fn load_corrupt_file_returns_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("peer_addrs.postcard");
        fs::write(&path, b"not a valid postcard envelope").unwrap();

        let cache = PeerAddrCache::new(path);
        assert!(cache.load().is_empty());
    }

    #[test]
    fn save_then_load_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("peer_addrs.postcard");
        let cache = PeerAddrCache::new(path);

        let mut want = vec![entry(0x01, 1000), entry(0x02, 2000), entry(0x03, 3000)];
        cache.save(want.clone());

        let got = cache.load();
        // Save sorts by last_seen desc; order should be 03, 02, 01.
        want.sort_by_key(|c| std::cmp::Reverse(c.last_seen_unix_secs));
        assert_eq!(got, want);
    }

    #[test]
    fn save_truncates_to_max_entries_keeping_most_recent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("peer_addrs.postcard");
        let mut cache = PeerAddrCache::new(path);
        cache.max_entries = 3;

        // 5 entries with monotonic last_seen; keep the 3 newest.
        let entries = vec![
            entry(0x01, 100),
            entry(0x02, 200),
            entry(0x03, 300),
            entry(0x04, 400),
            entry(0x05, 500),
        ];
        cache.save(entries);

        let got = cache.load();
        assert_eq!(got.len(), 3);
        let kept: BTreeSet<PeerId> = got.iter().map(|e| e.addr.peer_id).collect();
        let expected: BTreeSet<PeerId> =
            [0x03, 0x04, 0x05].into_iter().map(valid_peer_id).collect();
        assert_eq!(
            kept, expected,
            "should retain the 3 highest last_seen entries"
        );
    }

    #[test]
    fn save_drops_id_only_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("peer_addrs.postcard");
        let cache = PeerAddrCache::new(path);

        let useful = entry(0x01, 1000);
        let id_only = CacheEntry {
            addr: WireEndpointAddr::id_only(PeerId::from_bytes([0x02; 32])),
            last_seen_unix_secs: 2000,
        };
        cache.save(vec![useful.clone(), id_only]);

        let got = cache.load();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].addr.peer_id, useful.addr.peer_id);
    }

    #[test]
    fn save_creates_file_at_chmod_0600() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("peer_addrs.postcard");
        let cache = PeerAddrCache::new(path.clone());
        cache.save(vec![entry(0x01, 1000)]);

        let mode = fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(mode, CACHE_MODE);
    }

    #[test]
    fn save_then_load_through_iroh_round_trip() {
        // Pin that the wire shape cleanly round-trips through
        // iroh::EndpointAddr — the production path is
        // entry → iroh → MemoryLookup → resolver → dial.
        let dir = tempdir().unwrap();
        let path = dir.path().join("peer_addrs.postcard");
        let cache = PeerAddrCache::new(path);

        let original = entry(0x42, 12345);
        cache.save(vec![original.clone()]);
        let loaded = cache.load();
        assert_eq!(loaded.len(), 1);

        let iroh_addr = iroh_addr_from_entry(&loaded[0]).expect("conversion");
        assert_eq!(iroh_addr.id.as_bytes(), original.addr.peer_id.as_bytes());
        // Iroh's RelayUrl normalises to include a trailing slash;
        // confirm the host roundtrips by parsing both sides.
        let parsed_original = iroh::RelayUrl::from_str(&original.addr.relay_url).unwrap();
        let relay_urls: Vec<_> = iroh_addr.relay_urls().cloned().collect();
        assert_eq!(relay_urls, vec![parsed_original]);
        let ip_addrs: BTreeSet<SocketAddr> = iroh_addr.ip_addrs().copied().collect();
        assert_eq!(ip_addrs, original.addr.direct_addrs);
    }

    #[test]
    fn entry_from_iroh_round_trip() {
        let secret = iroh::SecretKey::from_bytes(&[0x77; 32]);
        let id: iroh::EndpointId = secret.public();
        let relay = iroh::RelayUrl::from_str("https://relay.example.com.").unwrap();
        let iroh_addr = EndpointAddr::new(id)
            .with_relay_url(relay)
            .with_ip_addr("198.51.100.7:5555".parse().unwrap());

        let entry = entry_from_iroh(&iroh_addr);
        assert_eq!(entry.addr.peer_id.as_bytes(), id.as_bytes());
        assert_eq!(entry.addr.relay_url, "https://relay.example.com./");
        assert!(
            entry
                .addr
                .direct_addrs
                .contains(&"198.51.100.7:5555".parse().unwrap())
        );
    }

    #[test]
    fn save_atomic_replace_does_not_corrupt_on_existing_file() {
        // Successive saves should fully replace, not append/garble.
        let dir = tempdir().unwrap();
        let path = dir.path().join("peer_addrs.postcard");
        let cache = PeerAddrCache::new(path);

        cache.save(vec![entry(0x01, 100), entry(0x02, 200)]);
        cache.save(vec![entry(0x99, 999)]);

        let got = cache.load();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].addr.peer_id, valid_peer_id(0x99));
    }
}
