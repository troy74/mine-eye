//! Disk-backed LRU cache for expensive external elevation and imagery fetches.
//!
//! # Layout
//! ```text
//! {root}/
//!   dem/
//!     {sha256_hex}.bin        ← raw response bytes
//!     {sha256_hex}.meta.json  ← CacheMeta (created_at_s, ttl_s, size_bytes, key)
//!   imagery/
//!     ...same structure...
//! ```
//!
//! # Eviction
//! After every `put`, if the total size in that category exceeds `max_bytes`,
//! the oldest entries (by `created_at_s`) are removed until the total falls
//! below the limit.  Errors during eviction are silently ignored.
//!
//! # Thread safety
//! The cache performs purely async file I/O and has no in-process state beyond
//! the `root` path and `max_bytes` limit, so it is safe to use from multiple
//! tasks simultaneously (last-writer-wins on concurrent writes to the same key).

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;

// ── TTL constants ─────────────────────────────────────────────────────────────

/// 90 days — elevation grids (OpenTopography / Open-Meteo).
/// DEM data at the resolution we use (COP-30, ~30 m) essentially never changes.
pub const DEM_TTL_S: u64 = 90 * 24 * 3_600;

/// 30 days — Open-Meteo elevation point batches.
pub const OPEN_METEO_TTL_S: u64 = 30 * 24 * 3_600;

/// 7 days — imagery / tile reference contracts.
#[allow(dead_code)]
pub const IMAGERY_TTL_S: u64 = 7 * 24 * 3_600;

/// Default hard cap across **all** categories combined.
pub const DEFAULT_MAX_BYTES: u64 = 2 * 1_024 * 1_024 * 1_024; // 2 GiB

// ── Internal metadata sidecar ─────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct CacheMeta {
    created_at_s: u64,
    ttl_s: u64,
    size_bytes: u64,
    /// Human-readable cache key (before hashing) — useful for debugging.
    key: String,
}

// ── TileCache ─────────────────────────────────────────────────────────────────

/// Simple disk-backed LRU cache.
pub struct TileCache {
    root: PathBuf,
    max_bytes: u64,
}

impl TileCache {
    /// Create a cache rooted at `root` with a hard byte cap.
    pub fn new(root: PathBuf, max_bytes: u64) -> Self {
        Self { root, max_bytes }
    }

    /// Convenience constructor: derive the cache root from the worker's
    /// `artifact_root` by stepping one level up, then appending `tile-cache`.
    ///
    /// ```text
    /// artifact_root = /data/artifacts  →  cache root = /data/tile-cache
    /// ```
    pub fn from_artifact_root(artifact_root: &Path) -> Self {
        let base = artifact_root.parent().unwrap_or(artifact_root);
        Self::new(base.join("tile-cache"), DEFAULT_MAX_BYTES)
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn sha256_hex(key: &str) -> String {
        let mut h = Sha256::new();
        h.update(key.as_bytes());
        hex::encode(h.finalize())
    }

    fn data_path(&self, category: &str, hk: &str) -> PathBuf {
        self.root.join(category).join(format!("{hk}.bin"))
    }

    fn meta_path(&self, category: &str, hk: &str) -> PathBuf {
        self.root.join(category).join(format!("{hk}.meta.json"))
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Look up `key` in `category`.  Returns the raw bytes if the entry exists
    /// and has not expired.  Returns `None` on cache miss, I/O error, or TTL
    /// expiry (and lazily removes expired files).
    pub async fn get(&self, category: &str, key: &str) -> Option<Vec<u8>> {
        let hk = Self::sha256_hex(key);
        let meta_bytes = fs::read(self.meta_path(category, &hk)).await.ok()?;
        let meta: CacheMeta = serde_json::from_slice(&meta_bytes).ok()?;

        let now_s = unix_now_s();
        if now_s > meta.created_at_s.saturating_add(meta.ttl_s) {
            // Entry is expired — remove lazily
            let _ = fs::remove_file(self.meta_path(category, &hk)).await;
            let _ = fs::remove_file(self.data_path(category, &hk)).await;
            return None;
        }

        fs::read(self.data_path(category, &hk)).await.ok()
    }

    /// Store `data` under `key` in `category` with the given TTL, then run
    /// LRU eviction for that category if the total bytes exceed `max_bytes`.
    pub async fn put(&self, category: &str, key: &str, data: &[u8], ttl_s: u64) {
        let hk = Self::sha256_hex(key);
        let dir = self.root.join(category);
        if fs::create_dir_all(&dir).await.is_err() {
            return;
        }
        if fs::write(self.data_path(category, &hk), data).await.is_err() {
            return;
        }
        let meta = CacheMeta {
            created_at_s: unix_now_s(),
            ttl_s,
            size_bytes: data.len() as u64,
            key: key.to_string(),
        };
        if let Ok(b) = serde_json::to_vec(&meta) {
            let _ = fs::write(self.meta_path(category, &hk), b).await;
        }
        // Best-effort eviction; errors are silently swallowed.
        self.evict_lru(category).await;
    }

    /// Delete all entries across all categories whose TTL has elapsed.
    /// Can be called periodically on a background task if desired.
    #[allow(dead_code)]
    pub async fn purge_expired(&self) {
        for category in &["dem", "imagery"] {
            let dir = self.root.join(category);
            let mut rd = match fs::read_dir(&dir).await {
                Ok(r) => r,
                Err(_) => continue,
            };
            let now_s = unix_now_s();
            while let Ok(Some(e)) = rd.next_entry().await {
                let p = e.path();
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
                if !name.ends_with(".meta.json") {
                    continue;
                }
                let hk = name.trim_end_matches(".meta.json").to_string();
                if let Ok(mb) = fs::read(&p).await {
                    if let Ok(m) = serde_json::from_slice::<CacheMeta>(&mb) {
                        if now_s > m.created_at_s.saturating_add(m.ttl_s) {
                            let _ = fs::remove_file(self.data_path(category, &hk)).await;
                            let _ = fs::remove_file(&p).await;
                        }
                    }
                }
            }
        }
    }

    // ── LRU eviction ──────────────────────────────────────────────────────────

    async fn evict_lru(&self, category: &str) {
        let dir = self.root.join(category);
        let mut rd = match fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(_) => return,
        };

        // Collect all meta entries: (created_at_s, size_bytes, data_path, meta_path)
        let mut entries: Vec<(u64, u64, PathBuf, PathBuf)> = Vec::new();
        let mut total: u64 = 0;

        while let Ok(Some(e)) = rd.next_entry().await {
            let p = e.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
            if !name.ends_with(".meta.json") {
                continue;
            }
            let hk = name.trim_end_matches(".meta.json").to_string();
            if let Ok(mb) = fs::read(&p).await {
                if let Ok(m) = serde_json::from_slice::<CacheMeta>(&mb) {
                    total += m.size_bytes;
                    entries.push((m.created_at_s, m.size_bytes, self.data_path(category, &hk), p));
                }
            }
        }

        if total <= self.max_bytes {
            return;
        }

        // Delete oldest first until under the cap.
        entries.sort_by_key(|(ts, _, _, _)| *ts);
        for (_, size, dp, mp) in entries {
            if total <= self.max_bytes {
                break;
            }
            let _ = fs::remove_file(&dp).await;
            let _ = fs::remove_file(&mp).await;
            total = total.saturating_sub(size);
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn unix_now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
