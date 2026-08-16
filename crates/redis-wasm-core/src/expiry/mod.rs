//! Expiry/TTL management

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use parking_lot::RwLock;

/// Expiry manager for handling key TTLs
#[derive(Clone)]
pub struct ExpiryManager {
    /// Key -> expiry timestamp (milliseconds since epoch)
    expiries: DashMap<String, u64>,
    /// Background cleanup interval
    cleanup_interval: Duration,
}

impl ExpiryManager {
    /// Create a new expiry manager
    pub fn new() -> Self {
        Self {
            expiries: DashMap::new(),
            cleanup_interval: Duration::from_millis(100),
        }
    }

    /// Create with custom cleanup interval
    pub fn with_interval(cleanup_interval: Duration) -> Self {
        Self {
            expiries: DashMap::new(),
            cleanup_interval,
        }
    }

    /// Set expiry in seconds from now
    pub fn set_expiry(&self, key: &str, seconds: u64) {
        let now_ms = current_time_ms();
        self.expiries.insert(key.to_string(), now_ms + seconds * 1000);
    }

    /// Set expiry in milliseconds from now
    pub fn set_expiry_ms(&self, key: &str, milliseconds: u64) {
        let now_ms = current_time_ms();
        self.expiries.insert(key.to_string(), now_ms + milliseconds);
    }

    /// Set absolute expiry timestamp (milliseconds since epoch)
    pub fn set_expiry_at(&self, key: &str, expiry_ms: u64) {
        self.expiries.insert(key.to_string(), expiry_ms);
    }

    /// Get expiry timestamp
    pub fn get_expiry_ms(&self, key: &str) -> Option<u64> {
        self.expiries.get(key).map(|v| *v)
    }

    /// Check if key is expired
    pub fn is_expired(&self, key: &str) -> bool {
        if let Some(expiry_ms) = self.expiries.get(key) {
            let now_ms = current_time_ms();
            if now_ms >= *expiry_ms {
                return true;
            }
        }
        false
    }

    /// Get TTL in seconds
    pub fn ttl_seconds(&self, key: &str) -> Option<i64> {
        self.expiries.get(key).map(|expiry_ms| {
            let now_ms = current_time_ms();
            if now_ms >= *expiry_ms {
                0
            } else {
                ((*expiry_ms - now_ms) / 1000) as i64
            }
        })
    }

    /// Get TTL in milliseconds
    pub fn ttl_ms(&self, key: &str) -> Option<i64> {
        self.expiries.get(key).map(|expiry_ms| {
            let now_ms = current_time_ms();
            if now_ms >= *expiry_ms {
                0
            } else {
                (*expiry_ms - now_ms) as i64
            }
        })
    }

    /// Remove expiry
    pub fn remove(&self, key: &str) -> bool {
        self.expiries.remove(key).is_some()
    }

    /// Clean up expired keys (call periodically)
    pub fn cleanup_expired<F>(&self, mut callback: F)
    where
        F: FnMut(&str),
    {
        let now_ms = current_time_ms();
        let mut expired_keys = Vec::new();

        for entry in self.expiries.iter() {
            if *entry.value() <= now_ms {
                expired_keys.push(entry.key().clone());
            }
        }

        for key in expired_keys {
            self.expiries.remove(&key);
            callback(&key);
        }
    }

    /// Get all expiring keys with their expiry times (for persistence)
    pub fn get_all_expiries(&self) -> Vec<(String, u64)> {
        self.expiries.iter().map(|e| (e.key().clone(), *e.value())).collect()
    }

    /// Get count of keys with expiry
    pub fn len(&self) -> usize {
        self.expiries.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.expiries.is_empty()
    }
}

impl Default for ExpiryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current time in milliseconds since epoch
pub fn current_time_ms() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        NOW_MS.load(std::sync::atomic::Ordering::Relaxed)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

/// Current time in milliseconds since epoch, supplied by the JS host.
///
/// `wasm32-unknown-unknown` has no system clock (`SystemTime::now()` always
/// returns the epoch), so the host must drive time. The WASM expiry cleaner
/// updates this on every tick.
#[cfg(target_arch = "wasm32")]
static NOW_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set the current time (milliseconds since epoch) — WASM only.
#[cfg(target_arch = "wasm32")]
pub fn set_now_ms(now_ms: u64) {
    NOW_MS.store(now_ms, std::sync::atomic::Ordering::Relaxed);
}

/// Background expiry cleaner
pub struct ExpiryCleaner {
    manager: Arc<ExpiryManager>,
    db: Arc<crate::db::RedisWasmDb>,
    running: Arc<RwLock<bool>>,
}

impl ExpiryCleaner {
    /// Create a new expiry cleaner
    pub fn new(manager: Arc<ExpiryManager>, db: Arc<crate::db::RedisWasmDb>) -> Self {
        Self {
            manager,
            db,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the background cleaner (native only)
    #[cfg(feature = "native")]
    pub fn start(&self) {
        *self.running.write() = true;
        let manager = self.manager.clone();
        let db = self.db.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            while *running.read() {
                interval.tick().await;
                manager.cleanup_expired(|key| {
                    db.remove_key(key);
                });
            }
        });
    }

    /// Start the background cleaner (WASM stub)
    #[cfg(not(feature = "native"))]
    pub fn start(&self) {
        // No-op in WASM
    }

    /// Stop the background cleaner
    pub fn stop(&self) {
        *self.running.write() = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_expiry_basic() {
        let manager = ExpiryManager::new();

        manager.set_expiry("key1", 1); // 1 second
        assert!(!manager.is_expired("key1"));
        // TTL truncates to whole seconds, so it may read 0 or 1 depending on
        // the millisecond boundary between the two calls.
        let ttl = manager.ttl_seconds("key1").unwrap();
        assert!(ttl == 0 || ttl == 1);

        thread::sleep(Duration::from_millis(1100));
        assert!(manager.is_expired("key1"));
        assert_eq!(manager.ttl_seconds("key1"), Some(0));
    }

    #[test]
    fn test_expiry_ms() {
        let manager = ExpiryManager::new();

        manager.set_expiry_ms("key1", 500); // 500ms
        assert!(!manager.is_expired("key1"));

        thread::sleep(Duration::from_millis(600));
        assert!(manager.is_expired("key1"));
    }

    #[test]
    fn test_persist() {
        let manager = ExpiryManager::new();

        manager.set_expiry("key1", 60);
        assert!(manager.remove("key1"));
        assert!(!manager.is_expired("key1"));
        assert_eq!(manager.ttl_seconds("key1"), None);
    }

    #[test]
    fn test_cleanup() {
        let manager = ExpiryManager::new();

        manager.set_expiry_ms("key1", 100);
        manager.set_expiry_ms("key2", 100);
        manager.set_expiry("key3", 60); // Long expiry

        thread::sleep(Duration::from_millis(200));

        let mut cleaned = Vec::new();
        manager.cleanup_expired(|k| cleaned.push(k.to_string()));

        assert_eq!(cleaned.len(), 2);
        assert!(cleaned.contains(&"key1".to_string()));
        assert!(cleaned.contains(&"key2".to_string()));
        assert!(!manager.is_expired("key3"));
    }
}