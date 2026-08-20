//! Main database structure and core operations

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use dashmap::DashMap;
use crate::types::{Value, ValueType, TypeError};
use crate::expiry::ExpiryManager;
use crate::wal::{WalWriter, WalEntry};
use crate::pubsub::PubSubManager;
use thiserror::Error;

/// Database errors
#[derive(Debug, Error)]
pub enum DbError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("Wrong type: {0}")]
    WrongType(#[from] TypeError),
    #[error("WAL error: {0}")]
    WalError(#[from] crate::wal::log::WalError),
    #[error("Expiry error: {0}")]
    ExpiryError(String),
    #[error("PubSub error: {0}")]
    PubSubError(String),
    #[error("Invalid pattern: {0}")]
    InvalidPattern(String),
}

/// Compact the WAL once it holds this many entries (see
/// [`RedisWasmDb::set_compaction_threshold`]).
pub const DEFAULT_COMPACTION_THRESHOLD: u64 = 1024;

/// Main Redis-like database.
///
/// Cloning is cheap and clones share the same underlying storage — a clone
/// sees and performs the same reads/writes as the original.
#[derive(Clone)]
pub struct RedisWasmDb {
    /// Main key-value storage (shared between clones)
    data: Arc<DashMap<String, Value>>,
    /// Expiry manager for TTL
    expiry: ExpiryManager,
    /// Optional WAL writer for persistence
    wal: Option<WalWriter>,
    /// Pub/Sub manager
    pubsub: PubSubManager,
    /// Number of entries currently in the WAL (replayed + appended)
    wal_len: Arc<AtomicU64>,
    /// Auto-compact once `wal_len` reaches this (0 disables auto-compaction)
    compaction_threshold: Arc<AtomicU64>,
    /// Snapshot size of the last successful compaction (growth gate base)
    last_compact_len: Arc<AtomicU64>,
    /// Guards against overlapping compaction runs
    compacting: Arc<AtomicBool>,
}

impl RedisWasmDb {
    /// Create a new in-memory database without persistence
    pub fn new() -> Self {
        Self::build(None)
    }

    /// Create a new database with WAL persistence
    pub fn with_wal(wal: WalWriter) -> Self {
        Self::build(Some(wal))
    }

    fn build(wal: Option<WalWriter>) -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            expiry: ExpiryManager::new(),
            wal,
            pubsub: PubSubManager::new(),
            wal_len: Arc::new(AtomicU64::new(0)),
            compaction_threshold: Arc::new(AtomicU64::new(DEFAULT_COMPACTION_THRESHOLD)),
            last_compact_len: Arc::new(AtomicU64::new(0)),
            compacting: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get the expiry manager
    pub fn expiry(&self) -> &ExpiryManager {
        &self.expiry
    }

    /// Get the pub/sub manager
    pub fn pubsub(&self) -> &PubSubManager {
        &self.pubsub
    }

    /// Check if WAL is enabled
    pub fn has_wal(&self) -> bool {
        self.wal.is_some()
    }

    /// Get direct access to data for WAL replay
    pub fn data_for_replay(&self) -> &DashMap<String, Value> {
        &self.data
    }

    /// Get direct access to expiry for WAL replay
    pub fn expiry_for_replay(&self) -> &ExpiryManager {
        &self.expiry
    }

    /// Remove a key (for expiry cleaner)
    pub fn remove_key(&self, key: &str) {
        self.data.remove(key);
    }

    /// Remove a key only if its TTL still reads as expired (for expiry
    /// cleaners). The re-check runs under the map guard, so a concurrent
    /// SET or EXPIRE that refreshed the key wins over a stale sweep decision.
    pub fn remove_key_if_expired(&self, key: &str) {
        self.data.remove_if(key, |_, _| self.expiry.is_expired(key));
    }

    /// Write a WAL entry if WAL is enabled, auto-compacting at the threshold
    async fn write_wal(&self, entry: &WalEntry) -> Result<(), DbError> {
        if let Some(wal) = &self.wal {
            wal.append(entry).await.map_err(DbError::WalError)?;
            self.wal_len.fetch_add(1, Ordering::Relaxed);
            // A failed background compaction must not fail the write that
            // tripped it — the append above already succeeded, and the
            // compaction retries on a later write.
            if let Err(e) = self.compact_if_needed().await {
                tracing::warn!("WAL auto-compaction failed: {e}");
            }
        }
        Ok(())
    }

    /// Flush any buffered WAL entries to durable storage
    pub async fn flush_wal(&self) -> Result<(), DbError> {
        if let Some(wal) = &self.wal {
            wal.flush().await.map_err(DbError::WalError)?;
        }
        Ok(())
    }

    /// Size of the WAL as reported by its writer (entry count for the
    /// IndexedDB writer, file bytes for the native writer); 0 without a WAL
    pub async fn wal_size(&self) -> Result<u64, DbError> {
        match &self.wal {
            Some(wal) => wal.size().await.map_err(DbError::WalError),
            None => Ok(0),
        }
    }

    /// Record how many entries the WAL currently holds. Called after startup
    /// replay so the compaction threshold accounts for pre-existing entries.
    pub fn set_wal_len(&self, len: u64) {
        self.wal_len.store(len, Ordering::Relaxed);
    }

    /// Auto-compact the WAL once it holds this many entries; 0 disables
    /// auto-compaction (manual [`compact`](Self::compact) still works)
    pub fn set_compaction_threshold(&self, threshold: u64) {
        self.compaction_threshold.store(threshold, Ordering::Relaxed);
    }

    /// Compact the WAL if auto-compaction is enabled and worthwhile: the log
    /// must have reached the threshold AND doubled since the last compaction
    /// (AOF-style growth gate), so a live set at or above the threshold
    /// cannot trigger a full rewrite on every subsequent write.
    pub async fn compact_if_needed(&self) -> Result<(), DbError> {
        let threshold = self.compaction_threshold.load(Ordering::Relaxed);
        if threshold == 0 {
            return Ok(());
        }
        let len = self.wal_len.load(Ordering::Relaxed);
        let base = self.last_compact_len.load(Ordering::Relaxed);
        if len >= threshold && len >= base.saturating_mul(2) {
            self.compact().await?;
        }
        Ok(())
    }

    /// Compact the WAL: atomically replace the log with the minimal set of
    /// entries that rebuilds the current state.
    ///
    /// No-op without a WAL or while another compaction is in flight. On the
    /// single-threaded WASM runtime the snapshot and the log rewrite cannot
    /// interleave with other commands, so this is always safe there. On
    /// native, do NOT mutate concurrently with `compact`: an entry appended
    /// after the snapshot is taken but before the rewrite lands is erased
    /// from the log even though its write was acknowledged — a crash before
    /// that key is written again loses it. Serialize writes with compaction.
    pub async fn compact(&self) -> Result<(), DbError> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        if self.compacting.swap(true, Ordering::Acquire) {
            return Ok(());
        }

        let entries = self.snapshot_entries();
        let snapshot_len = entries.len() as u64;
        let prev_len = self.wal_len.swap(snapshot_len, Ordering::Relaxed);
        let result = wal.rewrite(&entries).await.map_err(DbError::WalError);
        match &result {
            Ok(()) => {
                self.last_compact_len.store(snapshot_len, Ordering::Relaxed);
            }
            Err(_) => {
                // The old log survives a failed rewrite; put its entries back
                // into the counter (appends that raced in stay counted).
                self.wal_len
                    .fetch_add(prev_len.saturating_sub(snapshot_len), Ordering::Relaxed);
            }
        }
        self.compacting.store(false, Ordering::Release);
        result
    }

    /// The minimal WAL entries that recreate the current live state.
    /// Replaying them into an empty database yields this database, except
    /// that expired keys and empty collections are dropped — real Redis
    /// never keeps either, and an empty hash has no creating entry (emitting
    /// only its `Expire` would plant a TTL on a nonexistent key that the
    /// next key by that name would silently inherit).
    pub fn snapshot_entries(&self) -> Vec<WalEntry> {
        // Emitted after the entry that creates `key`, since replaying the
        // creation clears no TTL (only WalEntry::Set does, and its TTL
        // travels inline).
        fn push_expire(entries: &mut Vec<WalEntry>, key: &str, expiry: Option<u64>) {
            if let Some(expiry_ms) = expiry {
                entries.push(WalEntry::Expire {
                    key: key.to_string(),
                    expiry_ms,
                });
            }
        }

        let mut entries = Vec::new();
        for item in self.data.iter() {
            let key = item.key();
            if self.expiry.is_expired(key) {
                continue;
            }
            // Empty collections don't survive a snapshot (empty strings do —
            // they are legitimate Redis values).
            if item.value().value_type() != ValueType::String && item.value().is_empty() {
                continue;
            }
            let expiry = self.expiry.get_expiry_ms(key);
            match item.value() {
                Value::String(bytes) => entries.push(WalEntry::Set {
                    key: key.clone(),
                    value: bytes.clone(),
                    expiry,
                }),
                Value::List(list) => {
                    entries.push(WalEntry::RPush {
                        key: key.clone(),
                        values: list.iter().cloned().collect(),
                    });
                    push_expire(&mut entries, key, expiry);
                }
                Value::Set(set) => {
                    entries.push(WalEntry::SAdd {
                        key: key.clone(),
                        members: set.iter().cloned().collect(),
                    });
                    push_expire(&mut entries, key, expiry);
                }
                Value::SortedSet(zset) => {
                    entries.push(WalEntry::ZAdd {
                        key: key.clone(),
                        members: zset.iter().map(|(m, s)| (m.to_string(), s)).collect(),
                    });
                    push_expire(&mut entries, key, expiry);
                }
                Value::Hash(hash) => {
                    for (field, value) in hash {
                        entries.push(WalEntry::HSet {
                            key: key.clone(),
                            field: field.clone(),
                            value: value.clone(),
                        });
                    }
                    push_expire(&mut entries, key, expiry);
                }
            }
        }
        entries
    }

    // ========================================================================
    // Core Key Operations
    // ========================================================================

    /// Set a key to a string value (SET). Values are binary-safe bytes;
    /// `&str`, `String`, and `Vec<u8>` all convert.
    pub async fn set(&self, key: &str, value: impl Into<Vec<u8>>) -> Result<(), DbError> {
        let value = value.into();
        // SET clears any existing TTL (Redis semantics)
        self.expiry.remove(key);
        self.data.insert(key.to_string(), Value::new_string(value.clone()));
        let entry = WalEntry::Set {
            key: key.to_string(),
            value,
            expiry: None,
        };
        self.write_wal(&entry).await?;
        Ok(())
    }

    /// Get a string value decoded as UTF-8 (GET).
    ///
    /// Invalid UTF-8 (possible after byte-oriented SETRANGE or a binary
    /// `set`) is decoded lossily; use [`get_bytes`](Self::get_bytes) for the
    /// raw bytes.
    pub fn get(&self, key: &str) -> Result<Option<String>, DbError> {
        Ok(self
            .get_bytes(key)?
            .map(|b| String::from_utf8_lossy(&b).into_owned()))
    }

    /// Get the raw string bytes (GET)
    pub fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, DbError> {
        // Check expiry first
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(None);
        }

        match self.data.get(key) {
            None => Ok(None),
            Some(v) => v.as_bytes()
                .cloned()
                .map(Some)
                .ok_or(DbError::WrongType(TypeError::WrongType)),
        }
    }

    /// Delete keys (DEL) — synchronous, no WAL.
    ///
    /// Used internally for lazy expiry cleanup, where the key is already
    /// expired and its `Expire` entry was already persisted.
    pub fn del(&self, keys: &[&str]) -> Result<usize, DbError> {
        let mut count = 0;
        for key in keys {
            if self.data.remove(*key).is_some() {
                self.expiry.remove(*key);
                count += 1;
            }
        }
        Ok(count)
    }

    /// Delete keys and persist via WAL (DEL command)
    pub async fn del_async(&self, keys: &[&str]) -> Result<usize, DbError> {
        let count = self.del(keys)?;
        if count > 0 {
            let keys_owned: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
            let entry = WalEntry::Del { keys: keys_owned };
            self.write_wal(&entry).await?;
        }
        Ok(count)
    }

    /// Check if keys exist (EXISTS)
    pub fn exists(&self, keys: &[&str]) -> Result<usize, DbError> {
        let mut count = 0;
        for key in keys {
            if self.expiry.is_expired(key) {
                self.del(&[key])?;
            } else if self.data.contains_key(*key) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Get key type (TYPE)
    pub fn type_(&self, key: &str) -> Result<ValueType, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(ValueType::None);
        }

        Ok(self.data.get(key)
            .map(|v| v.value_type())
            .unwrap_or(ValueType::None))
    }

    /// Get all keys matching pattern (KEYS)
    pub fn keys(&self, pattern: &str) -> Result<Vec<String>, DbError> {
        let regex_pattern = glob_to_regex(pattern);
        let regex = regex::Regex::new(&format!("^{}$", regex_pattern))
            .map_err(|e| DbError::InvalidPattern(e.to_string()))?;

        let mut result = Vec::new();
        for entry in self.data.iter() {
            let key = entry.key();
            if !self.expiry.is_expired(key) && regex.is_match(key) {
                result.push(key.clone());
            }
        }
        Ok(result)
    }

    // ========================================================================
    // String Operations
    // ========================================================================

    /// Get string length (STRLEN)
    pub fn strlen(&self, key: &str) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        match self.data.get(key) {
            Some(v) => v.str_len().map_err(DbError::WrongType),
            None => Ok(0),
        }
    }

    /// Append bytes to string (APPEND), returning the new byte length
    pub async fn append(&self, key: &str, value: &[u8]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        // Scoped so the map guard drops before the WAL await below.
        let (new_len, wal_entry) = {
            let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_empty_string);
            let new_len = entry.append(value)?;
            let wal_entry = WalEntry::Set {
                key: key.to_string(),
                value: entry.as_bytes().unwrap().clone(),
                expiry: self.expiry.get_expiry_ms(key),
            };
            (new_len, wal_entry)
        };
        self.write_wal(&wal_entry).await?;

        Ok(new_len)
    }

    /// Get byte range, start/end inclusive (GETRANGE). A missing key reads
    /// as the empty string, like Redis.
    pub fn getrange(&self, key: &str, start: isize, end: isize) -> Result<Vec<u8>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        match self.data.get(key) {
            Some(v) => v.get_range(start, end).map_err(DbError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    /// Overwrite bytes at a byte offset, zero-padding any gap (SETRANGE)
    pub async fn setrange(&self, key: &str, offset: usize, value: &[u8]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        // Redis: an empty value mutates nothing and never creates the key —
        // just report the current length.
        if value.is_empty() {
            return self.strlen(key);
        }

        let (new_len, wal_entry) = {
            let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_empty_string);
            let new_len = entry.set_range(offset, value)?;
            let wal_entry = WalEntry::Set {
                key: key.to_string(),
                value: entry.as_bytes().unwrap().clone(),
                expiry: self.expiry.get_expiry_ms(key),
            };
            (new_len, wal_entry)
        };
        self.write_wal(&wal_entry).await?;

        Ok(new_len)
    }

    // ========================================================================
    // List Operations
    // ========================================================================

    /// Push to left (LPUSH)
    pub async fn lpush(&self, key: &str, values: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        // No values mutates nothing and never creates the key (Redis
        // rejects the arity outright); report the current length.
        if values.is_empty() {
            return match self.llen(key) {
                Err(DbError::KeyNotFound) => Ok(0),
                other => other,
            };
        }

        let new_len = {
            let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_list);
            entry.lpush(values)?
        };
        self.write_wal(&WalEntry::LPush {
            key: key.to_string(),
            values: values.to_vec(),
        }).await?;

        Ok(new_len)
    }

    /// Push to right (RPUSH)
    pub async fn rpush(&self, key: &str, values: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        if values.is_empty() {
            return match self.llen(key) {
                Err(DbError::KeyNotFound) => Ok(0),
                other => other,
            };
        }

        let new_len = {
            let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_list);
            entry.rpush(values)?
        };
        self.write_wal(&WalEntry::RPush {
            key: key.to_string(),
            values: values.to_vec(),
        }).await?;

        Ok(new_len)
    }

    /// Pop from left (LPOP)
    pub async fn lpop(&self, key: &str, count: usize) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        let result = {
            let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
            entry.lpop(count)?
        };
        if !result.is_empty() {
            self.write_wal(&WalEntry::LPop {
                key: key.to_string(),
                count,
            }).await?;
        }

        Ok(result)
    }

    /// Pop from right (RPOP)
    pub async fn rpop(&self, key: &str, count: usize) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        let result = {
            let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
            entry.rpop(count)?
        };
        if !result.is_empty() {
            self.write_wal(&WalEntry::RPop {
                key: key.to_string(),
                count,
            }).await?;
        }

        Ok(result)
    }

    /// Get range (LRANGE)
    pub fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .lrange(start, stop)
            .map_err(DbError::WrongType)
    }

    /// Get length (LLEN)
    pub fn llen(&self, key: &str) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .llen()
            .map_err(DbError::WrongType)
    }

    /// Get element at index (LINDEX)
    pub fn lindex(&self, key: &str, index: isize) -> Result<Option<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(None);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .lindex(index)
            .map_err(DbError::WrongType)
    }

    /// Set element at index (LSET)
    pub async fn lset(&self, key: &str, index: isize, value: String) -> Result<(), DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Err(DbError::KeyNotFound);
        }

        self.data.get_mut(key)
            .ok_or(DbError::KeyNotFound)?
            .lset(index, value.clone())?;

        self.write_wal(&WalEntry::LSet {
            key: key.to_string(),
            index,
            value,
        }).await?;

        Ok(())
    }

    /// Remove elements (LREM)
    pub async fn lrem(&self, key: &str, count: isize, value: &str) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        let removed = self.data.get_mut(key)
            .ok_or(DbError::KeyNotFound)?
            .lrem(count, value)?;

        if removed > 0 {
            self.write_wal(&WalEntry::LRem {
                key: key.to_string(),
                count,
                value: value.to_string(),
            }).await?;
        }

        Ok(removed)
    }

    /// Trim list (LTRIM)
    pub async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<(), DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(());
        }

        self.data.get_mut(key)
            .ok_or(DbError::KeyNotFound)?
            .ltrim(start, stop)?;

        self.write_wal(&WalEntry::LTrim {
            key: key.to_string(),
            start,
            stop,
        }).await?;

        Ok(())
    }

    // ========================================================================
    // Set Operations
    // ========================================================================

    /// Add members (SADD)
    pub async fn sadd(&self, key: &str, members: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        // No members mutates nothing and never creates the key; still
        // surface WRONGTYPE for existing non-set keys via SCARD.
        if members.is_empty() {
            return match self.scard(key) {
                Err(DbError::KeyNotFound) => Ok(0),
                Ok(_) => Ok(0),
                Err(other) => Err(other),
            };
        }

        let added = {
            let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_set);
            entry.sadd(members)?
        };
        if added > 0 {
            self.write_wal(&WalEntry::SAdd {
                key: key.to_string(),
                members: members.to_vec(),
            }).await?;
        }

        Ok(added)
    }

    /// Remove members (SREM)
    pub async fn srem(&self, key: &str, members: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        let removed = {
            let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
            entry.srem(members)?
        };
        if removed > 0 {
            self.write_wal(&WalEntry::SRem {
                key: key.to_string(),
                members: members.to_vec(),
            }).await?;
        }

        Ok(removed)
    }

    /// Check membership (SISMEMBER)
    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(false);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .sismember(member)
            .map_err(DbError::WrongType)
    }

    /// Get all members (SMEMBERS)
    pub fn smembers(&self, key: &str) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .smembers()
            .map_err(DbError::WrongType)
    }

    /// Get cardinality (SCARD)
    pub fn scard(&self, key: &str) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .scard()
            .map_err(DbError::WrongType)
    }

    /// Intersection (SINTER)
    pub fn sinter(&self, keys: &[&str]) -> Result<Vec<String>, DbError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Get first set
        let first_key = keys[0];
        if self.expiry.is_expired(first_key) {
            self.del(&[first_key])?;
            return Ok(Vec::new());
        }

        let mut result = match self.data.get(first_key) {
            Some(entry) => entry
                .as_set()
                .ok_or_else(|| DbError::WrongType(TypeError::WrongType))?
                .clone(),
            None => return Ok(Vec::new()),
        };

        // Intersect with remaining
        for key in &keys[1..] {
            if self.expiry.is_expired(key) {
                self.del(&[*key])?;
                result.clear();
                break;
            }

            match self.data.get(*key) {
                Some(entry) => {
                    let other = entry.as_set().ok_or_else(|| DbError::WrongType(TypeError::WrongType))?;
                    result.retain(|v| other.contains(v));
                }
                None => {
                    result.clear();
                    break;
                }
            }
        }

        Ok(result.into_iter().collect())
    }

    /// Union (SUNION)
    pub fn sunion(&self, keys: &[&str]) -> Result<Vec<String>, DbError> {
        let mut result = std::collections::HashSet::new();

        for key in keys {
            if self.expiry.is_expired(key) {
                self.del(&[*key])?;
                continue;
            }

            if let Some(entry) = self.data.get(*key) {
                let set = entry.as_set().ok_or_else(|| DbError::WrongType(TypeError::WrongType))?;
                result.extend(set.iter().cloned());
            }
        }

        Ok(result.into_iter().collect())
    }

    /// Difference (SDIFF)
    pub fn sdiff(&self, key: &str, other_keys: &[&str]) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        let mut result = match self.data.get(key) {
            Some(entry) => entry
                .as_set()
                .ok_or_else(|| DbError::WrongType(TypeError::WrongType))?
                .clone(),
            None => return Ok(Vec::new()),
        };

        for other_key in other_keys {
            if self.expiry.is_expired(other_key) {
                self.del(&[*other_key])?;
                continue;
            }

            if let Some(entry) = self.data.get(*other_key) {
                let other = entry.as_set().ok_or_else(|| DbError::WrongType(TypeError::WrongType))?;
                for v in other {
                    result.remove(v);
                }
            }
        }

        Ok(result.into_iter().collect())
    }

    // ========================================================================
    // Sorted Set Operations
    // ========================================================================

    /// Add members with scores (ZADD)
    pub async fn zadd(&self, key: &str, members: &[(String, f64)]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        // No members mutates nothing and never creates the key; still
        // surface WRONGTYPE for existing non-zset keys via ZCARD.
        if members.is_empty() {
            return match self.zcard(key) {
                Err(DbError::KeyNotFound) => Ok(0),
                Ok(_) => Ok(0),
                Err(other) => Err(other),
            };
        }

        let added = {
            let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_sorted_set);
            entry.zadd(members)?
        };
        // Always persist: ZADD may update the score of an existing member
        // (added == 0), which is still a mutation that must be replayed.
        self.write_wal(&WalEntry::ZAdd {
            key: key.to_string(),
            members: members.to_vec(),
        }).await?;

        Ok(added)
    }

    /// Remove members (ZREM)
    pub async fn zrem(&self, key: &str, members: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        let removed = {
            let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
            entry.zrem(members)?
        };
        if removed > 0 {
            self.write_wal(&WalEntry::ZRem {
                key: key.to_string(),
                members: members.to_vec(),
            }).await?;
        }

        Ok(removed)
    }

    /// Get score (ZSCORE)
    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(None);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .zscore(member)
            .map_err(DbError::WrongType)
    }

    /// Get rank (ZRANK)
    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(None);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .zrank(member)
            .map_err(DbError::WrongType)
    }

    /// Get reverse rank (ZREVRANK)
    pub fn zrevrank(&self, key: &str, member: &str) -> Result<Option<usize>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(None);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .zrevrank(member)
            .map_err(DbError::WrongType)
    }

    /// Get range by index (ZRANGE)
    pub fn zrange(&self, key: &str, start: isize, stop: isize, with_scores: bool) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .zrange(start, stop, with_scores)
            .map_err(DbError::WrongType)
    }

    /// Get reverse range by index (ZREVRANGE)
    pub fn zrevrange(&self, key: &str, start: isize, stop: isize, with_scores: bool) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .zrevrange(start, stop, with_scores)
            .map_err(DbError::WrongType)
    }

    /// Get range by score (ZRANGEBYSCORE)
    pub fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<(String, f64)>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .zrangebyscore(min, max)
            .map_err(DbError::WrongType)
    }

    /// Get cardinality (ZCARD)
    pub fn zcard(&self, key: &str) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .zcard()
            .map_err(DbError::WrongType)
    }

    /// Count in score range (ZCOUNT)
    pub fn zcount(&self, key: &str, min: f64, max: f64) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .zcount(min, max)
            .map_err(DbError::WrongType)
    }

    // ========================================================================
    // Hash Operations
    // ========================================================================

    /// Set field (HSET)
    pub async fn hset(&self, key: &str, field: String, value: String) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        let result = {
            let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_hash);
            entry.hset(field.clone(), value.clone())?
        };
        self.write_wal(&WalEntry::HSet {
            key: key.to_string(),
            field,
            value,
        }).await?;

        Ok(result)
    }

    /// Get field (HGET)
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(None);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .hget(field)
            .map_err(DbError::WrongType)
    }

    /// Get all fields and values (HGETALL)
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .hgetall()
            .map_err(DbError::WrongType)
    }

    /// Delete fields (HDEL)
    pub async fn hdel(&self, key: &str, fields: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        let deleted = {
            let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
            entry.hdel(fields)?
        };
        if deleted > 0 {
            self.write_wal(&WalEntry::HDel {
                key: key.to_string(),
                fields: fields.to_vec(),
            }).await?;
        }

        Ok(deleted)
    }

    /// Check field exists (HEXISTS)
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(false);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .hexists(field)
            .map_err(DbError::WrongType)
    }

    /// Get length (HLEN)
    pub fn hlen(&self, key: &str) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .hlen()
            .map_err(DbError::WrongType)
    }

    /// Get all keys (HKEYS)
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .hkeys()
            .map_err(DbError::WrongType)
    }

    /// Get all values (HVALS)
    pub fn hvals(&self, key: &str) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .hvals()
            .map_err(DbError::WrongType)
    }

    // ========================================================================
    // Expiry Operations
    // ========================================================================

    /// Set expiry in seconds (EXPIRE)
    pub async fn expire(&self, key: &str, seconds: u64) -> Result<bool, DbError> {
        if !self.data.contains_key(key) || self.expiry.is_expired(key) {
            return Ok(false);
        }

        // Saturating: absurd durations mean "never", not a wrapped past time
        let expiry_ms =
            crate::expiry::current_time_ms().saturating_add(seconds.saturating_mul(1000));
        self.expiry.set_expiry_at(key, expiry_ms);
        let wal_entry = WalEntry::Expire {
            key: key.to_string(),
            expiry_ms,
        };
        self.write_wal(&wal_entry).await?;

        Ok(true)
    }

    /// Set expiry in milliseconds (PEXPIRE)
    pub async fn pexpire(&self, key: &str, milliseconds: u64) -> Result<bool, DbError> {
        if !self.data.contains_key(key) || self.expiry.is_expired(key) {
            return Ok(false);
        }

        let expiry_ms = crate::expiry::current_time_ms().saturating_add(milliseconds);
        self.expiry.set_expiry_at(key, expiry_ms);
        let wal_entry = WalEntry::Expire {
            key: key.to_string(),
            expiry_ms,
        };
        self.write_wal(&wal_entry).await?;

        Ok(true)
    }

    /// Get TTL in seconds (TTL)
    pub fn ttl(&self, key: &str) -> Result<i64, DbError> {
        if !self.data.contains_key(key) {
            return Ok(-2); // Key doesn't exist
        }

        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(-2);
        }

        Ok(self.expiry.ttl_seconds(key).unwrap_or(-1))
    }

    /// Get TTL in milliseconds (PTTL)
    pub fn pttl(&self, key: &str) -> Result<i64, DbError> {
        if !self.data.contains_key(key) {
            return Ok(-2);
        }

        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(-2);
        }

        Ok(self.expiry.ttl_ms(key).unwrap_or(-1))
    }

    /// Remove expiry (PERSIST)
    pub async fn persist(&self, key: &str) -> Result<bool, DbError> {
        if !self.data.contains_key(key) || self.expiry.is_expired(key) {
            return Ok(false);
        }

        let had_expiry = self.expiry.remove(key);
        if had_expiry {
            let wal_entry = WalEntry::Persist {
                key: key.to_string(),
            };
            self.write_wal(&wal_entry).await?;
        }

        Ok(had_expiry)
    }
}

impl Default for RedisWasmDb {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a Redis-style glob pattern to a regex pattern.
///
/// Supports `*` (any sequence), `?` (any single char), `[...]`/`[^...]`
/// character classes, and `\x` escaping. All other regex metacharacters are
/// escaped so they match literally.
fn glob_to_regex(pattern: &str) -> String {
    fn flush(literal: &mut String, out: &mut String) {
        if !literal.is_empty() {
            out.push_str(&regex::escape(literal));
            literal.clear();
        }
    }

    let mut out = String::new();
    let mut literal = String::new();
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '*' => {
                flush(&mut literal, &mut out);
                out.push_str(".*");
            }
            '?' => {
                flush(&mut literal, &mut out);
                out.push('.');
            }
            '\\' => {
                if let Some(next) = chars.next() {
                    literal.push(next);
                }
            }
            '[' => {
                // Copy a character class verbatim (Redis and regex share syntax).
                let mut class = String::from("[");
                if chars.peek() == Some(&'^') {
                    class.push('^');
                    chars.next();
                }
                let mut closed = false;
                while let Some(cc) = chars.next() {
                    class.push(cc);
                    if cc == ']' {
                        closed = true;
                        break;
                    }
                }
                if closed {
                    flush(&mut literal, &mut out);
                    out.push_str(&class);
                } else {
                    // Unterminated class: treat '[' as a literal.
                    literal.push_str(&class);
                }
            }
            _ => literal.push(c),
        }
    }
    flush(&mut literal, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_set_clears_ttl() {
        let db = RedisWasmDb::new();
        db.set("k", "v".to_string()).await.unwrap();
        db.expire("k", 100).await.unwrap();
        assert!(db.ttl("k").unwrap() > 0);

        db.set("k", "v2".to_string()).await.unwrap();
        assert_eq!(db.ttl("k").unwrap(), -1);
        assert_eq!(db.get("k").unwrap(), Some("v2".to_string()));
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let db = RedisWasmDb::new();
        let clone = db.clone();

        clone.set("k", "v".to_string()).await.unwrap();
        assert_eq!(db.get("k").unwrap(), Some("v".to_string()));

        clone.expire("k", 100).await.unwrap();
        assert!(db.ttl("k").unwrap() > 0);

        db.del_async(&["k"]).await.unwrap();
        assert_eq!(clone.get("k").unwrap(), None);
    }

    #[tokio::test]
    async fn test_strlen_missing_key() {
        let db = RedisWasmDb::new();
        assert_eq!(db.strlen("missing").unwrap(), 0);
    }

    #[tokio::test]
    async fn test_sinter_missing_key() {
        let db = RedisWasmDb::new();
        db.sadd("a", &["1".to_string()]).await.unwrap();
        assert!(db.sinter(&["a", "missing"]).unwrap().is_empty());
        assert!(db.sinter(&["missing"]).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_sdiff_missing_key() {
        let db = RedisWasmDb::new();
        assert!(db.sdiff("missing", &[]).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_keys_glob_escapes_metachars() {
        let db = RedisWasmDb::new();
        db.set("a+b", "1".to_string()).await.unwrap();
        db.set("axb", "2".to_string()).await.unwrap();
        // '+' must be treated literally, not as a regex quantifier
        assert_eq!(db.keys("a+b").unwrap(), vec!["a+b"]);
        // '?' matches any single char (including '+')
        let mut keys = db.keys("a?b").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a+b", "axb"]);
        assert_eq!(db.keys("a*").unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_setrange_hostile_offset_errors_instead_of_panicking() {
        let db = RedisWasmDb::new();
        db.set("k", "ab".to_string()).await.unwrap();
        // Would wrap on 32-bit / attempt a huge allocation before the cap
        assert!(db.setrange("k", usize::MAX, b"x").await.is_err());
        assert!(db.setrange("k", 4_294_967_295, b"x").await.is_err());
        // Value untouched
        assert_eq!(db.get("k").unwrap(), Some("ab".to_string()));
        // Append past the cap errors too
        assert!(matches!(
            db.append("k", &[0u8; 1]).await,
            Ok(3) // small append fine
        ));
    }

    #[tokio::test]
    async fn test_getrange_missing_key_returns_empty() {
        let db = RedisWasmDb::new();
        assert_eq!(db.getrange("missing", 0, -1).unwrap(), Vec::<u8>::new());
    }

    #[tokio::test]
    async fn test_empty_write_ops_do_not_create_keys() {
        let db = RedisWasmDb::new();
        assert_eq!(db.lpush("l", &[]).await.unwrap(), 0);
        assert_eq!(db.rpush("l", &[]).await.unwrap(), 0);
        assert_eq!(db.sadd("s", &[]).await.unwrap(), 0);
        assert_eq!(db.zadd("z", &[]).await.unwrap(), 0);
        assert_eq!(db.exists(&["l", "s", "z"]).unwrap(), 0);
        // An empty SETRANGE never creates the key either
        assert_eq!(db.setrange("str", 0, b"").await.unwrap(), 0);
        assert_eq!(db.exists(&["str"]).unwrap(), 0);
    }

    #[tokio::test]
    async fn test_expire_saturates_instead_of_wrapping() {
        let db = RedisWasmDb::new();
        db.set("k", "v".to_string()).await.unwrap();
        db.expire("k", u64::MAX).await.unwrap();
        // A wrapped (tiny) timestamp would read as already expired
        assert!(db.ttl("k").unwrap() > 0);
        assert_eq!(db.get("k").unwrap(), Some("v".to_string()));
    }

    #[tokio::test]
    async fn test_snapshot_skips_empty_collections_and_their_ttls() {
        let db = RedisWasmDb::new();
        db.hset("h", "f".into(), "v".into()).await.unwrap();
        db.expire("h", 1000).await.unwrap();
        db.hdel("h", &["f".to_string()]).await.unwrap();
        db.set("", String::new()).await.unwrap(); // empty string survives

        let entries = db.snapshot_entries();
        // No HSet and, crucially, no phantom Expire for the empty hash
        assert!(entries
            .iter()
            .all(|e| !matches!(e, WalEntry::HSet { .. } | WalEntry::Expire { .. })));
        assert_eq!(entries.len(), 1); // just the empty-string Set
    }

    #[tokio::test]
    async fn test_snapshot_entries_rebuild_state() {
        let db = RedisWasmDb::new();
        db.set("s", "value").await.unwrap();
        db.expire("s", 1000).await.unwrap();
        db.rpush("l", &["a".into(), "b".into()]).await.unwrap();
        db.sadd("set", &["x".into()]).await.unwrap();
        db.zadd("z", &[("m".into(), 1.5)]).await.unwrap();
        db.hset("h", "f".into(), "v".into()).await.unwrap();
        db.expire("l", 1000).await.unwrap();

        let restored = Arc::new(RedisWasmDb::new());
        let replayer = crate::wal::WalReplayer::new(restored.clone());
        let mut reader = crate::wal::VecWalReader::new(db.snapshot_entries());
        replayer.replay(&mut reader).await.unwrap();

        assert_eq!(restored.get("s").unwrap(), Some("value".to_string()));
        assert!(restored.ttl("s").unwrap() > 0);
        assert_eq!(restored.lrange("l", 0, -1).unwrap(), vec!["a", "b"]);
        assert!(restored.ttl("l").unwrap() > 0);
        assert!(restored.sismember("set", "x").unwrap());
        assert_eq!(restored.zscore("z", "m").unwrap(), Some(1.5));
        assert_eq!(restored.hget("h", "f").unwrap(), Some("v".to_string()));
    }

    #[cfg(feature = "native")]
    mod native_compaction {
        use super::*;
        use crate::wal::replayer::native::NativeWalReader;
        use crate::wal::writer::native::NativeWalWriter;

        fn temp_wal_path(name: &str) -> std::path::PathBuf {
            std::env::temp_dir().join(format!("kvasm-test-{}-{}.wal", name, std::process::id()))
        }

        async fn replay_file(path: &std::path::Path) -> (Arc<RedisWasmDb>, usize) {
            let restored = Arc::new(RedisWasmDb::new());
            let mut reader = NativeWalReader::new(path).unwrap();
            let count = crate::wal::WalReplayer::new(restored.clone())
                .replay(&mut reader)
                .await
                .unwrap();
            (restored, count)
        }

        #[tokio::test]
        async fn test_compact_shrinks_log_and_preserves_state() {
            let path = temp_wal_path("manual");
            let _ = std::fs::remove_file(&path);
            let db = RedisWasmDb::with_wal(Arc::new(NativeWalWriter::new(&path, 1).unwrap()));
            db.set_compaction_threshold(0); // manual compaction only

            for i in 0..50 {
                db.set("counter", format!("{i}")).await.unwrap();
            }
            db.rpush("list", &["a".into(), "b".into()]).await.unwrap();
            db.expire("counter", 1000).await.unwrap();

            let before = db.wal_size().await.unwrap();
            db.compact().await.unwrap();
            assert!(db.wal_size().await.unwrap() < before);

            let (restored, count) = replay_file(&path).await;
            assert!(count <= 3); // Set + RPush + Expire
            assert_eq!(restored.get("counter").unwrap(), Some("49".to_string()));
            assert!(restored.ttl("counter").unwrap() > 0);
            assert_eq!(restored.lrange("list", 0, -1).unwrap(), vec!["a", "b"]);
            let _ = std::fs::remove_file(&path);
        }

        #[tokio::test]
        async fn test_auto_compaction_triggers_at_threshold() {
            let path = temp_wal_path("auto");
            let _ = std::fs::remove_file(&path);
            let db = RedisWasmDb::with_wal(Arc::new(NativeWalWriter::new(&path, 1).unwrap()));
            db.set_compaction_threshold(10);

            for i in 0..25 {
                db.set("k", format!("{i}")).await.unwrap();
            }

            // One live key: auto-compaction must have collapsed the log.
            let (restored, count) = replay_file(&path).await;
            assert!(count < 25);
            assert_eq!(restored.get("k").unwrap(), Some("24".to_string()));
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn test_glob_to_regex() {
        // '*' -> '.*', '?' -> '.', literal chars escaped
        assert_eq!(glob_to_regex("a*b"), "a.*b");
        assert_eq!(glob_to_regex("a.b"), "a\\.b");
        assert_eq!(glob_to_regex("a?b"), "a.b");
        assert_eq!(glob_to_regex("a[bc]d"), "a[bc]d");
        assert_eq!(glob_to_regex("a+b"), "a\\+b");
    }
}