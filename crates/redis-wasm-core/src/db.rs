//! Main database structure and core operations

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
}

/// Main Redis-like database
#[derive(Clone)]
pub struct RedisWasmDb {
    /// Main key-value storage
    data: DashMap<String, Value>,
    /// Expiry manager for TTL
    expiry: ExpiryManager,
    /// Optional WAL writer for persistence
    wal: Option<Arc<WalWriter>>,
    /// Pub/Sub manager
    pubsub: PubSubManager,
}

impl RedisWasmDb {
    /// Create a new in-memory database without persistence
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            expiry: ExpiryManager::new(),
            wal: None,
            pubsub: PubSubManager::new(),
        }
    }

    /// Create a new database with WAL persistence
    pub fn with_wal(wal: WalWriter) -> Self {
        Self {
            data: DashMap::new(),
            expiry: ExpiryManager::new(),
            wal: Some(Arc::new(wal)),
            pubsub: PubSubManager::new(),
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

    /// Write a WAL entry if WAL is enabled
    async fn write_wal(&self, entry: &WalEntry) -> Result<(), DbError> {
        if let Some(wal) = &self.wal {
            wal.append(entry).await.map_err(DbError::WalError)?;
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

    // ========================================================================
    // Core Key Operations
    // ========================================================================

    /// Set a key to a string value (SET)
    pub async fn set(&self, key: &str, value: String) -> Result<(), DbError> {
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

    /// Get a string value (GET)
    pub fn get(&self, key: &str) -> Result<Option<String>, DbError> {
        // Check expiry first
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(None);
        }

        Ok(self.data.get(key)
            .and_then(|v| v.as_string().cloned()))
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
                self.data.remove(*key);
                self.expiry.remove(*key);
            } else if self.data.contains_key(*key) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Get key type (TYPE)
    pub fn type_(&self, key: &str) -> Result<ValueType, DbError> {
        if self.expiry.is_expired(key) {
            self.data.remove(key);
            self.expiry.remove(key);
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
            .map_err(|_| DbError::WrongType(TypeError::WrongType))?;

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

    /// Append to string (APPEND)
    pub async fn append(&self, key: &str, value: &str) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_empty_string);
        let new_len = entry.append(value)?;

        let wal_entry = WalEntry::Set {
            key: key.to_string(),
            value: entry.as_string().unwrap().clone(),
            expiry: self.expiry.get_expiry_ms(key),
        };
        self.write_wal(&wal_entry).await?;

        Ok(new_len)
    }

    /// Get substring (GETRANGE)
    pub fn getrange(&self, key: &str, start: isize, end: isize) -> Result<String, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(String::new());
        }

        self.data.get(key)
            .ok_or(DbError::KeyNotFound)?
            .get_range(start, end)
            .map_err(DbError::WrongType)
    }

    /// Set substring (SETRANGE)
    pub async fn setrange(&self, key: &str, offset: usize, value: &str) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_empty_string);
        let new_len = entry.set_range(offset, value)?;

        let wal_entry = WalEntry::Set {
            key: key.to_string(),
            value: entry.as_string().unwrap().clone(),
            expiry: self.expiry.get_expiry_ms(key),
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

        let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_list);
        let new_len = entry.lpush(values)?;

        if let Some(wal) = &self.wal {
            let wal_entry = WalEntry::LPush {
                key: key.to_string(),
                values: values.to_vec(),
            };
            self.write_wal(&wal_entry).await?;
        }

        Ok(new_len)
    }

    /// Push to right (RPUSH)
    pub async fn rpush(&self, key: &str, values: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
        }

        let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_list);
        let new_len = entry.rpush(values)?;

        if let Some(wal) = &self.wal {
            let wal_entry = WalEntry::RPush {
                key: key.to_string(),
                values: values.to_vec(),
            };
            self.write_wal(&wal_entry).await?;
        }

        Ok(new_len)
    }

    /// Pop from left (LPOP)
    pub async fn lpop(&self, key: &str, count: usize) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
        let result = entry.lpop(count)?;

        if !result.is_empty() {
            let wal_entry = WalEntry::LPop {
                key: key.to_string(),
                count,
            };
            self.write_wal(&wal_entry).await?;
        }

        Ok(result)
    }

    /// Pop from right (RPOP)
    pub async fn rpop(&self, key: &str, count: usize) -> Result<Vec<String>, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(Vec::new());
        }

        let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
        let result = entry.rpop(count)?;

        if !result.is_empty() {
            let wal_entry = WalEntry::RPop {
                key: key.to_string(),
                count,
            };
            self.write_wal(&wal_entry).await?;
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

        if let Some(wal) = &self.wal {
            let wal_entry = WalEntry::LSet {
                key: key.to_string(),
                index,
                value,
            };
            self.write_wal(&wal_entry).await?;
        }

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
            let wal_entry = WalEntry::LRem {
                key: key.to_string(),
                count,
                value: value.to_string(),
            };
            self.write_wal(&wal_entry).await?;
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

        if let Some(wal) = &self.wal {
            let wal_entry = WalEntry::LTrim {
                key: key.to_string(),
                start,
                stop,
            };
            self.write_wal(&wal_entry).await?;
        }

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

        let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_set);
        let added = entry.sadd(members)?;

        if added > 0 {
            let wal_entry = WalEntry::SAdd {
                key: key.to_string(),
                members: members.to_vec(),
            };
            self.write_wal(&wal_entry).await?;
        }

        Ok(added)
    }

    /// Remove members (SREM)
    pub async fn srem(&self, key: &str, members: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
        let removed = entry.srem(members)?;

        if removed > 0 {
            let wal_entry = WalEntry::SRem {
                key: key.to_string(),
                members: members.to_vec(),
            };
            self.write_wal(&wal_entry).await?;
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

        let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_sorted_set);
        let added = entry.zadd(members)?;

        // Always persist: ZADD may update the score of an existing member
        // (added == 0), which is still a mutation that must be replayed.
        let wal_entry = WalEntry::ZAdd {
            key: key.to_string(),
            members: members.to_vec(),
        };
        self.write_wal(&wal_entry).await?;

        Ok(added)
    }

    /// Remove members (ZREM)
    pub async fn zrem(&self, key: &str, members: &[String]) -> Result<usize, DbError> {
        if self.expiry.is_expired(key) {
            self.del(&[key])?;
            return Ok(0);
        }

        let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
        let removed = entry.zrem(members)?;

        if removed > 0 {
            let wal_entry = WalEntry::ZRem {
                key: key.to_string(),
                members: members.to_vec(),
            };
            self.write_wal(&wal_entry).await?;
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

        let mut entry = self.data.entry(key.to_string()).or_insert_with(Value::new_hash);
        let result = entry.hset(field.clone(), value.clone())?;

        if let Some(wal) = &self.wal {
            let wal_entry = WalEntry::HSet {
                key: key.to_string(),
                field,
                value,
            };
            self.write_wal(&wal_entry).await?;
        }

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

        let mut entry = self.data.get_mut(key).ok_or(DbError::KeyNotFound)?;
        let deleted = entry.hdel(fields)?;

        if deleted > 0 {
            let wal_entry = WalEntry::HDel {
                key: key.to_string(),
                fields: fields.to_vec(),
            };
            self.write_wal(&wal_entry).await?;
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

        let expiry_ms = crate::expiry::current_time_ms() + seconds * 1000;
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

        let expiry_ms = crate::expiry::current_time_ms() + milliseconds;
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