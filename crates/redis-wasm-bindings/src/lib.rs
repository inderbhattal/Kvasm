//! redis-wasm-bindings: WebAssembly bindings for redis-wasm-core
//!
//! This crate provides the WASM interface using wasm-bindgen,
//! exposing both Redis-compatible and idiomatic JS APIs.

use wasm_bindgen::prelude::*;

// Re-export core types
pub use redis_wasm_core::{RedisWasmDb, DbError, Value, ValueType, WalEntry, wal::writer::WalWriterTrait};

mod api;
mod expiry;
mod persistence;
mod pubsub;
mod serialization;

pub use api::{RedisClient, RedisDb};
pub use expiry::{start_expiry_cleaner, WasmDbWithExpiry};
pub use persistence::{IndexedDbWal, WasmWalWriter};
pub use pubsub::{WasmBroadcastChannel, WasmBroadcastChannelSubscriber, WasmPubSubManager, start_pubsub_listener};

/// Helper trait to convert DbError to JsValue
pub trait ToJsValue<T> {
    fn to_js_value(self) -> Result<T, JsValue>;
}

impl<T> ToJsValue<T> for Result<T, DbError> {
    fn to_js_value(self) -> Result<T, JsValue> {
        self.map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

/// Initialize the WASM module (call once on load)
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
    // Seed the WASM clock from JS (wasm32 has no system clock).
    redis_wasm_core::expiry::set_now_ms(js_sys::Date::now() as u64);
}

/// WASM-compatible database wrapper
#[wasm_bindgen]
pub struct WasmDb {
    inner: RedisWasmDb,
}

#[wasm_bindgen]
impl WasmDb {
    /// Create a new in-memory database
    #[wasm_bindgen(constructor)]
    pub fn new() -> WasmDb {
        WasmDb {
            inner: RedisWasmDb::new(),
        }
    }

    /// Create a new database with IndexedDB persistence. Any WAL entries from
    /// previous sessions are replayed so the database starts with its
    /// persisted state.
    #[wasm_bindgen(js_name = "withPersistence")]
    pub async fn with_persistence(db_name: &str) -> Result<WasmDb, JsValue> {
        let wal = IndexedDbWal::new(db_name).await?;
        let entries = wal
            .replay_entries()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let writer = WasmWalWriter::from_wal(wal);
        let db = RedisWasmDb::with_wal(std::sync::Arc::new(writer));

        if !entries.is_empty() {
            // Replay bypasses the WAL, so entries are not re-appended.
            let replayer = redis_wasm_core::wal::WalReplayer::new(std::sync::Arc::new(db.clone()));
            let mut reader = redis_wasm_core::wal::VecWalReader::new(entries);
            replayer
                .replay(&mut reader)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
        }

        Ok(WasmDb { inner: db })
    }

    // ========================================================================
    // Redis-compatible API
    // ========================================================================

    /// SET key value [EX seconds]
    #[wasm_bindgen(js_name = "set")]
    pub async fn set(&self, key: &str, value: &str, ex: Option<u32>) -> Result<(), JsValue> {
        self.inner.set(key, value.to_string()).await.to_js_value()?;
        if let Some(seconds) = ex {
            self.inner.expire(key, seconds as u64).await.to_js_value()?;
        }
        Ok(())
    }

    /// GET key
    #[wasm_bindgen(js_name = "get")]
    pub fn get(&self, key: &str) -> Result<Option<String>, JsValue> {
        Ok(self.inner.get(key).to_js_value()?)
    }

    /// DEL key [key ...]
    #[wasm_bindgen(js_name = "del")]
    pub async fn del(&self, keys: Vec<String>) -> Result<usize, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.inner.del_async(&key_refs).await.to_js_value()?)
    }

    /// EXISTS key [key ...]
    #[wasm_bindgen(js_name = "exists")]
    pub fn exists(&self, keys: Vec<String>) -> Result<usize, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.inner.exists(&key_refs).to_js_value()?)
    }

    /// TYPE key
    #[wasm_bindgen(js_name = "type")]
    pub fn type_(&self, key: &str) -> Result<String, JsValue> {
        Ok(self.inner.type_(key).to_js_value()?.as_str().to_string())
    }

    /// KEYS pattern
    #[wasm_bindgen(js_name = "keys")]
    pub fn keys(&self, pattern: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.keys(pattern).to_js_value()?)
    }

    // String commands
    #[wasm_bindgen(js_name = "strlen")]
    pub fn strlen(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.inner.strlen(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "append")]
    pub async fn append(&self, key: &str, value: &str) -> Result<usize, JsValue> {
        Ok(self.inner.append(key, value).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "getrange")]
    pub fn getrange(&self, key: &str, start: i32, end: i32) -> Result<String, JsValue> {
        Ok(self.inner.getrange(key, start as isize, end as isize).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "setrange")]
    pub async fn setrange(&self, key: &str, offset: u32, value: &str) -> Result<usize, JsValue> {
        Ok(self.inner.setrange(key, offset as usize, value).await.to_js_value()?)
    }

    // List commands
    #[wasm_bindgen(js_name = "lpush")]
    pub async fn lpush(&self, key: &str, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.inner.lpush(key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "rpush")]
    pub async fn rpush(&self, key: &str, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.inner.rpush(key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lpop")]
    pub async fn lpop(&self, key: &str, count: Option<u32>) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.lpop(key, count.unwrap_or(1) as usize).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "rpop")]
    pub async fn rpop(&self, key: &str, count: Option<u32>) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.rpop(key, count.unwrap_or(1) as usize).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lrange")]
    pub fn lrange(&self, key: &str, start: i32, stop: i32) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.lrange(key, start as isize, stop as isize).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "llen")]
    pub fn llen(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.inner.llen(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lindex")]
    pub fn lindex(&self, key: &str, index: i32) -> Result<Option<String>, JsValue> {
        Ok(self.inner.lindex(key, index as isize).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lset")]
    pub async fn lset(&self, key: &str, index: i32, value: &str) -> Result<(), JsValue> {
        Ok(self.inner.lset(key, index as isize, value.to_string()).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lrem")]
    pub async fn lrem(&self, key: &str, count: i32, value: &str) -> Result<usize, JsValue> {
        Ok(self.inner.lrem(key, count as isize, value).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "ltrim")]
    pub async fn ltrim(&self, key: &str, start: i32, stop: i32) -> Result<(), JsValue> {
        Ok(self.inner.ltrim(key, start as isize, stop as isize).await.to_js_value()?)
    }

    // Set commands
    #[wasm_bindgen(js_name = "sadd")]
    pub async fn sadd(&self, key: &str, members: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.inner.sadd(key, &members).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "srem")]
    pub async fn srem(&self, key: &str, members: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.inner.srem(key, &members).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "sismember")]
    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, JsValue> {
        Ok(self.inner.sismember(key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "smembers")]
    pub fn smembers(&self, key: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.smembers(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "scard")]
    pub fn scard(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.inner.scard(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "sinter")]
    pub fn sinter(&self, keys: Vec<String>) -> Result<Vec<String>, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.inner.sinter(&key_refs).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "sunion")]
    pub fn sunion(&self, keys: Vec<String>) -> Result<Vec<String>, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.inner.sunion(&key_refs).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "sdiff")]
    pub fn sdiff(&self, key: &str, other_keys: Vec<String>) -> Result<Vec<String>, JsValue> {
        let key_refs: Vec<&str> = other_keys.iter().map(|s| s.as_str()).collect();
        Ok(self.inner.sdiff(key, &key_refs).to_js_value()?)
    }

    // Sorted set commands
    #[wasm_bindgen(js_name = "zadd")]
    pub async fn zadd(&self, key: &str, members: Vec<JsValue>) -> Result<usize, JsValue> {
        // members is [member1, score1, member2, score2, ...]
        let mut parsed = Vec::new();
        for chunk in members.chunks(2) {
            if chunk.len() == 2 {
                let member = chunk[0].as_string().unwrap_or_default();
                let score = chunk[1].as_f64().unwrap_or(0.0);
                parsed.push((member, score));
            }
        }
        Ok(self.inner.zadd(key, &parsed).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrem")]
    pub async fn zrem(&self, key: &str, members: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.inner.zrem(key, &members).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zscore")]
    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, JsValue> {
        Ok(self.inner.zscore(key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrank")]
    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>, JsValue> {
        Ok(self.inner.zrank(key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrevrank")]
    pub fn zrevrank(&self, key: &str, member: &str) -> Result<Option<usize>, JsValue> {
        Ok(self.inner.zrevrank(key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrange")]
    pub fn zrange(&self, key: &str, start: i32, stop: i32, with_scores: Option<bool>) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.zrange(key, start as isize, stop as isize, with_scores.unwrap_or(false)).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrevrange")]
    pub fn zrevrange(&self, key: &str, start: i32, stop: i32, with_scores: Option<bool>) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.zrevrange(key, start as isize, stop as isize, with_scores.unwrap_or(false)).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrangebyscore")]
    pub fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<JsValue>, JsValue> {
        let result = self.inner.zrangebyscore(key, min, max).to_js_value()?;
        let mut js_result = Vec::new();
        for (member, score) in result {
            js_result.push(JsValue::from_str(&member));
            js_result.push(JsValue::from_f64(score));
        }
        Ok(js_result)
    }

    #[wasm_bindgen(js_name = "zcard")]
    pub fn zcard(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.inner.zcard(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zcount")]
    pub fn zcount(&self, key: &str, min: f64, max: f64) -> Result<usize, JsValue> {
        Ok(self.inner.zcount(key, min, max).to_js_value()?)
    }

    // Hash commands
    #[wasm_bindgen(js_name = "hset")]
    pub async fn hset(&self, key: &str, field: &str, value: &str) -> Result<usize, JsValue> {
        Ok(self.inner.hset(key, field.to_string(), value.to_string()).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hget")]
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>, JsValue> {
        Ok(self.inner.hget(key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hgetall")]
    pub fn hgetall(&self, key: &str) -> Result<JsValue, JsValue> {
        let result = self.inner.hgetall(key).to_js_value()?;
        let obj = js_sys::Object::new();
        for (k, v) in result {
            js_sys::Reflect::set(&obj, &JsValue::from_str(&k), &JsValue::from_str(&v))?;
        }
        Ok(obj.into())
    }

    #[wasm_bindgen(js_name = "hdel")]
    pub async fn hdel(&self, key: &str, fields: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.inner.hdel(key, &fields).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hexists")]
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, JsValue> {
        Ok(self.inner.hexists(key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hlen")]
    pub fn hlen(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.inner.hlen(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hkeys")]
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.hkeys(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hvals")]
    pub fn hvals(&self, key: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.hvals(key).to_js_value()?)
    }

    // Expiry commands
    #[wasm_bindgen(js_name = "expire")]
    pub async fn expire(&self, key: &str, seconds: u32) -> Result<bool, JsValue> {
        Ok(self.inner.expire(key, seconds as u64).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "pexpire")]
    pub async fn pexpire(&self, key: &str, milliseconds: u32) -> Result<bool, JsValue> {
        Ok(self.inner.pexpire(key, milliseconds as u64).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "ttl")]
    pub fn ttl(&self, key: &str) -> Result<i64, JsValue> {
        Ok(self.inner.ttl(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "pttl")]
    pub fn pttl(&self, key: &str) -> Result<i64, JsValue> {
        Ok(self.inner.pttl(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "persist")]
    pub async fn persist(&self, key: &str) -> Result<bool, JsValue> {
        Ok(self.inner.persist(key).await.to_js_value()?)
    }

    // Pub/Sub commands
    #[wasm_bindgen(js_name = "publish")]
    pub fn publish(&self, channel: &str, message: &str) -> Result<usize, JsValue> {
        Ok(self.inner.pubsub().publish(channel, message.to_string()))
    }

    // Note: subscribe returns a stream - needs special handling in JS
    // We'll provide a different API for that

    // Persistence commands
    #[wasm_bindgen(js_name = "save")]
    pub async fn save(&self) -> Result<(), JsValue> {
        self.inner.flush_wal().await.to_js_value()
    }

    // ========================================================================
    // Idiomatic JS API
    // ========================================================================

    /// Get a value (Map-like)
    pub fn get_value(&self, key: &str) -> Result<Option<String>, JsValue> {
        Ok(self.inner.get(key).to_js_value()?)
    }

    /// Set a value (Map-like)
    pub async fn set_value(&self, key: &str, value: &str, ttl_ms: Option<u32>) -> Result<(), JsValue> {
        self.inner.set(key, value.to_string()).await.to_js_value()?;
        if let Some(ms) = ttl_ms {
            self.inner.pexpire(key, ms as u64).await.to_js_value()?;
        }
        Ok(())
    }

    /// Check if key exists
    pub fn has(&self, key: &str) -> Result<bool, JsValue> {
        Ok(self.inner.exists(&[key]).to_js_value()? > 0)
    }

    /// Delete a key
    pub async fn delete(&self, key: &str) -> Result<bool, JsValue> {
        Ok(self.inner.del_async(&[key]).await.to_js_value()? > 0)
    }

    /// Get typed list
    #[wasm_bindgen(js_name = "getList")]
    pub fn get_list(&self, key: &str) -> WasmList {
        WasmList { db: self.inner.clone(), key: key.to_string() }
    }

    /// Get typed set
    #[wasm_bindgen(js_name = "getSet")]
    pub fn get_set(&self, key: &str) -> WasmSet {
        WasmSet { db: self.inner.clone(), key: key.to_string() }
    }

    /// Get typed sorted set
    #[wasm_bindgen(js_name = "getSortedSet")]
    pub fn get_sorted_set(&self, key: &str) -> WasmSortedSet {
        WasmSortedSet { db: self.inner.clone(), key: key.to_string() }
    }

    /// Get typed hash
    #[wasm_bindgen(js_name = "getHash")]
    pub fn get_hash(&self, key: &str) -> WasmHash {
        WasmHash { db: self.inner.clone(), key: key.to_string() }
    }

    /// Get channel
    #[wasm_bindgen(js_name = "getChannel")]
    pub fn get_channel(&self, name: &str) -> WasmChannel {
        WasmChannel { db: self.inner.clone(), name: name.to_string() }
    }
}

/// Typed List wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmList {
    db: RedisWasmDb,
    key: String,
}

#[wasm_bindgen]
impl WasmList {
    #[wasm_bindgen(js_name = "push")]
    pub async fn push(&self, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db.rpush(&self.key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "unshift")]
    pub async fn unshift(&self, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db.lpush(&self.key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "pop")]
    pub async fn pop(&self) -> Result<Option<String>, JsValue> {
        let result = self.db.rpop(&self.key, 1).await.to_js_value()?;
        Ok(result.into_iter().next())
    }

    #[wasm_bindgen(js_name = "shift")]
    pub async fn shift(&self) -> Result<Option<String>, JsValue> {
        let result = self.db.lpop(&self.key, 1).await.to_js_value()?;
        Ok(result.into_iter().next())
    }

    #[wasm_bindgen(js_name = "get")]
    pub fn get(&self, index: i32) -> Result<Option<String>, JsValue> {
        Ok(self.db.lindex(&self.key, index as isize).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "set")]
    pub async fn set(&self, index: i32, value: &str) -> Result<(), JsValue> {
        Ok(self.db.lset(&self.key, index as isize, value.to_string()).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "range")]
    pub fn range(&self, start: i32, end: i32) -> Result<Vec<String>, JsValue> {
        Ok(self.db.lrange(&self.key, start as isize, end as isize).to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn length(&self) -> Result<usize, JsValue> {
        Ok(self.db.llen(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "remove")]
    pub async fn remove(&self, value: &str, count: Option<i32>) -> Result<usize, JsValue> {
        Ok(self.db.lrem(&self.key, count.unwrap_or(0) as isize, value).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "trim")]
    pub async fn trim(&self, start: i32, end: i32) -> Result<(), JsValue> {
        Ok(self.db.ltrim(&self.key, start as isize, end as isize).await.to_js_value()?)
    }
}

/// Typed Set wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmSet {
    db: RedisWasmDb,
    key: String,
}

#[wasm_bindgen]
impl WasmSet {
    #[wasm_bindgen(js_name = "add")]
    pub async fn add(&self, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db.sadd(&self.key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "delete")]
    pub async fn delete(&self, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db.srem(&self.key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "has")]
    pub fn has(&self, value: &str) -> Result<bool, JsValue> {
        Ok(self.db.sismember(&self.key, value).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "values")]
    pub fn values(&self) -> Result<Vec<String>, JsValue> {
        Ok(self.db.smembers(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> Result<usize, JsValue> {
        Ok(self.db.scard(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "intersection")]
    pub fn intersection(&self, other: &WasmSet) -> Result<Vec<String>, JsValue> {
        let other_members = other.db.smembers(&other.key).to_js_value()?;
        Ok(self.db.sinter(&[&self.key, &other.key]).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "union")]
    pub fn union(&self, other: &WasmSet) -> Result<Vec<String>, JsValue> {
        Ok(self.db.sunion(&[&self.key, &other.key]).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "difference")]
    pub fn difference(&self, other: &WasmSet) -> Result<Vec<String>, JsValue> {
        Ok(self.db.sdiff(&self.key, &[&other.key]).to_js_value()?)
    }
}

/// Typed Sorted Set wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmSortedSet {
    db: RedisWasmDb,
    key: String,
}

#[wasm_bindgen]
impl WasmSortedSet {
    #[wasm_bindgen(js_name = "add")]
    pub async fn add(&self, member: &str, score: f64) -> Result<usize, JsValue> {
        Ok(self.db.zadd(&self.key, &[(member.to_string(), score)]).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "remove")]
    pub async fn remove(&self, members: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db.zrem(&self.key, &members).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "score")]
    pub fn score(&self, member: &str) -> Result<Option<f64>, JsValue> {
        Ok(self.db.zscore(&self.key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "rank")]
    pub fn rank(&self, member: &str) -> Result<Option<usize>, JsValue> {
        Ok(self.db.zrank(&self.key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "range")]
    pub fn range(&self, start: i32, stop: i32, with_scores: Option<bool>) -> Result<Vec<String>, JsValue> {
        Ok(self.db.zrange(&self.key, start as isize, stop as isize, with_scores.unwrap_or(false)).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "revRange")]
    pub fn rev_range(&self, start: i32, stop: i32, with_scores: Option<bool>) -> Result<Vec<String>, JsValue> {
        Ok(self.db.zrevrange(&self.key, start as isize, stop as isize, with_scores.unwrap_or(false)).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "rangeByScore")]
    pub fn range_by_score(&self, min: f64, max: f64) -> Result<Vec<JsValue>, JsValue> {
        let result = self.db.zrangebyscore(&self.key, min, max).to_js_value()?;
        let mut js_result = Vec::new();
        for (member, score) in result {
            js_result.push(JsValue::from_str(&member));
            js_result.push(JsValue::from_f64(score));
        }
        Ok(js_result)
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> Result<usize, JsValue> {
        Ok(self.db.zcard(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "count")]
    pub fn count(&self, min: f64, max: f64) -> Result<usize, JsValue> {
        Ok(self.db.zcount(&self.key, min, max).to_js_value()?)
    }
}

/// Typed Hash wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmHash {
    db: RedisWasmDb,
    key: String,
}

#[wasm_bindgen]
impl WasmHash {
    #[wasm_bindgen(js_name = "set")]
    pub async fn set(&self, field: &str, value: &str) -> Result<usize, JsValue> {
        Ok(self.db.hset(&self.key, field.to_string(), value.to_string()).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "get")]
    pub fn get(&self, field: &str) -> Result<Option<String>, JsValue> {
        Ok(self.db.hget(&self.key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "getAll")]
    pub fn get_all(&self) -> Result<JsValue, JsValue> {
        let result = self.db.hgetall(&self.key).to_js_value()?;
        let obj = js_sys::Object::new();
        for (k, v) in result {
            js_sys::Reflect::set(&obj, &JsValue::from_str(&k), &JsValue::from_str(&v))?;
        }
        Ok(obj.into())
    }

    #[wasm_bindgen(js_name = "delete")]
    pub async fn delete(&self, fields: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db.hdel(&self.key, &fields).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "has")]
    pub fn has(&self, field: &str) -> Result<bool, JsValue> {
        Ok(self.db.hexists(&self.key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "keys")]
    pub fn keys(&self) -> Result<Vec<String>, JsValue> {
        Ok(self.db.hkeys(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "values")]
    pub fn values(&self) -> Result<Vec<String>, JsValue> {
        Ok(self.db.hvals(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> Result<usize, JsValue> {
        Ok(self.db.hlen(&self.key).to_js_value()?)
    }
}

/// Channel wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmChannel {
    db: RedisWasmDb,
    name: String,
}

#[wasm_bindgen]
impl WasmChannel {
    #[wasm_bindgen(js_name = "publish")]
    pub fn publish(&self, message: &str) -> Result<usize, JsValue> {
        Ok(self.db.pubsub().publish(&self.name, message.to_string()))
    }

    // Note: subscribe would return an async iterator - complex in WASM
    // We'll handle this via a different mechanism
}