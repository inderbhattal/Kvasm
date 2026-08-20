//! redis-wasm-bindings: WebAssembly bindings for redis-wasm-core
//!
//! This crate provides the WASM interface using wasm-bindgen,
//! exposing both Redis-compatible and idiomatic JS APIs.

use wasm_bindgen::prelude::*;

// Re-export core types
pub use redis_wasm_core::{
    wal::writer::WalWriterTrait, DbError, RedisWasmDb, Value, ValueType, WalEntry,
};

mod expiry;
mod persistence;
mod pubsub;
mod serialization;

pub use expiry::{start_expiry_cleaner, ExpiryCleaner};
pub use persistence::{IndexedDbWal, WasmWalWriter};
pub use pubsub::{WasmPubSub, WasmSubscriber};

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

/// Reseed the module clock from the host. wasm32 has no system clock, so
/// without this lazy expiry would compare against a time frozen at module
/// load whenever no background cleaner is ticking.
fn sync_clock() {
    redis_wasm_core::expiry::set_now_ms(js_sys::Date::now() as u64);
}

/// Validate that a JS number is a safe integer delta for INCRBY/DECRBY —
/// fractions and values past 2^53 would silently change the increment.
fn safe_integer(delta: f64) -> Result<i64, JsValue> {
    const MAX_SAFE: f64 = 9_007_199_254_740_991.0; // Number.MAX_SAFE_INTEGER
    if delta.fract() == 0.0 && delta.abs() <= MAX_SAFE {
        Ok(delta as i64)
    } else {
        Err(JsValue::from_str(
            "ERR value is not an integer or out of range",
        ))
    }
}

impl WasmDb {
    /// Shared-state handle to the underlying database
    pub(crate) fn inner(&self) -> &RedisWasmDb {
        &self.inner
    }

    /// Clock-synced database access — every command entry point goes through
    /// here so TTL checks always see the real current time
    fn db(&self) -> &RedisWasmDb {
        sync_clock();
        &self.inner
    }
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
        let wal = IndexedDbWal::open(db_name).await?;
        let entries = wal
            .replay_entries()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let writer = WasmWalWriter::from_wal(wal);
        let db = RedisWasmDb::with_wal(std::sync::Arc::new(writer));

        if !entries.is_empty() {
            let entry_count = entries.len() as u64;
            // Replay bypasses the WAL, so entries are not re-appended.
            let replayer = redis_wasm_core::wal::WalReplayer::new(std::sync::Arc::new(db.clone()));
            let mut reader = redis_wasm_core::wal::VecWalReader::new(entries);
            replayer
                .replay(&mut reader)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            // An oversized log compacts on the first write past the
            // threshold — deliberately not here, so the caller can call
            // setCompactionThreshold(0) first to preserve full history.
            db.set_wal_len(entry_count);
        }

        Ok(WasmDb { inner: db })
    }

    // ========================================================================
    // Redis-compatible API
    // ========================================================================

    /// SET key value [EX seconds]
    #[wasm_bindgen(js_name = "set")]
    pub async fn set(&self, key: &str, value: &str, ex: Option<u32>) -> Result<(), JsValue> {
        self.db().set(key, value).await.to_js_value()?;
        if let Some(seconds) = ex {
            self.db().expire(key, seconds as u64).await.to_js_value()?;
        }
        Ok(())
    }

    /// GET key, decoded as UTF-8 (lossily if the value holds binary data —
    /// use `getBuffer` for raw bytes)
    #[wasm_bindgen(js_name = "get")]
    pub fn get(&self, key: &str) -> Result<Option<String>, JsValue> {
        Ok(self.db().get(key).to_js_value()?)
    }

    /// SET key to raw bytes [EX seconds]
    #[wasm_bindgen(js_name = "setBuffer")]
    pub async fn set_buffer(
        &self,
        key: &str,
        value: Vec<u8>,
        ex: Option<u32>,
    ) -> Result<(), JsValue> {
        self.db().set(key, value).await.to_js_value()?;
        if let Some(seconds) = ex {
            self.db().expire(key, seconds as u64).await.to_js_value()?;
        }
        Ok(())
    }

    /// GET key as a Uint8Array (binary-safe)
    #[wasm_bindgen(js_name = "getBuffer")]
    pub fn get_buffer(&self, key: &str) -> Result<Option<Vec<u8>>, JsValue> {
        Ok(self.db().get_bytes(key).to_js_value()?)
    }

    /// DEL key [key ...]
    #[wasm_bindgen(js_name = "del")]
    pub async fn del(&self, keys: Vec<String>) -> Result<usize, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.db().del_async(&key_refs).await.to_js_value()?)
    }

    /// EXISTS key [key ...]
    #[wasm_bindgen(js_name = "exists")]
    pub fn exists(&self, keys: Vec<String>) -> Result<usize, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.db().exists(&key_refs).to_js_value()?)
    }

    /// TYPE key
    #[wasm_bindgen(js_name = "type")]
    pub fn type_(&self, key: &str) -> Result<String, JsValue> {
        Ok(self.db().type_(key).to_js_value()?.as_str().to_string())
    }

    /// KEYS pattern
    #[wasm_bindgen(js_name = "keys")]
    pub fn keys(&self, pattern: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.db().keys(pattern).to_js_value()?)
    }

    // String commands — byte-oriented like Redis: STRLEN counts bytes and
    // GETRANGE/SETRANGE take byte offsets (multi-byte characters span
    // several positions).
    #[wasm_bindgen(js_name = "strlen")]
    pub fn strlen(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.db().strlen(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "append")]
    pub async fn append(&self, key: &str, value: &str) -> Result<usize, JsValue> {
        Ok(self
            .db()
            .append(key, value.as_bytes())
            .await
            .to_js_value()?)
    }

    /// GETRANGE key start end (byte offsets, inclusive). Decoded as UTF-8,
    /// lossily if the range splits a multi-byte character.
    #[wasm_bindgen(js_name = "getrange")]
    pub fn getrange(&self, key: &str, start: i32, end: i32) -> Result<String, JsValue> {
        let bytes = self
            .db()
            .getrange(key, start as isize, end as isize)
            .to_js_value()?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    #[wasm_bindgen(js_name = "setrange")]
    pub async fn setrange(&self, key: &str, offset: u32, value: &str) -> Result<usize, JsValue> {
        Ok(self
            .db()
            .setrange(key, offset as usize, value.as_bytes())
            .await
            .to_js_value()?)
    }

    // Counter commands. Results come back as JS numbers, so counters beyond
    // Number.MAX_SAFE_INTEGER (2^53 - 1) lose precision at the JS boundary
    // even though the stored value stays exact.

    /// INCR key — increment by one, creating the key at 0 if missing.
    /// Preserves the key's TTL.
    #[wasm_bindgen(js_name = "incr")]
    pub async fn incr(&self, key: &str) -> Result<f64, JsValue> {
        Ok(self.db().incr(key).await.to_js_value()? as f64)
    }

    /// DECR key — decrement by one
    #[wasm_bindgen(js_name = "decr")]
    pub async fn decr(&self, key: &str) -> Result<f64, JsValue> {
        Ok(self.db().decr(key).await.to_js_value()? as f64)
    }

    /// INCRBY key delta (delta must be a safe integer)
    #[wasm_bindgen(js_name = "incrby")]
    pub async fn incrby(&self, key: &str, delta: f64) -> Result<f64, JsValue> {
        Ok(self
            .db()
            .incr_by(key, safe_integer(delta)?)
            .await
            .to_js_value()? as f64)
    }

    /// DECRBY key delta (delta must be a safe integer)
    #[wasm_bindgen(js_name = "decrby")]
    pub async fn decrby(&self, key: &str, delta: f64) -> Result<f64, JsValue> {
        let delta = safe_integer(delta)?
            .checked_neg()
            .ok_or_else(|| JsValue::from_str("ERR value is not an integer or out of range"))?;
        Ok(self.db().incr_by(key, delta).await.to_js_value()? as f64)
    }

    /// INCRBYFLOAT key delta
    #[wasm_bindgen(js_name = "incrbyfloat")]
    pub async fn incrbyfloat(&self, key: &str, delta: f64) -> Result<f64, JsValue> {
        Ok(self.db().incr_by_float(key, delta).await.to_js_value()?)
    }

    // List commands
    #[wasm_bindgen(js_name = "lpush")]
    pub async fn lpush(&self, key: &str, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().lpush(key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "rpush")]
    pub async fn rpush(&self, key: &str, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().rpush(key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lpop")]
    pub async fn lpop(&self, key: &str, count: Option<u32>) -> Result<Vec<String>, JsValue> {
        Ok(self
            .db()
            .lpop(key, count.unwrap_or(1) as usize)
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "rpop")]
    pub async fn rpop(&self, key: &str, count: Option<u32>) -> Result<Vec<String>, JsValue> {
        Ok(self
            .db()
            .rpop(key, count.unwrap_or(1) as usize)
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lrange")]
    pub fn lrange(&self, key: &str, start: i32, stop: i32) -> Result<Vec<String>, JsValue> {
        Ok(self
            .db()
            .lrange(key, start as isize, stop as isize)
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "llen")]
    pub fn llen(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.db().llen(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lindex")]
    pub fn lindex(&self, key: &str, index: i32) -> Result<Option<String>, JsValue> {
        Ok(self.db().lindex(key, index as isize).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lset")]
    pub async fn lset(&self, key: &str, index: i32, value: &str) -> Result<(), JsValue> {
        Ok(self
            .db()
            .lset(key, index as isize, value.to_string())
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "lrem")]
    pub async fn lrem(&self, key: &str, count: i32, value: &str) -> Result<usize, JsValue> {
        Ok(self
            .db()
            .lrem(key, count as isize, value)
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "ltrim")]
    pub async fn ltrim(&self, key: &str, start: i32, stop: i32) -> Result<(), JsValue> {
        Ok(self
            .db()
            .ltrim(key, start as isize, stop as isize)
            .await
            .to_js_value()?)
    }

    // Set commands
    #[wasm_bindgen(js_name = "sadd")]
    pub async fn sadd(&self, key: &str, members: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().sadd(key, &members).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "srem")]
    pub async fn srem(&self, key: &str, members: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().srem(key, &members).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "sismember")]
    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, JsValue> {
        Ok(self.db().sismember(key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "smembers")]
    pub fn smembers(&self, key: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.db().smembers(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "scard")]
    pub fn scard(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.db().scard(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "sinter")]
    pub fn sinter(&self, keys: Vec<String>) -> Result<Vec<String>, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.db().sinter(&key_refs).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "sunion")]
    pub fn sunion(&self, keys: Vec<String>) -> Result<Vec<String>, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.db().sunion(&key_refs).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "sdiff")]
    pub fn sdiff(&self, key: &str, other_keys: Vec<String>) -> Result<Vec<String>, JsValue> {
        let key_refs: Vec<&str> = other_keys.iter().map(|s| s.as_str()).collect();
        Ok(self.db().sdiff(key, &key_refs).to_js_value()?)
    }

    // Sorted set commands
    #[wasm_bindgen(js_name = "zadd")]
    pub async fn zadd(&self, key: &str, members: Vec<JsValue>) -> Result<usize, JsValue> {
        // members is [member1, score1, member2, score2, ...]
        if members.len() % 2 != 0 {
            return Err(JsValue::from_str("zadd expects [member, score, ...] pairs"));
        }
        let mut parsed = Vec::new();
        for chunk in members.chunks(2) {
            let member = chunk[0]
                .as_string()
                .ok_or_else(|| JsValue::from_str("zadd member must be a string"))?;
            let score = chunk[1]
                .as_f64()
                .ok_or_else(|| JsValue::from_str("zadd score must be a number"))?;
            parsed.push((member, score));
        }
        Ok(self.db().zadd(key, &parsed).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrem")]
    pub async fn zrem(&self, key: &str, members: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().zrem(key, &members).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zscore")]
    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, JsValue> {
        Ok(self.db().zscore(key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrank")]
    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>, JsValue> {
        Ok(self.db().zrank(key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrevrank")]
    pub fn zrevrank(&self, key: &str, member: &str) -> Result<Option<usize>, JsValue> {
        Ok(self.db().zrevrank(key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrange")]
    pub fn zrange(
        &self,
        key: &str,
        start: i32,
        stop: i32,
        with_scores: Option<bool>,
    ) -> Result<Vec<String>, JsValue> {
        Ok(self
            .db()
            .zrange(
                key,
                start as isize,
                stop as isize,
                with_scores.unwrap_or(false),
            )
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrevrange")]
    pub fn zrevrange(
        &self,
        key: &str,
        start: i32,
        stop: i32,
        with_scores: Option<bool>,
    ) -> Result<Vec<String>, JsValue> {
        Ok(self
            .db()
            .zrevrange(
                key,
                start as isize,
                stop as isize,
                with_scores.unwrap_or(false),
            )
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zrangebyscore")]
    pub fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<JsValue>, JsValue> {
        let result = self.db().zrangebyscore(key, min, max).to_js_value()?;
        let mut js_result = Vec::new();
        for (member, score) in result {
            js_result.push(JsValue::from_str(&member));
            js_result.push(JsValue::from_f64(score));
        }
        Ok(js_result)
    }

    #[wasm_bindgen(js_name = "zcard")]
    pub fn zcard(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.db().zcard(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "zcount")]
    pub fn zcount(&self, key: &str, min: f64, max: f64) -> Result<usize, JsValue> {
        Ok(self.db().zcount(key, min, max).to_js_value()?)
    }

    // Hash commands
    #[wasm_bindgen(js_name = "hset")]
    pub async fn hset(&self, key: &str, field: &str, value: &str) -> Result<usize, JsValue> {
        Ok(self
            .db()
            .hset(key, field.to_string(), value.to_string())
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hget")]
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>, JsValue> {
        Ok(self.db().hget(key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hgetall")]
    pub fn hgetall(&self, key: &str) -> Result<JsValue, JsValue> {
        let result = self.db().hgetall(key).to_js_value()?;
        let obj = js_sys::Object::new();
        for (k, v) in result {
            js_sys::Reflect::set(&obj, &JsValue::from_str(&k), &JsValue::from_str(&v))?;
        }
        Ok(obj.into())
    }

    #[wasm_bindgen(js_name = "hdel")]
    pub async fn hdel(&self, key: &str, fields: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().hdel(key, &fields).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hexists")]
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, JsValue> {
        Ok(self.db().hexists(key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hlen")]
    pub fn hlen(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.db().hlen(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hkeys")]
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.db().hkeys(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "hvals")]
    pub fn hvals(&self, key: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.db().hvals(key).to_js_value()?)
    }

    // Expiry commands
    #[wasm_bindgen(js_name = "expire")]
    pub async fn expire(&self, key: &str, seconds: u32) -> Result<bool, JsValue> {
        Ok(self.db().expire(key, seconds as u64).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "pexpire")]
    pub async fn pexpire(&self, key: &str, milliseconds: u32) -> Result<bool, JsValue> {
        Ok(self
            .db()
            .pexpire(key, milliseconds as u64)
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "ttl")]
    pub fn ttl(&self, key: &str) -> Result<i64, JsValue> {
        Ok(self.db().ttl(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "pttl")]
    pub fn pttl(&self, key: &str) -> Result<i64, JsValue> {
        Ok(self.db().pttl(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "persist")]
    pub async fn persist(&self, key: &str) -> Result<bool, JsValue> {
        Ok(self.db().persist(key).await.to_js_value()?)
    }

    // Pub/Sub: use WasmPubSub — the in-process pubsub on RedisWasmDb only
    // functions on the native build; browsers use BroadcastChannel instead.

    /// Start the background expiry cleaner. Works in windows and web
    /// workers. The cleaner runs while the returned handle stays alive:
    /// call `stop()` to halt it and free its database reference. Keep the
    /// handle reachable for a page-lifetime cleaner (a garbage-collected
    /// handle stops the sweep; lazy expiry on access always keeps working).
    #[wasm_bindgen(js_name = "startExpiryCleaner")]
    pub fn start_expiry_cleaner(&self) -> Result<ExpiryCleaner, JsValue> {
        crate::expiry::start_expiry_cleaner(self)
    }

    // Persistence commands
    #[wasm_bindgen(js_name = "save")]
    pub async fn save(&self) -> Result<(), JsValue> {
        self.db().flush_wal().await.to_js_value()
    }

    /// Compact the WAL now: replace the log with the minimal entries that
    /// rebuild the current state. Also runs automatically once the log
    /// reaches the compaction threshold.
    #[wasm_bindgen(js_name = "compact")]
    pub async fn compact(&self) -> Result<(), JsValue> {
        self.db().compact().await.to_js_value()
    }

    /// Auto-compact once the WAL holds this many entries (default 1024).
    /// Pass 0 to disable auto-compaction; `compact()` still works.
    #[wasm_bindgen(js_name = "setCompactionThreshold")]
    pub fn set_compaction_threshold(&self, threshold: u32) {
        self.db().set_compaction_threshold(threshold as u64);
    }

    /// Number of entries currently persisted in the WAL
    #[wasm_bindgen(js_name = "walSize")]
    pub async fn wal_size(&self) -> Result<f64, JsValue> {
        Ok(self.db().wal_size().await.to_js_value()? as f64)
    }

    // ========================================================================
    // Idiomatic JS API
    // ========================================================================

    /// Get a value (Map-like)
    pub fn get_value(&self, key: &str) -> Result<Option<String>, JsValue> {
        Ok(self.db().get(key).to_js_value()?)
    }

    /// Set a value (Map-like)
    pub async fn set_value(
        &self,
        key: &str,
        value: &str,
        ttl_ms: Option<u32>,
    ) -> Result<(), JsValue> {
        self.db().set(key, value).await.to_js_value()?;
        if let Some(ms) = ttl_ms {
            self.db().pexpire(key, ms as u64).await.to_js_value()?;
        }
        Ok(())
    }

    /// Check if key exists
    pub fn has(&self, key: &str) -> Result<bool, JsValue> {
        Ok(self.db().exists(&[key]).to_js_value()? > 0)
    }

    /// Delete a key
    pub async fn delete(&self, key: &str) -> Result<bool, JsValue> {
        Ok(self.db().del_async(&[key]).await.to_js_value()? > 0)
    }

    /// Get typed list
    #[wasm_bindgen(js_name = "getList")]
    pub fn get_list(&self, key: &str) -> WasmList {
        WasmList {
            db: self.db().clone(),
            key: key.to_string(),
        }
    }

    /// Get typed set
    #[wasm_bindgen(js_name = "getSet")]
    pub fn get_set(&self, key: &str) -> WasmSet {
        WasmSet {
            db: self.db().clone(),
            key: key.to_string(),
        }
    }

    /// Get typed sorted set
    #[wasm_bindgen(js_name = "getSortedSet")]
    pub fn get_sorted_set(&self, key: &str) -> WasmSortedSet {
        WasmSortedSet {
            db: self.db().clone(),
            key: key.to_string(),
        }
    }

    /// Get typed hash
    #[wasm_bindgen(js_name = "getHash")]
    pub fn get_hash(&self, key: &str) -> WasmHash {
        WasmHash {
            db: self.db().clone(),
            key: key.to_string(),
        }
    }
}

/// Adds the clock-synced `db()` accessor used by every wrapper entry point
macro_rules! clock_synced_db {
    ($wrapper:ident) => {
        impl $wrapper {
            fn db(&self) -> &RedisWasmDb {
                sync_clock();
                &self.db
            }
        }
    };
}

/// Typed List wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmList {
    db: RedisWasmDb,
    key: String,
}

clock_synced_db!(WasmList);

#[wasm_bindgen]
impl WasmList {
    #[wasm_bindgen(js_name = "push")]
    pub async fn push(&self, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().rpush(&self.key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "unshift")]
    pub async fn unshift(&self, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().lpush(&self.key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "pop")]
    pub async fn pop(&self) -> Result<Option<String>, JsValue> {
        let result = self.db().rpop(&self.key, 1).await.to_js_value()?;
        Ok(result.into_iter().next())
    }

    #[wasm_bindgen(js_name = "shift")]
    pub async fn shift(&self) -> Result<Option<String>, JsValue> {
        let result = self.db().lpop(&self.key, 1).await.to_js_value()?;
        Ok(result.into_iter().next())
    }

    #[wasm_bindgen(js_name = "get")]
    pub fn get(&self, index: i32) -> Result<Option<String>, JsValue> {
        Ok(self.db().lindex(&self.key, index as isize).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "set")]
    pub async fn set(&self, index: i32, value: &str) -> Result<(), JsValue> {
        Ok(self
            .db()
            .lset(&self.key, index as isize, value.to_string())
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "range")]
    pub fn range(&self, start: i32, end: i32) -> Result<Vec<String>, JsValue> {
        Ok(self
            .db()
            .lrange(&self.key, start as isize, end as isize)
            .to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn length(&self) -> Result<usize, JsValue> {
        Ok(self.db().llen(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "remove")]
    pub async fn remove(&self, value: &str, count: Option<i32>) -> Result<usize, JsValue> {
        Ok(self
            .db()
            .lrem(&self.key, count.unwrap_or(0) as isize, value)
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "trim")]
    pub async fn trim(&self, start: i32, end: i32) -> Result<(), JsValue> {
        Ok(self
            .db()
            .ltrim(&self.key, start as isize, end as isize)
            .await
            .to_js_value()?)
    }
}

/// Typed Set wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmSet {
    db: RedisWasmDb,
    key: String,
}

clock_synced_db!(WasmSet);

#[wasm_bindgen]
impl WasmSet {
    #[wasm_bindgen(js_name = "add")]
    pub async fn add(&self, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().sadd(&self.key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "delete")]
    pub async fn delete(&self, values: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().srem(&self.key, &values).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "has")]
    pub fn has(&self, value: &str) -> Result<bool, JsValue> {
        Ok(self.db().sismember(&self.key, value).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "values")]
    pub fn values(&self) -> Result<Vec<String>, JsValue> {
        Ok(self.db().smembers(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> Result<usize, JsValue> {
        Ok(self.db().scard(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "intersection")]
    pub fn intersection(&self, other: &WasmSet) -> Result<Vec<String>, JsValue> {
        Ok(self.db().sinter(&[&self.key, &other.key]).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "union")]
    pub fn union(&self, other: &WasmSet) -> Result<Vec<String>, JsValue> {
        Ok(self.db().sunion(&[&self.key, &other.key]).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "difference")]
    pub fn difference(&self, other: &WasmSet) -> Result<Vec<String>, JsValue> {
        Ok(self.db().sdiff(&self.key, &[&other.key]).to_js_value()?)
    }
}

/// Typed Sorted Set wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmSortedSet {
    db: RedisWasmDb,
    key: String,
}

clock_synced_db!(WasmSortedSet);

#[wasm_bindgen]
impl WasmSortedSet {
    #[wasm_bindgen(js_name = "add")]
    pub async fn add(&self, member: &str, score: f64) -> Result<usize, JsValue> {
        Ok(self
            .db()
            .zadd(&self.key, &[(member.to_string(), score)])
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "remove")]
    pub async fn remove(&self, members: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().zrem(&self.key, &members).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "score")]
    pub fn score(&self, member: &str) -> Result<Option<f64>, JsValue> {
        Ok(self.db().zscore(&self.key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "rank")]
    pub fn rank(&self, member: &str) -> Result<Option<usize>, JsValue> {
        Ok(self.db().zrank(&self.key, member).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "range")]
    pub fn range(
        &self,
        start: i32,
        stop: i32,
        with_scores: Option<bool>,
    ) -> Result<Vec<String>, JsValue> {
        Ok(self
            .db()
            .zrange(
                &self.key,
                start as isize,
                stop as isize,
                with_scores.unwrap_or(false),
            )
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "revRange")]
    pub fn rev_range(
        &self,
        start: i32,
        stop: i32,
        with_scores: Option<bool>,
    ) -> Result<Vec<String>, JsValue> {
        Ok(self
            .db()
            .zrevrange(
                &self.key,
                start as isize,
                stop as isize,
                with_scores.unwrap_or(false),
            )
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "rangeByScore")]
    pub fn range_by_score(&self, min: f64, max: f64) -> Result<Vec<JsValue>, JsValue> {
        let result = self.db().zrangebyscore(&self.key, min, max).to_js_value()?;
        let mut js_result = Vec::new();
        for (member, score) in result {
            js_result.push(JsValue::from_str(&member));
            js_result.push(JsValue::from_f64(score));
        }
        Ok(js_result)
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> Result<usize, JsValue> {
        Ok(self.db().zcard(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "count")]
    pub fn count(&self, min: f64, max: f64) -> Result<usize, JsValue> {
        Ok(self.db().zcount(&self.key, min, max).to_js_value()?)
    }
}

/// Typed Hash wrapper for idiomatic JS API
#[wasm_bindgen]
pub struct WasmHash {
    db: RedisWasmDb,
    key: String,
}

clock_synced_db!(WasmHash);

#[wasm_bindgen]
impl WasmHash {
    #[wasm_bindgen(js_name = "set")]
    pub async fn set(&self, field: &str, value: &str) -> Result<usize, JsValue> {
        Ok(self
            .db()
            .hset(&self.key, field.to_string(), value.to_string())
            .await
            .to_js_value()?)
    }

    #[wasm_bindgen(js_name = "get")]
    pub fn get(&self, field: &str) -> Result<Option<String>, JsValue> {
        Ok(self.db().hget(&self.key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "getAll")]
    pub fn get_all(&self) -> Result<JsValue, JsValue> {
        let result = self.db().hgetall(&self.key).to_js_value()?;
        let obj = js_sys::Object::new();
        for (k, v) in result {
            js_sys::Reflect::set(&obj, &JsValue::from_str(&k), &JsValue::from_str(&v))?;
        }
        Ok(obj.into())
    }

    #[wasm_bindgen(js_name = "delete")]
    pub async fn delete(&self, fields: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db().hdel(&self.key, &fields).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "has")]
    pub fn has(&self, field: &str) -> Result<bool, JsValue> {
        Ok(self.db().hexists(&self.key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "keys")]
    pub fn keys(&self) -> Result<Vec<String>, JsValue> {
        Ok(self.db().hkeys(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "values")]
    pub fn values(&self) -> Result<Vec<String>, JsValue> {
        Ok(self.db().hvals(&self.key).to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> Result<usize, JsValue> {
        Ok(self.db().hlen(&self.key).to_js_value()?)
    }
}
