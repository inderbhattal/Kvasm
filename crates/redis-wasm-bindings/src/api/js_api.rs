//! Idiomatic JS/Map-like API

use crate::serialization::js_value::*;
use crate::ToJsValue;
use redis_wasm_core::{RedisWasmDb, DbError};
use wasm_bindgen::prelude::*;

/// Idiomatic JS database API
#[wasm_bindgen]
pub struct RedisDb {
    inner: RedisWasmDb,
}

#[wasm_bindgen]
impl RedisDb {
    #[wasm_bindgen(constructor)]
    pub fn new() -> RedisDb {
        RedisDb {
            inner: RedisWasmDb::new(),
        }
    }

    // Map-like interface
    pub fn get(&self, key: &str) -> Result<Option<String>, JsValue> {
        Ok(self.inner.get(key).to_js_value()?)
    }

    pub async fn set(&self, key: &str, value: &str, ttl_ms: Option<u32>) -> Result<(), JsValue> {
        self.inner.set(key, value.to_string()).await.to_js_value()?;
        if let Some(ms) = ttl_ms {
            self.inner.pexpire(key, ms as u64).await.to_js_value()?;
        }
        Ok(())
    }

    pub fn has(&self, key: &str) -> Result<bool, JsValue> {
        Ok(self.inner.exists(&[key]).to_js_value()? > 0)
    }

    pub async fn delete(&self, key: &str) -> Result<bool, JsValue> {
        Ok(self.inner.del_async(&[key]).await.to_js_value()? > 0)
    }

    // Typed collections
    pub fn list(&self, key: &str) -> RedisList {
        RedisList { db: self.inner.clone(), key: key.to_string() }
    }

    #[wasm_bindgen(js_name = "setCollection")]
    pub fn set_collection(&self, key: &str) -> RedisSet {
        RedisSet { db: self.inner.clone(), key: key.to_string() }
    }

    pub fn sorted_set(&self, key: &str) -> RedisSortedSet {
        RedisSortedSet { db: self.inner.clone(), key: key.to_string() }
    }

    pub fn hash(&self, key: &str) -> RedisHash {
        RedisHash { db: self.inner.clone(), key: key.to_string() }
    }

    pub fn channel(&self, name: &str) -> RedisChannel {
        RedisChannel { db: self.inner.clone(), name: name.to_string() }
    }
}

/// Typed List wrapper
#[wasm_bindgen]
pub struct RedisList {
    db: RedisWasmDb,
    key: String,
}

#[wasm_bindgen]
impl RedisList {
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

    #[wasm_bindgen(js_name = "range")]
    pub fn range(&self, start: i32, end: i32) -> Result<Vec<String>, JsValue> {
        Ok(self.db.lrange(&self.key, start as isize, end as isize).to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn length(&self) -> Result<usize, JsValue> {
        Ok(self.db.llen(&self.key).to_js_value()?)
    }
}

/// Typed Set wrapper
#[wasm_bindgen]
pub struct RedisSet {
    db: RedisWasmDb,
    key: String,
}

#[wasm_bindgen]
impl RedisSet {
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
}

/// Typed Sorted Set wrapper
#[wasm_bindgen]
pub struct RedisSortedSet {
    db: RedisWasmDb,
    key: String,
}

#[wasm_bindgen]
impl RedisSortedSet {
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

    #[wasm_bindgen(js_name = "range")]
    pub fn range(&self, start: i32, stop: i32, with_scores: Option<bool>) -> Result<Vec<String>, JsValue> {
        Ok(self.db.zrange(&self.key, start as isize, stop as isize, with_scores.unwrap_or(false)).to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> Result<usize, JsValue> {
        Ok(self.db.zcard(&self.key).to_js_value()?)
    }
}

/// Typed Hash wrapper
#[wasm_bindgen]
pub struct RedisHash {
    db: RedisWasmDb,
    key: String,
}

#[wasm_bindgen]
impl RedisHash {
    #[wasm_bindgen(js_name = "set")]
    pub async fn set(&self, field: &str, value: &str) -> Result<usize, JsValue> {
        Ok(self.db.hset(&self.key, field.to_string(), value.to_string()).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "get")]
    pub fn get(&self, field: &str) -> Result<Option<String>, JsValue> {
        Ok(self.db.hget(&self.key, field).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "delete")]
    pub async fn delete(&self, fields: Vec<String>) -> Result<usize, JsValue> {
        Ok(self.db.hdel(&self.key, &fields).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "has")]
    pub fn has(&self, field: &str) -> Result<bool, JsValue> {
        Ok(self.db.hexists(&self.key, field).to_js_value()?)
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> Result<usize, JsValue> {
        Ok(self.db.hlen(&self.key).to_js_value()?)
    }
}

/// Channel wrapper for Pub/Sub
#[wasm_bindgen]
pub struct RedisChannel {
    db: RedisWasmDb,
    name: String,
}

#[wasm_bindgen]
impl RedisChannel {
    #[wasm_bindgen(js_name = "publish")]
    pub fn publish(&self, message: &str) -> Result<usize, JsValue> {
        Ok(self.db.pubsub().publish(&self.name, message.to_string()))
    }
}