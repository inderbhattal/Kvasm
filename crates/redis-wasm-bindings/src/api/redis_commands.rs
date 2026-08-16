//! Redis-compatible command API

use crate::serialization::js_value::*;
use crate::ToJsValue;
use redis_wasm_core::{RedisWasmDb, DbError};
use wasm_bindgen::prelude::*;

/// Redis-compatible client API
#[wasm_bindgen]
pub struct RedisClient {
    inner: RedisWasmDb,
}

#[wasm_bindgen]
impl RedisClient {
    #[wasm_bindgen(constructor)]
    pub fn new() -> RedisClient {
        RedisClient {
            inner: RedisWasmDb::new(),
        }
    }

    // ========================================================================
    // String commands
    // ========================================================================

    #[wasm_bindgen(js_name = "set")]
    pub async fn set(&self, key: &str, value: &str, ex: Option<u32>) -> Result<(), JsValue> {
        self.inner.set(key, value.to_string()).await.to_js_value()?;
        if let Some(seconds) = ex {
            self.inner.expire(key, seconds as u64).await.to_js_value()?;
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = "get")]
    pub fn get(&self, key: &str) -> Result<Option<String>, JsValue> {
        Ok(self.inner.get(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "strlen")]
    pub fn strlen(&self, key: &str) -> Result<usize, JsValue> {
        Ok(self.inner.strlen(key).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "append")]
    pub async fn append(&self, key: &str, value: &str) -> Result<usize, JsValue> {
        Ok(self.inner.append(key, value).await.to_js_value()?)
    }

    // ========================================================================
    // List commands
    // ========================================================================

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

    // ========================================================================
    // Key commands
    // ========================================================================

    #[wasm_bindgen(js_name = "del")]
    pub async fn del(&self, keys: Vec<String>) -> Result<usize, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.inner.del_async(&key_refs).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "exists")]
    pub fn exists(&self, keys: Vec<String>) -> Result<usize, JsValue> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        Ok(self.inner.exists(&key_refs).to_js_value()?)
    }

    #[wasm_bindgen(js_name = "type")]
    pub fn type_(&self, key: &str) -> Result<String, JsValue> {
        Ok(self.inner.type_(key).to_js_value()?.as_str().to_string())
    }

    #[wasm_bindgen(js_name = "keys")]
    pub fn keys(&self, pattern: &str) -> Result<Vec<String>, JsValue> {
        Ok(self.inner.keys(pattern).to_js_value()?)
    }

    // ========================================================================
    // Expiry commands
    // ========================================================================

    #[wasm_bindgen(js_name = "expire")]
    pub async fn expire(&self, key: &str, seconds: u32) -> Result<bool, JsValue> {
        Ok(self.inner.expire(key, seconds as u64).await.to_js_value()?)
    }

    #[wasm_bindgen(js_name = "ttl")]
    pub fn ttl(&self, key: &str) -> Result<i64, JsValue> {
        Ok(self.inner.ttl(key).to_js_value()?)
    }
}