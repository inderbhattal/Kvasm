//! WASM-specific expiry cleaning using JavaScript setInterval

use crate::serialization::js_value::*;
use crate::{RedisWasmDb, DbError, ToJsValue, WasmDb};
use redis_wasm_core::expiry::ExpiryManager;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::window;

/// Start the expiry cleaner for a database using JavaScript setInterval
/// This should be called once after creating the database in WASM
#[wasm_bindgen]
pub fn start_expiry_cleaner(db: &WasmDb) -> Result<(), JsValue> {
    let inner = db.inner.clone();
    let manager = inner.expiry().clone();

    // Get the window object
    let window = window().ok_or_else(|| JsValue::from_str("No window object"))?;

    // Create a closure that will be called by setInterval
    let closure = Closure::<dyn FnMut()>::new(move || {
        // Supply the current time from JS (wasm32 has no system clock).
        redis_wasm_core::expiry::set_now_ms(js_sys::Date::now() as u64);
        let mut cleaned = Vec::new();
        manager.cleanup_expired(|key| cleaned.push(key.to_string()));
        for key in cleaned {
            inner.remove_key(&key);
        }
    });

    // Set up interval (100ms like native)
    let interval_id = window
        .set_interval_with_callback_and_timeout_and_arguments_0(
            closure.as_ref().unchecked_ref(),
            100,
        )
        .map_err(|e| JsValue::from_str(&format!("Failed to set interval: {:?}", e)))?;

    // Store the closure to prevent it from being dropped (the interval keeps
    // running for the lifetime of the page). The interval id is a plain i32
    // handle and needs no special treatment.
    std::mem::forget(closure);
    let _ = interval_id;

    Ok(())
}

/// WASM-compatible database wrapper with expiry cleaner support
#[wasm_bindgen]
pub struct WasmDbWithExpiry {
    inner: RedisWasmDb,
    _cleaner_started: bool,
}

#[wasm_bindgen]
impl WasmDbWithExpiry {
    /// Create a new in-memory database with expiry cleaning
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<WasmDbWithExpiry, JsValue> {
        let db = RedisWasmDb::new();
        let mut result = WasmDbWithExpiry {
            inner: db,
            _cleaner_started: false,
        };
        // Start the expiry cleaner automatically
        start_expiry_cleaner(&WasmDb { inner: result.inner.clone() })?;
        result._cleaner_started = true;
        Ok(result)
    }

    /// Create a new database with IndexedDB persistence and expiry cleaning
    #[wasm_bindgen(js_name = "withPersistence")]
    pub async fn with_persistence(db_name: &str) -> Result<WasmDbWithExpiry, JsValue> {
        let wal = crate::persistence::WasmWalWriter::new(db_name)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let db = RedisWasmDb::with_wal(std::sync::Arc::new(wal));
        let mut result = WasmDbWithExpiry {
            inner: db,
            _cleaner_started: false,
        };
        // Start the expiry cleaner automatically
        start_expiry_cleaner(&WasmDb { inner: result.inner.clone() })?;
        result._cleaner_started = true;
        Ok(result)
    }

    // ========================================================================
    // Redis-compatible API (delegate to inner)
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
}