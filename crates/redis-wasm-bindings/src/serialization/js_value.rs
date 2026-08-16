//! Serialization utilities for JS <-> Rust value conversion

use redis_wasm_core::wal::log::WalEntry;
use wasm_bindgen::prelude::*;

/// Convert a WAL entry to a JS value for storage in IndexedDB
pub fn wal_entry_to_js_value(entry: &WalEntry) -> Result<JsValue, JsValue> {
    // Serialize to JSON for simplicity (could use bincode for efficiency)
    let json = serde_json::to_string(entry).map_err(|e| JsValue::from_str(&e.to_string()))?;
    Ok(JsValue::from_str(&json))
}

/// Convert a JS value back to a WAL entry
pub fn js_value_to_wal_entry(value: &JsValue) -> Result<WalEntry, JsValue> {
    let json = value.as_string().ok_or_else(|| JsValue::from_str("Expected string"))?;
    serde_json::from_str(&json).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Convert a Rust string to JsValue
pub fn string_to_js_value(s: &str) -> JsValue {
    JsValue::from_str(s)
}

/// Convert a JsValue to a Rust String
pub fn js_value_to_string(value: &JsValue) -> Result<String, JsValue> {
    value.as_string().ok_or_else(|| JsValue::from_str("Expected string"))
}

/// Convert a Rust number to JsValue
pub fn number_to_js_value(n: f64) -> JsValue {
    JsValue::from_f64(n)
}

/// Convert a JsValue to a Rust f64
pub fn js_value_to_number(value: &JsValue) -> Result<f64, JsValue> {
    value.as_f64().ok_or_else(|| JsValue::from_str("Expected number"))
}

/// Convert a Rust bool to JsValue
pub fn bool_to_js_value(b: bool) -> JsValue {
    JsValue::from_bool(b)
}

/// Convert a JsValue to a Rust bool
pub fn js_value_to_bool(value: &JsValue) -> Result<bool, JsValue> {
    value.as_bool().ok_or_else(|| JsValue::from_str("Expected boolean"))
}

/// Convert a Rust Vec<String> to JsValue array
pub fn vec_string_to_js_value(vec: Vec<String>) -> JsValue {
    let arr = js_sys::Array::new();
    for s in vec {
        arr.push(&JsValue::from_str(&s));
    }
    arr.into()
}

/// Convert a JsValue array to Vec<String>
pub fn js_value_to_vec_string(value: &JsValue) -> Result<Vec<String>, JsValue> {
    let arr = js_sys::Array::from(value);
    let mut result = Vec::with_capacity(arr.length() as usize);
    for i in 0..arr.length() {
        let item = arr.get(i);
        result.push(js_value_to_string(&item)?);
    }
    Ok(result)
}