//! Serialization utilities for JS <-> Rust value conversion

use redis_wasm_core::wal::log::WalEntry;
use wasm_bindgen::prelude::*;

/// Convert a WAL entry to a JS value (JSON string) for storage in IndexedDB
pub fn wal_entry_to_js_value(entry: &WalEntry) -> Result<JsValue, JsValue> {
    let json = serde_json::to_string(entry).map_err(|e| JsValue::from_str(&e.to_string()))?;
    Ok(JsValue::from_str(&json))
}

/// Convert a stored JS value (JSON string) back to a WAL entry
pub fn js_value_to_wal_entry(value: &JsValue) -> Result<WalEntry, JsValue> {
    let json = value.as_string().ok_or_else(|| JsValue::from_str("Expected string"))?;
    serde_json::from_str(&json).map_err(|e| JsValue::from_str(&e.to_string()))
}
