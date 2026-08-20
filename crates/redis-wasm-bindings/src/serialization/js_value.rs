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
    let json = value
        .as_string()
        .ok_or_else(|| JsValue::from_str("Expected string"))?;
    match serde_json::from_str(&json) {
        Ok(entry) => Ok(entry),
        Err(err) => {
            upgrade_legacy_set_entry(&json).ok_or_else(|| JsValue::from_str(&err.to_string()))
        }
    }
}

/// Logs written before string values became binary-safe stored `Set.value`
/// as a JSON string instead of a byte array. Rewrite it and retry.
fn upgrade_legacy_set_entry(json: &str) -> Option<WalEntry> {
    let mut parsed: serde_json::Value = serde_json::from_str(json).ok()?;
    let value = parsed.get_mut("Set")?.get_mut("value")?;
    let bytes: Vec<serde_json::Value> = value.as_str()?.bytes().map(Into::into).collect();
    *value = serde_json::Value::Array(bytes);
    serde_json::from_value(parsed).ok()
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    fn upgrades_legacy_string_set_entries() {
        let legacy = r#"{"Set":{"key":"k","value":"héllo","expiry":null}}"#;
        let entry = js_value_to_wal_entry(&JsValue::from_str(legacy)).unwrap();
        match entry {
            WalEntry::Set { key, value, expiry } => {
                assert_eq!(key, "k");
                assert_eq!(value, "héllo".as_bytes());
                assert_eq!(expiry, None);
            }
            other => panic!("wrong entry: {:?}", other),
        }
    }

    #[wasm_bindgen_test]
    fn round_trips_binary_set_entries() {
        let entry = WalEntry::Set {
            key: "k".to_string(),
            value: vec![0, 159, 255],
            expiry: Some(5),
        };
        let js = wal_entry_to_js_value(&entry).unwrap();
        match js_value_to_wal_entry(&js).unwrap() {
            WalEntry::Set { value, expiry, .. } => {
                assert_eq!(value, vec![0, 159, 255]);
                assert_eq!(expiry, Some(5));
            }
            other => panic!("wrong entry: {:?}", other),
        }
    }
}
