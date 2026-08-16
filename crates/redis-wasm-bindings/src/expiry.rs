//! WASM-specific expiry cleaning using JavaScript setInterval

use crate::WasmDb;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::window;

/// Start the expiry cleaner for a database using JavaScript setInterval.
/// Call once after creating the database (or use
/// `WasmDb.startExpiryCleaner()`).
#[wasm_bindgen]
pub fn start_expiry_cleaner(db: &WasmDb) -> Result<(), JsValue> {
    // Clones share storage with the original database.
    let inner = db.inner().clone();
    let manager = inner.expiry().clone();

    let window = window().ok_or_else(|| JsValue::from_str("No window object"))?;

    let closure = Closure::<dyn FnMut()>::new(move || {
        // Supply the current time from JS (wasm32 has no system clock).
        redis_wasm_core::expiry::set_now_ms(js_sys::Date::now() as u64);
        let mut cleaned = Vec::new();
        manager.cleanup_expired(|key| cleaned.push(key.to_string()));
        for key in cleaned {
            inner.remove_key(&key);
        }
    });

    // Clean every 100ms, like the native cleaner
    window
        .set_interval_with_callback_and_timeout_and_arguments_0(
            closure.as_ref().unchecked_ref(),
            100,
        )
        .map_err(|e| JsValue::from_str(&format!("Failed to set interval: {:?}", e)))?;

    // Keep the closure alive for the lifetime of the page (the interval
    // runs until the page unloads; there is no stop handle).
    std::mem::forget(closure);

    Ok(())
}
