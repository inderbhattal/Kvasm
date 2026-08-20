//! WASM-specific expiry cleaning using JavaScript setInterval.
//!
//! Uses the global scope's `setInterval` (via `js_sys::global()`), so it
//! works in windows, dedicated/shared workers, and service workers alike —
//! anywhere the timer API exists.

use crate::WasmDb;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

/// Handle for a running expiry cleaner.
///
/// The cleaner runs for as long as this handle is alive: call
/// [`stop`](ExpiryCleaner::stop) (or let the handle be garbage-collected) to
/// halt the background sweep and release the database reference the interval
/// callback holds. Lazy expiry on access keeps working either way. Keep the
/// handle reachable if the cleaner should run for the whole page lifetime.
#[wasm_bindgen]
pub struct ExpiryCleaner {
    interval_id: JsValue,
    /// Keeps the interval callback (and its database handle) alive while the
    /// cleaner runs; released by `Drop` after the interval is cleared.
    _closure: Closure<dyn FnMut()>,
}

#[wasm_bindgen]
impl ExpiryCleaner {
    /// Stop the cleaner now: clears the interval and frees the callback and
    /// its database reference.
    pub fn stop(self) {
        // Dropping does the work — see the Drop impl.
    }
}

impl Drop for ExpiryCleaner {
    fn drop(&mut self) {
        // Clear the interval BEFORE the closure field drops, so the browser
        // can never call into a freed closure.
        let global = js_sys::global();
        if let Ok(clear_interval) = global_fn(&global, "clearInterval") {
            let _ = clear_interval.call1(&global, &self.interval_id);
        }
    }
}

/// Look up a function on the global scope (works without `window`)
fn global_fn(global: &JsValue, name: &str) -> Result<js_sys::Function, JsValue> {
    js_sys::Reflect::get(global, &JsValue::from_str(name))?
        .dyn_into()
        .map_err(|_| JsValue::from_str(&format!("{name} is not available in this environment")))
}

/// Start the expiry cleaner for a database (or use
/// `WasmDb.startExpiryCleaner()`). The cleaner runs until the returned
/// handle is stopped, freed, or garbage-collected.
#[wasm_bindgen]
pub fn start_expiry_cleaner(db: &WasmDb) -> Result<ExpiryCleaner, JsValue> {
    // Clones share storage with the original database.
    let inner = db.inner().clone();
    let manager = inner.expiry().clone();

    let closure = Closure::<dyn FnMut()>::new(move || {
        // Supply the current time from JS (wasm32 has no system clock).
        redis_wasm_core::expiry::set_now_ms(js_sys::Date::now() as u64);
        // Data removal re-checks the TTL under the map guard, so a write
        // that races the sweep wins.
        manager.cleanup_expired(|key| inner.remove_key_if_expired(key));
    });

    let global = js_sys::global();
    // Clean every 100ms, like the native cleaner
    let interval_id = global_fn(&global, "setInterval")?.call2(
        &global,
        closure.as_ref().unchecked_ref(),
        &JsValue::from_f64(100.0),
    )?;

    Ok(ExpiryCleaner {
        interval_id,
        _closure: closure,
    })
}
