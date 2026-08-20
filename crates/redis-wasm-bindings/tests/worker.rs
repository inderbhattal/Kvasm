//! Web-worker integration tests — verifies the pieces that used to require
//! `window` (the expiry cleaner) work in a dedicated worker. Run with:
//!
//! ```sh
//! wasm-pack test --headless --chrome crates/redis-wasm-bindings
//! ```

#![cfg(target_arch = "wasm32")]

use redis_wasm_bindings::WasmDb;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

async fn sleep_ms(ms: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let global = js_sys::global();
        let set_timeout: js_sys::Function = js_sys::Reflect::get(&global, &"setTimeout".into())
            .unwrap()
            .dyn_into()
            .unwrap();
        set_timeout.call2(&global, &resolve, &JsValue::from(ms)).unwrap();
    });
    wasm_bindgen_futures::JsFuture::from(promise).await.unwrap();
}

#[wasm_bindgen_test]
async fn expiry_cleaner_works_without_window() {
    redis_wasm_core::expiry::set_now_ms(js_sys::Date::now() as u64);

    let db = WasmDb::new();
    db.set("gone", "v", None).await.unwrap();
    db.pexpire("gone", 50).await.unwrap();
    db.set("kept", "v", None).await.unwrap();

    // With the old window-based implementation this returned
    // "No window object" inside a worker.
    let cleaner = db.start_expiry_cleaner().unwrap();
    sleep_ms(400).await;

    assert_eq!(db.exists(vec!["gone".into()]).unwrap(), 0);
    assert_eq!(db.exists(vec!["kept".into()]).unwrap(), 1);
    cleaner.stop();
}

#[wasm_bindgen_test]
async fn persistence_works_in_worker() {
    let name = format!(
        "kvasm-worker-test-{}-{}",
        js_sys::Date::now(),
        (js_sys::Math::random() * 1e9) as u64
    );

    {
        let db = WasmDb::with_persistence(&name).await.unwrap();
        db.set("k", "from-worker", None).await.unwrap();
        db.save().await.unwrap();
    }

    let db = WasmDb::with_persistence(&name).await.unwrap();
    assert_eq!(db.get("k").unwrap(), Some("from-worker".to_string()));
}
