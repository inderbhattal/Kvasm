//! Browser integration tests. Run with:
//!
//! ```sh
//! wasm-pack test --headless --chrome crates/redis-wasm-bindings
//! ```

#![cfg(target_arch = "wasm32")]

use redis_wasm_bindings::{WasmDb, WasmPubSub};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

/// Seed the module clock from real time. Tests share one wasm instance, so
/// without this a TTL test's outcome could depend on whether an earlier test
/// already advanced the clock via an expiry-cleaner tick.
fn seed_clock() {
    redis_wasm_core::expiry::set_now_ms(js_sys::Date::now() as u64);
}

/// Unique per-test database name so tests never share IndexedDB state
fn unique_db_name(prefix: &str) -> String {
    format!(
        "kvasm-test-{}-{}-{}",
        prefix,
        js_sys::Date::now(),
        (js_sys::Math::random() * 1e9) as u64
    )
}

/// Await a JS setTimeout — lets IndexedDB transactions and interval
/// callbacks run
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
async fn string_commands_are_byte_oriented() {
    let db = WasmDb::new();
    db.set("k", "héllo", None).await.unwrap();

    // "é" is 2 bytes in UTF-8
    assert_eq!(db.strlen("k").unwrap(), 6);
    assert_eq!(db.getrange("k", 1, 2).unwrap(), "é");
    assert_eq!(db.append("k", "!").await.unwrap(), 7);

    // SETRANGE zero-pads the gap like Redis
    db.set("pad", "ab", None).await.unwrap();
    assert_eq!(db.setrange("pad", 4, "cd").await.unwrap(), 6);
    assert_eq!(db.get_buffer("pad").unwrap().unwrap(), b"ab\0\0cd");
}

#[wasm_bindgen_test]
async fn buffers_round_trip_binary_data() {
    let db = WasmDb::new();
    let payload = vec![0u8, 159, 146, 150, 255];
    db.set_buffer("bin", payload.clone(), None).await.unwrap();
    assert_eq!(db.get_buffer("bin").unwrap().unwrap(), payload);
    assert_eq!(db.strlen("bin").unwrap(), payload.len());
}

#[wasm_bindgen_test]
async fn persistence_round_trips_all_types() {
    seed_clock();
    let name = unique_db_name("roundtrip");

    {
        let db = WasmDb::with_persistence(&name).await.unwrap();
        db.set("s", "value", Some(3600)).await.unwrap();
        db.rpush("l", vec!["a".into(), "b".into()]).await.unwrap();
        db.sadd("set", vec!["x".into()]).await.unwrap();
        db.zadd("z", vec![JsValue::from_str("m"), JsValue::from_f64(1.5)])
            .await
            .unwrap();
        db.hset("h", "f", "v").await.unwrap();
        db.set_buffer("bin", vec![0, 200, 255], None).await.unwrap();
        db.save().await.unwrap();
    }

    let db = WasmDb::with_persistence(&name).await.unwrap();
    assert_eq!(db.get("s").unwrap(), Some("value".to_string()));
    assert!(db.ttl("s").unwrap() > 0);
    assert_eq!(db.lrange("l", 0, -1).unwrap(), vec!["a", "b"]);
    assert!(db.sismember("set", "x").unwrap());
    assert_eq!(db.zscore("z", "m").unwrap(), Some(1.5));
    assert_eq!(db.hget("h", "f").unwrap(), Some("v".to_string()));
    assert_eq!(db.get_buffer("bin").unwrap().unwrap(), vec![0, 200, 255]);
}

#[wasm_bindgen_test]
async fn compaction_shrinks_wal_and_preserves_state() {
    let name = unique_db_name("compact");

    {
        let db = WasmDb::with_persistence(&name).await.unwrap();
        db.set_compaction_threshold(0); // manual compaction only
        for i in 0..30 {
            db.set("counter", &i.to_string(), None).await.unwrap();
        }
        db.rpush("l", vec!["a".into(), "b".into()]).await.unwrap();
        db.save().await.unwrap();

        assert_eq!(db.wal_size().await.unwrap(), 31.0);
        db.compact().await.unwrap();
        assert_eq!(db.wal_size().await.unwrap(), 2.0); // Set + RPush
    }

    let db = WasmDb::with_persistence(&name).await.unwrap();
    assert_eq!(db.get("counter").unwrap(), Some("29".to_string()));
    assert_eq!(db.lrange("l", 0, -1).unwrap(), vec!["a", "b"]);
}

#[wasm_bindgen_test]
async fn auto_compaction_triggers_at_threshold() {
    let name = unique_db_name("autocompact");

    let db = WasmDb::with_persistence(&name).await.unwrap();
    db.set_compaction_threshold(5);
    for i in 0..23 {
        db.set("k", &i.to_string(), None).await.unwrap();
    }
    db.save().await.unwrap();

    // One live key, threshold 5: the log must have collapsed repeatedly.
    assert!(db.wal_size().await.unwrap() < 23.0);

    let db2 = WasmDb::with_persistence(&name).await.unwrap();
    assert_eq!(db2.get("k").unwrap(), Some("22".to_string()));
}

#[wasm_bindgen_test]
async fn expiry_cleaner_removes_expired_keys() {
    seed_clock();
    let db = WasmDb::new();
    db.set("gone", "v", None).await.unwrap();
    db.pexpire("gone", 50).await.unwrap();
    db.set("kept", "v", None).await.unwrap();

    let cleaner = db.start_expiry_cleaner().unwrap();
    sleep_ms(400).await;

    assert_eq!(db.exists(vec!["gone".into()]).unwrap(), 0);
    assert_eq!(db.exists(vec!["kept".into()]).unwrap(), 1);
    cleaner.stop();
}

#[wasm_bindgen_test]
async fn counters_increment_and_persist() {
    seed_clock();
    let name = unique_db_name("counters");

    {
        let db = WasmDb::with_persistence(&name).await.unwrap();
        assert_eq!(db.incr("hits").await.unwrap(), 1.0);
        assert_eq!(db.incrby("hits", 9.0).await.unwrap(), 10.0);
        assert_eq!(db.decrby("hits", 3.0).await.unwrap(), 7.0);
        assert_eq!(db.decr("hits").await.unwrap(), 6.0);
        assert_eq!(db.incrbyfloat("ratio", 0.5).await.unwrap(), 0.5);

        // TTL survives increments (unlike SET)
        db.expire("hits", 3600).await.unwrap();
        db.incr("hits").await.unwrap();
        assert!(db.ttl("hits").unwrap() > 0);

        // Fractional and unsafe integer deltas are rejected
        assert!(db.incrby("hits", 1.5).await.is_err());
        assert!(db.incr("ratio").await.is_err()); // float value, integer op

        db.save().await.unwrap();
    }

    let db = WasmDb::with_persistence(&name).await.unwrap();
    assert_eq!(db.incr("hits").await.unwrap(), 8.0);
    assert!(db.ttl("hits").unwrap() > 0);
    assert_eq!(db.get("ratio").unwrap(), Some("0.5".to_string()));
}

#[wasm_bindgen_test]
async fn pubsub_delivers_to_local_subscribers() {
    let mut pubsub = WasmPubSub::new();
    let mut sub = pubsub.subscribe("chat").unwrap();

    let delivered = pubsub.publish("chat", "hello").unwrap();
    assert_eq!(delivered, 1);
    assert_eq!(sub.next().await, Some("hello".to_string()));
}
