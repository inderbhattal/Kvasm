# Kvasm

A Redis-like in-memory database that runs in the browser via WebAssembly.

Kvasm gives web apps a familiar Redis-style toolbox — strings, lists, sets,
sorted sets, hashes, TTLs, and pub/sub — backed by durable persistence
through IndexedDB and cross-tab messaging through `BroadcastChannel`.

## Features

- **Redis-compatible commands**: `SET`/`GET`, `DEL`, `EXISTS`, `TYPE`, `KEYS`
  (glob patterns), `APPEND`, `GETRANGE`/`SETRANGE`, `LPUSH`/`RPUSH`/`LPOP`/
  `RPOP`/`LRANGE`/`LREM`/`LTRIM`, `SADD`/`SREM`/`SINTER`/`SUNION`/`SDIFF`,
  `ZADD`/`ZRANGE`/`ZRANK`/`ZRANGEBYSCORE`, `HSET`/`HGETALL`/`HDEL`,
  `EXPIRE`/`TTL`/`PERSIST`, and more
- **Idiomatic JS wrappers**: `Array`-like lists, `Set`-like sets, `Map`-like
  hashes on top of the same shared storage
- **Persistence**: every mutation is appended to a write-ahead log in
  IndexedDB; `WasmDb.withPersistence(name)` replays it so state survives
  reloads
- **Key expiry**: lazy expiry on access plus an optional background cleaner
  driven by `setInterval`
- **Pub/Sub**: local subscribers in the same page and cross-tab delivery via
  `BroadcastChannel`
- **Pure-Rust core**: `redis-wasm-core` has no WASM dependencies and also
  runs natively (with a file-based WAL) behind the `native` feature

## Quick start

Build the WASM package:

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-pack
wasm-pack build crates/redis-wasm-bindings --target web --out-dir ../../pkg
```

Then use it from JavaScript:

```js
import init, { WasmDb, WasmPubSub } from "./pkg/redis_wasm_bindings.js";

await init();

// In-memory database (pass a name to withPersistence for durable state)
const db = await WasmDb.withPersistence("my-app");
db.startExpiryCleaner(); // background TTL cleanup

// Strings + TTL
await db.set("session:42", "alice", 3600); // EX seconds (optional)
db.get("session:42");                      // "alice"
db.ttl("session:42");                      // 3600

// Lists
await db.rpush("queue", ["a", "b", "c"]);
db.lrange("queue", 0, -1);                 // ["a", "b", "c"]

// Sorted sets: [member, score, member, score, ...]
await db.zadd("leaderboard", ["alice", 120, "bob", 95]);
db.zrevrange("leaderboard", 0, 9, true);   // top 10 with scores

// Hashes
await db.hset("user:1", "name", "Alice");
db.hgetall("user:1");                      // { name: "Alice" }

// Typed wrappers over the same data
const queue = db.getList("queue");
await queue.push(["d"]);
queue.length;                              // 4

// Ensure queued WAL writes have committed to IndexedDB
await db.save();
```

Pub/sub is independent of the database and works across tabs:

```js
const pubsub = new WasmPubSub();

const sub = pubsub.subscribe("chat");
(async () => {
  for (let msg; (msg = await sub.next()) !== undefined; ) {
    console.log("got:", msg);
  }
})();

pubsub.publish("chat", "hello"); // delivered locally and to other tabs
```

## Architecture

```
crates/
├── redis-wasm-core       Pure-Rust engine: data types, keyspace, TTLs,
│                         WAL format + replay, pub/sub primitives
└── redis-wasm-bindings   wasm-bindgen layer: WasmDb JS API, IndexedDB
                          WAL writer, BroadcastChannel pub/sub,
                          setInterval expiry cleaner
```

Persistence is a write-ahead log: each mutating command appends one entry
(as JSON, under an auto-increment key) to an IndexedDB object store via a
background task on the JS event loop. On startup, `withPersistence` replays
the log to rebuild state. `save()` resolves only after every queued append
has committed.

Handles are cheap to clone and share storage — a `WasmDb`, its typed
wrappers, and the expiry cleaner all see the same data.

## Development

```sh
# Core unit tests
cargo test -p redis-wasm-core

# Including native-only paths (tokio pub/sub, file WAL, background cleaner)
cargo test -p redis-wasm-core --features native

# Type-check the WASM bindings
cargo check -p redis-wasm-bindings --target wasm32-unknown-unknown
```

## Current limitations

- The WAL grows without bound — compaction/snapshotting is not implemented
  yet (`IndexedDbWal.clear()` exists for manual resets)
- The expiry cleaner uses `window.setInterval`, so it doesn't start inside
  web workers (lazy expiry on access still works)
- String commands are character-oriented (`STRLEN` counts bytes,
  `GETRANGE`/`SETRANGE` operate on chars), unlike byte-oriented Redis
- No browser-side (`wasm-bindgen-test`) integration test suite yet

## License

MIT
