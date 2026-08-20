# Kvasm

A Redis-like in-memory database that runs in the browser via WebAssembly.

Kvasm gives web apps a familiar Redis-style toolbox — strings, lists, sets,
sorted sets, hashes, TTLs, and pub/sub — backed by durable persistence
through IndexedDB and cross-tab messaging through `BroadcastChannel`.

## Features

- **Redis-compatible commands**: `SET`/`GET`, `DEL`, `EXISTS`, `TYPE`, `KEYS`
  (glob patterns), `INCR`/`DECR`/`INCRBY`/`DECRBY`/`INCRBYFLOAT`,
  `APPEND`, `GETRANGE`/`SETRANGE`, `LPUSH`/`RPUSH`/`LPOP`/
  `RPOP`/`LRANGE`/`LREM`/`LTRIM`, `SADD`/`SREM`/`SINTER`/`SUNION`/`SDIFF`,
  `ZADD`/`ZRANGE`/`ZRANK`/`ZRANGEBYSCORE`, `HSET`/`HGETALL`/`HDEL`,
  `EXPIRE`/`TTL`/`PERSIST`, and more
- **Idiomatic JS wrappers**: `Array`-like lists, `Set`-like sets, `Map`-like
  hashes on top of the same shared storage
- **Binary-safe strings**: values are byte sequences like real Redis —
  `STRLEN` counts bytes, `GETRANGE`/`SETRANGE` use byte offsets with
  zero-padding, and `getBuffer`/`setBuffer` move raw `Uint8Array` data
- **Persistence**: every mutation is appended to a write-ahead log in
  IndexedDB; `WasmDb.withPersistence(name)` replays it so state survives
  reloads
- **WAL compaction**: the log is automatically rewritten to the minimal
  entries that rebuild current state once it crosses a threshold
  (configurable via `setCompactionThreshold`, or on demand with `compact()`)
- **Key expiry**: lazy expiry on access plus an optional background cleaner
  driven by `setInterval` — works in windows and web workers, and runs for
  as long as you hold on to the handle it returns (`stop()` ends it early)
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
// Background TTL cleanup (works in workers too). Keep the handle around —
// the cleaner stops when it's stopped or garbage-collected.
const cleaner = db.startExpiryCleaner();

// Strings + TTL
await db.set("session:42", "alice", 3600); // EX seconds (optional)
db.get("session:42");                      // "alice"
db.ttl("session:42");                      // 3600

// Counters (a key's TTL survives increments, like Redis)
await db.incr("hits");                     // 1
await db.incrby("hits", 41);               // 42
await db.incrbyfloat("ratio", 0.25);       // 0.25

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

// Binary values
await db.setBuffer("avatar", new Uint8Array([0x89, 0x50, 0x4e, 0x47]));
db.getBuffer("avatar");                    // Uint8Array

// Ensure queued WAL writes have committed to IndexedDB
await db.save();

// The WAL compacts itself automatically; force it or tune it if you like
await db.compact();
db.setCompactionThreshold(4096);           // 0 disables auto-compaction
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
has been attempted, and rejects if any failed (e.g. quota exhaustion) — a
later successful compaction rewrites the log wholesale and clears the
failure.

Compaction keeps the log bounded: once it holds more entries than the
threshold (default 1024) *and* has doubled since the last compaction (so a
large live set can't force a rewrite on every write), the database snapshots
its live state as the minimal equivalent entry list and atomically replaces
the log with it in a single IndexedDB transaction. An oversized log inherited
from a previous session compacts on the first write past the threshold.

Handles are cheap to clone and share storage — a `WasmDb`, its typed
wrappers, and the expiry cleaner all see the same data.

## Development

```sh
# Core unit tests
cargo test -p redis-wasm-core

# Including native-only paths (tokio pub/sub, file WAL, background cleaner)
cargo test -p redis-wasm-core --features native

# Type-check the WASM bindings (including the browser test suite)
cargo check -p redis-wasm-bindings --target wasm32-unknown-unknown --all-targets

# Browser integration tests (needs Chrome; --firefox also works).
# Covers persistence round-trips, compaction, byte-oriented strings,
# pub/sub, and the expiry cleaner — including a dedicated-worker suite.
wasm-pack test --headless --chrome crates/redis-wasm-bindings
```

## Current limitations

- `get()`/`getrange()` decode values as UTF-8 and substitute replacement
  characters for invalid sequences (e.g. a `getrange` that splits a
  multi-byte character); use `getBuffer()` when you need exact bytes
- Persistence assumes one writing context per database name: tabs sharing a
  name all append safely, but a compaction in one tab drops entries it has
  never replayed. For multi-tab writers, disable auto-compaction
  (`setCompactionThreshold(0)`) and compact from a single owner
- The WAL stores string values as JSON byte arrays, which is larger than the
  raw text (compaction keeps the total bounded)

## License

MIT
