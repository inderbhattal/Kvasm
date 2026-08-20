//! redis-wasm-core: Core Redis-like data structures and operations
//!
//! This crate provides the pure Rust implementation of Redis-compatible
//! data structures without any WASM-specific dependencies.

pub mod db;
pub mod expiry;
pub mod pubsub;
pub mod types;
pub mod wal;

pub use db::{DbError, RedisWasmDb};
pub use expiry::ExpiryManager;
pub use pubsub::{Channel, PubSubManager};
pub use types::{Value, ValueType};
pub use wal::{WalEntry, WalReplayer, WalWriter};
