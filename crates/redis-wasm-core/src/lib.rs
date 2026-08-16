//! redis-wasm-core: Core Redis-like data structures and operations
//!
//! This crate provides the pure Rust implementation of Redis-compatible
//! data structures without any WASM-specific dependencies.

pub mod db;
pub mod types;
pub mod wal;
pub mod expiry;
pub mod pubsub;

pub use db::{RedisWasmDb, DbError};
pub use types::{Value, ValueType};
pub use expiry::ExpiryManager;
pub use wal::{WalEntry, WalWriter, WalReplayer};
pub use pubsub::{PubSubManager, Channel};