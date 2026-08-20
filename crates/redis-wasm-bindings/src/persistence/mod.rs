//! Persistence layer for WASM (IndexedDB)

pub mod indexed_db;

pub use indexed_db::{IndexedDbWal, WasmWalWriter};
