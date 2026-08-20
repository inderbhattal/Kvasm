//! WAL entry definitions and serialization

use bincode;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// WAL entry types for all mutating operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    // String operations
    //
    // `value` is the raw string bytes (Redis strings are binary-safe). In
    // bincode this encodes identically to the old `String` field, so native
    // WAL files written before the change still replay.
    Set {
        key: String,
        value: Vec<u8>,
        expiry: Option<u64>,
    },
    Del {
        keys: Vec<String>,
    },
    Expire {
        key: String,
        expiry_ms: u64,
    },

    // List operations
    LPush {
        key: String,
        values: Vec<String>,
    },
    RPush {
        key: String,
        values: Vec<String>,
    },
    LPop {
        key: String,
        count: usize,
    },
    RPop {
        key: String,
        count: usize,
    },
    LSet {
        key: String,
        index: isize,
        value: String,
    },
    LRem {
        key: String,
        count: isize,
        value: String,
    },
    LTrim {
        key: String,
        start: isize,
        stop: isize,
    },

    // Set operations
    SAdd {
        key: String,
        members: Vec<String>,
    },
    SRem {
        key: String,
        members: Vec<String>,
    },

    // Sorted set operations
    ZAdd {
        key: String,
        members: Vec<(String, f64)>,
    },
    ZRem {
        key: String,
        members: Vec<String>,
    },

    // Hash operations
    HSet {
        key: String,
        field: String,
        value: String,
    },
    HDel {
        key: String,
        fields: Vec<String>,
    },

    // Expiry operations
    Persist {
        key: String,
    },
}

impl WalEntry {
    /// Serialize to bytes using bincode
    pub fn encode(&self) -> Result<Vec<u8>, WalError> {
        bincode::serialize(self).map_err(WalError::Serialization)
    }

    /// Deserialize from bytes
    pub fn decode(bytes: &[u8]) -> Result<Self, WalError> {
        bincode::deserialize(bytes).map_err(WalError::Deserialization)
    }

    /// Get the key this entry operates on (for indexing)
    pub fn key(&self) -> &str {
        match self {
            WalEntry::Set { key, .. }
            | WalEntry::Expire { key, .. }
            | WalEntry::LPush { key, .. }
            | WalEntry::RPush { key, .. }
            | WalEntry::LPop { key, .. }
            | WalEntry::RPop { key, .. }
            | WalEntry::LSet { key, .. }
            | WalEntry::LRem { key, .. }
            | WalEntry::LTrim { key, .. }
            | WalEntry::SAdd { key, .. }
            | WalEntry::SRem { key, .. }
            | WalEntry::ZAdd { key, .. }
            | WalEntry::ZRem { key, .. }
            | WalEntry::HSet { key, .. }
            | WalEntry::HDel { key, .. }
            | WalEntry::Persist { key, .. } => key,
            WalEntry::Del { keys } => keys.first().map(|s| s.as_str()).unwrap_or(""),
        }
    }

    /// Check if this entry modifies data (vs just metadata)
    pub fn is_mutating(&self) -> bool {
        !matches!(self, WalEntry::Expire { .. } | WalEntry::Persist { .. })
    }
}

/// WAL errors
#[derive(Debug, Error)]
pub enum WalError {
    #[error("Serialization error: {0}")]
    Serialization(bincode::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(bincode::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("IndexedDB error: {0}")]
    IndexedDb(String),
}
