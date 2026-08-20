//! WAL Replayer for restoring database state from WAL

use crate::db::RedisWasmDb;
use crate::wal::log::{WalEntry, WalError};
use std::sync::Arc;

/// WAL Replayer - replays WAL entries to restore database state
pub struct WalReplayer {
    db: Arc<RedisWasmDb>,
}

impl WalReplayer {
    /// Create a new replayer for the given database
    pub fn new(db: Arc<RedisWasmDb>) -> Self {
        Self { db }
    }

    /// Replay WAL entries from a reader
    pub async fn replay<R: WalReader>(&self, reader: &mut R) -> Result<usize, WalError> {
        let mut count = 0;
        while let Some(entry) = reader.next_entry().await? {
            self.apply_entry(&entry).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Apply a single WAL entry to the database
    async fn apply_entry(&self, entry: &WalEntry) -> Result<(), WalError> {
        match entry {
            WalEntry::Set { key, value, expiry } => {
                self.db
                    .data_for_replay()
                    .insert(key.clone(), crate::types::Value::new_string(value.clone()));
                // SET clears any prior TTL; re-apply only if one was recorded.
                self.db.expiry_for_replay().remove(key);
                if let Some(expiry_ms) = expiry {
                    self.db.expiry_for_replay().set_expiry_at(key, *expiry_ms);
                }
            }
            WalEntry::Del { keys } => {
                for key in keys {
                    self.db.data_for_replay().remove(key);
                    self.db.expiry_for_replay().remove(key);
                }
            }
            WalEntry::Expire { key, expiry_ms } => {
                self.db.expiry_for_replay().set_expiry_at(key, *expiry_ms);
            }
            WalEntry::LPush { key, values } => {
                let mut entry = self
                    .db
                    .data_for_replay()
                    .entry(key.clone())
                    .or_insert_with(crate::types::Value::new_list);
                entry
                    .lpush(values)
                    .map_err(|e| WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            }
            WalEntry::RPush { key, values } => {
                let mut entry = self
                    .db
                    .data_for_replay()
                    .entry(key.clone())
                    .or_insert_with(crate::types::Value::new_list);
                entry
                    .rpush(values)
                    .map_err(|e| WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            }
            WalEntry::LPop { key, count } => {
                if let Some(mut entry) = self.db.data_for_replay().get_mut(key) {
                    entry.lpop(*count).map_err(|e| {
                        WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                }
            }
            WalEntry::RPop { key, count } => {
                if let Some(mut entry) = self.db.data_for_replay().get_mut(key) {
                    entry.rpop(*count).map_err(|e| {
                        WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                }
            }
            WalEntry::LSet { key, index, value } => {
                if let Some(mut entry) = self.db.data_for_replay().get_mut(key) {
                    entry.lset(*index, value.clone()).map_err(|e| {
                        WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                }
            }
            WalEntry::LRem { key, count, value } => {
                if let Some(mut entry) = self.db.data_for_replay().get_mut(key) {
                    entry.lrem(*count, value).map_err(|e| {
                        WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                }
            }
            WalEntry::LTrim { key, start, stop } => {
                if let Some(mut entry) = self.db.data_for_replay().get_mut(key) {
                    entry.ltrim(*start, *stop).map_err(|e| {
                        WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                }
            }
            WalEntry::SAdd { key, members } => {
                let mut entry = self
                    .db
                    .data_for_replay()
                    .entry(key.clone())
                    .or_insert_with(crate::types::Value::new_set);
                entry
                    .sadd(members)
                    .map_err(|e| WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            }
            WalEntry::SRem { key, members } => {
                if let Some(mut entry) = self.db.data_for_replay().get_mut(key) {
                    entry.srem(members).map_err(|e| {
                        WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                }
            }
            WalEntry::ZAdd { key, members } => {
                let mut entry = self
                    .db
                    .data_for_replay()
                    .entry(key.clone())
                    .or_insert_with(crate::types::Value::new_sorted_set);
                entry
                    .zadd(members)
                    .map_err(|e| WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            }
            WalEntry::ZRem { key, members } => {
                if let Some(mut entry) = self.db.data_for_replay().get_mut(key) {
                    entry.zrem(members).map_err(|e| {
                        WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                }
            }
            WalEntry::HSet { key, field, value } => {
                let mut entry = self
                    .db
                    .data_for_replay()
                    .entry(key.clone())
                    .or_insert_with(crate::types::Value::new_hash);
                entry
                    .hset(field.clone(), value.clone())
                    .map_err(|e| WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            }
            WalEntry::HDel { key, fields } => {
                if let Some(mut entry) = self.db.data_for_replay().get_mut(key) {
                    entry.hdel(fields).map_err(|e| {
                        WalError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                }
            }
            WalEntry::Persist { key } => {
                self.db.expiry_for_replay().remove(key);
            }
        }
        Ok(())
    }
}

/// Trait for reading WAL entries
#[async_trait::async_trait]
pub trait WalReader: Send + Sync {
    /// Read the next WAL entry
    async fn next_entry(&mut self) -> Result<Option<WalEntry>, WalError>;
}

/// In-memory WAL reader over a pre-loaded list of entries. Used when the
/// backing store (e.g. IndexedDB in WASM) is read in one batch.
pub struct VecWalReader {
    entries: std::vec::IntoIter<WalEntry>,
}

impl VecWalReader {
    pub fn new(entries: Vec<WalEntry>) -> Self {
        Self {
            entries: entries.into_iter(),
        }
    }
}

#[async_trait::async_trait]
impl WalReader for VecWalReader {
    async fn next_entry(&mut self) -> Result<Option<WalEntry>, WalError> {
        Ok(self.entries.next())
    }
}

/// Native file-based WAL reader
#[cfg(feature = "native")]
pub mod native {
    use super::*;
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::path::Path;

    pub struct NativeWalReader {
        reader: BufReader<File>,
        buffer: Vec<u8>,
    }

    impl NativeWalReader {
        pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            Ok(Self {
                reader,
                buffer: Vec::new(),
            })
        }
    }

    #[async_trait::async_trait]
    impl WalReader for NativeWalReader {
        async fn next_entry(&mut self) -> Result<Option<WalEntry>, WalError> {
            // Read length prefix (4 bytes)
            let mut len_bytes = [0u8; 4];
            match self.reader.read_exact(&mut len_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(WalError::Io(e)),
            }

            let len = u32::from_le_bytes(len_bytes) as usize;
            self.buffer.resize(len, 0);
            self.reader.read_exact(&mut self.buffer)?;

            let entry = WalEntry::decode(&self.buffer)?;
            Ok(Some(entry))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::log::WalEntry;

    #[tokio::test]
    async fn test_replay_lpush_creates_list() {
        let db = Arc::new(RedisWasmDb::new());
        let replayer = WalReplayer::new(db.clone());
        let mut reader = VecWalReader::new(vec![WalEntry::LPush {
            key: "list".into(),
            values: vec!["a".into(), "b".into()],
        }]);
        replayer.replay(&mut reader).await.unwrap();
        assert_eq!(db.lrange("list", 0, -1).unwrap(), vec!["b", "a"]);
    }

    #[tokio::test]
    async fn test_replay_set_clears_expiry() {
        let db = Arc::new(RedisWasmDb::new());
        db.expiry_for_replay().set_expiry_at("k", 9_999_999_999_999);
        let replayer = WalReplayer::new(db.clone());
        let mut reader = VecWalReader::new(vec![WalEntry::Set {
            key: "k".into(),
            value: "v".into(),
            expiry: None,
        }]);
        replayer.replay(&mut reader).await.unwrap();
        assert_eq!(db.get("k").unwrap(), Some("v".to_string()));
        assert_eq!(db.ttl("k").unwrap(), -1);
    }

    #[tokio::test]
    async fn test_replay_expire_uses_absolute_time() {
        let db = Arc::new(RedisWasmDb::new());
        let replayer = WalReplayer::new(db.clone());
        let ts = 1_234_567_890u64;
        let mut reader = VecWalReader::new(vec![WalEntry::Expire {
            key: "k".into(),
            expiry_ms: ts,
        }]);
        replayer.replay(&mut reader).await.unwrap();
        // Must be stored as an absolute timestamp, not now + ts.
        assert_eq!(db.expiry_for_replay().get_expiry_ms("k"), Some(ts));
    }
}
