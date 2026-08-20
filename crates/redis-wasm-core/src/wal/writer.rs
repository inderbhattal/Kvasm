//! WAL Writer trait and native implementation

use crate::wal::log::{WalEntry, WalError};
use std::sync::Arc;
use async_trait::async_trait;

/// Trait for WAL writers - allows different implementations (native, WASM/IndexedDB)
#[async_trait]
pub trait WalWriterTrait: Send + Sync {
    /// Append an entry to the WAL
    async fn append(&self, entry: &WalEntry) -> Result<(), WalError>;

    /// Flush any buffered entries
    async fn flush(&self) -> Result<(), WalError>;

    /// Get the current WAL size in bytes
    async fn size(&self) -> Result<u64, WalError>;

    /// Atomically replace the whole log with `entries` (compaction).
    /// Any buffered-but-unwritten entries are discarded — the caller's
    /// snapshot already reflects them.
    async fn rewrite(&self, entries: &[WalEntry]) -> Result<(), WalError>;
}

/// Native file-based WAL writer (for server-side use)
#[cfg(feature = "native")]
pub mod native {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{BufWriter, Write};
    use std::path::Path;
    use tokio::sync::Mutex;

    pub struct NativeWalWriter {
        writer: Arc<Mutex<BufWriter<std::fs::File>>>,
        path: std::path::PathBuf,
        buffer: Arc<Mutex<Vec<WalEntry>>>,
        buffer_size: usize,
    }

    impl NativeWalWriter {
        pub fn new<P: AsRef<Path>>(path: P, buffer_size: usize) -> Result<Self, WalError> {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path.as_ref())?;
            let writer = BufWriter::new(file);

            Ok(Self {
                writer: Arc::new(Mutex::new(writer)),
                path: path.as_ref().to_path_buf(),
                buffer: Arc::new(Mutex::new(Vec::new())),
                buffer_size,
            })
        }
    }

    #[async_trait]
    impl WalWriterTrait for NativeWalWriter {
        async fn append(&self, entry: &WalEntry) -> Result<(), WalError> {
            let mut buffer = self.buffer.lock().await;
            buffer.push(entry.clone());

            if buffer.len() >= self.buffer_size {
                let mut writer = self.writer.lock().await;
                for e in buffer.drain(..) {
                    let bytes = e.encode()?;
                    // Write length prefix + data
                    writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
                    writer.write_all(&bytes)?;
                }
                writer.flush()?;
            }
            Ok(())
        }

        async fn flush(&self) -> Result<(), WalError> {
            let mut buffer = self.buffer.lock().await;
            if !buffer.is_empty() {
                let mut writer = self.writer.lock().await;
                for e in buffer.drain(..) {
                    let bytes = e.encode()?;
                    writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
                    writer.write_all(&bytes)?;
                }
                writer.flush()?;
            }
            Ok(())
        }

        async fn size(&self) -> Result<u64, WalError> {
            let metadata = std::fs::metadata(&self.path)?;
            Ok(metadata.len())
        }

        async fn rewrite(&self, entries: &[WalEntry]) -> Result<(), WalError> {
            // Lock order matches append/flush (buffer, then writer).
            let mut buffer = self.buffer.lock().await;
            let mut writer = self.writer.lock().await;
            buffer.clear();

            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.path)?;
            let mut new_writer = BufWriter::new(file);
            for e in entries {
                let bytes = e.encode()?;
                new_writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
                new_writer.write_all(&bytes)?;
            }
            new_writer.flush()?;
            *writer = new_writer;
            Ok(())
        }
    }
}

/// Type-erased WAL writer for use in RedisWasmDb
pub type WalWriter = Arc<dyn WalWriterTrait>;

#[cfg(test)]
mod tests {
    use crate::wal::log::WalEntry;

    #[tokio::test]
    async fn test_wal_entry_encoding() {
        let entry = WalEntry::Set {
            key: "test".to_string(),
            value: b"hello".to_vec(),
            expiry: None,
        };

        let encoded = entry.encode().unwrap();
        let decoded = WalEntry::decode(&encoded).unwrap();

        match decoded {
            WalEntry::Set { key, value, expiry } => {
                assert_eq!(key, "test");
                assert_eq!(value, b"hello");
                assert_eq!(expiry, None);
            }
            _ => panic!("Wrong entry type"),
        }
    }
}