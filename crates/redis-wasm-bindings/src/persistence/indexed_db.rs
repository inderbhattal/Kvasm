//! IndexedDB WAL implementation for WASM

use crate::serialization::js_value::{js_value_to_wal_entry, wal_entry_to_js_value};
use indexed_db::{Database, Factory, Transaction};
use redis_wasm_core::wal::log::{WalEntry, WalError};
use redis_wasm_core::wal::writer::WalWriterTrait;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{spawn_local, JsFuture};
use std::rc::Rc;
use std::cell::RefCell;
use crossbeam::channel;

/// WASM-compatible WAL writer using IndexedDB
#[wasm_bindgen]
pub struct IndexedDbWal {
    db_name: String,
    store_name: String,
    db: Option<Database>,
}

#[wasm_bindgen]
impl IndexedDbWal {
    /// Create a new IndexedDB WAL writer
    #[wasm_bindgen(constructor)]
    pub async fn new(db_name: &str) -> Result<IndexedDbWal, JsValue> {
        let store_name = "wal_entries".to_string();
        let db = Self::open_database(db_name, &store_name).await?;

        Ok(IndexedDbWal {
            db_name: db_name.to_string(),
            store_name,
            db: Some(db),
        })
    }

    /// Open or create the IndexedDB database
    async fn open_database(db_name: &str, store_name: &str) -> Result<Database, JsValue> {
        let factory = Factory::get().map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        let database = factory
            .open_latest_version(db_name)
            .await
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        // Create object store if it doesn't exist
        if !database.object_store_names().contains(&store_name.to_string()) {
            let version = database.version() + 1;
            let factory2 = Factory::get().map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
            let store_name_owned = store_name.to_string();
            // Return the newly-opened database (which has the object store),
            // not the pre-store handle opened above.
            return factory2
                .open(db_name, version, move |event: indexed_db::VersionChangeEvent<JsValue>| {
                    let store_name = store_name_owned.clone();
                    async move {
                        event.build_object_store(&store_name).create()?;
                        Ok(())
                    }
                })
                .await
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
                .map(|db| db.into_manual_close());
        }

        Ok(database)
    }

    /// Append a WAL entry to IndexedDB (JS API - takes JsValue)
    pub async fn append(&self, entry: &JsValue) -> Result<(), JsValue> {
        let db = self.db.as_ref().ok_or_else(|| JsValue::from_str("Database not initialized"))?;

        let store_name = self.store_name.clone();
        let entry_clone = entry.clone();

        let transaction = db
            .transaction(&[&store_name])
            .rw();

        transaction
            .run(move |tx: Transaction<JsValue>| {
                let store_name = store_name.clone();
                let entry_clone = entry_clone.clone();
                async move {
                    let store = tx.object_store(&store_name)?;
                    let key = js_sys::Date::now();
                    store.put_kv(&entry_clone, &JsValue::from_f64(key)).await?;
                    Ok(())
                }
            })
            .await
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(())
    }

    /// Flush any pending writes (no-op for IndexedDB as writes are immediate)
    pub async fn flush(&self) -> Result<(), JsValue> {
        Ok(())
    }

    /// Replay WAL entries to restore database state
    /// Note: This returns the entries for the caller to apply
    pub async fn replay(&self) -> Result<Vec<JsValue>, JsValue> {
        let db_ref = self.db.as_ref().ok_or_else(|| JsValue::from_str("Database not initialized"))?;

        let store_name = self.store_name.clone();
        let entries = db_ref
            .transaction(&[&store_name])
            .run(move |tx: Transaction<JsValue>| {
                let store_name = store_name.clone();
                async move {
                    let store = tx.object_store(&store_name)?;
                    let entries: Vec<JsValue> = store.get_all(None).await?;
                    Ok(entries)
                }
            })
            .await
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(entries)
    }

    /// Clear all WAL entries
    pub async fn clear(&self) -> Result<(), JsValue> {
        let db = self.db.as_ref().ok_or_else(|| JsValue::from_str("Database not initialized"))?;

        let store_name = self.store_name.clone();
        db.transaction(&[&store_name])
            .rw()
            .run(move |tx: Transaction<JsValue>| {
                let store_name = store_name.clone();
                async move {
                    let store = tx.object_store(&store_name)?;
                    store.clear().await?;
                    Ok(())
                }
            })
            .await
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(())
    }
}

/// Convert WalEntry to JsValue for storage
async fn wal_entry_to_js_value_inner(entry: &WalEntry) -> Result<JsValue, WalError> {
    let json = serde_json::to_string(entry).map_err(|e| WalError::Serialization(bincode::Error::new(bincode::ErrorKind::Custom(format!("{}", e)))))?;
    Ok(JsValue::from_str(&json))
}

/// Background task state - holds the IndexedDbWal
struct WalBackgroundTask {
    wal: IndexedDbWal,
}

impl WalBackgroundTask {
    /// Create a new background task
    fn new(wal: IndexedDbWal) -> Self {
        Self { wal }
    }

    /// Run the background task - processes entries from the receiver
    async fn run(mut self, receiver: channel::Receiver<WalEntry>) {
        while let Ok(entry) = receiver.recv() {
            if let Err(e) = Self::write_entry(&self.wal, &entry).await {
                tracing::error!("Failed to write WAL entry: {:?}", e);
            }
        }
    }

    /// Write a single entry to IndexedDB
    async fn write_entry(wal: &IndexedDbWal, entry: &WalEntry) -> Result<(), WalError> {
        let db = wal.db.as_ref().ok_or_else(|| WalError::IndexedDb("Database not initialized".to_string()))?;

        let store_name = wal.store_name.clone();
        let js_entry = wal_entry_to_js_value_inner(entry).await?;

        db.transaction(&[&store_name])
            .rw()
            .run(move |tx: Transaction<JsValue>| {
                let store_name = store_name.clone();
                let js_entry = js_entry.clone();
                async move {
                    let store = tx.object_store(&store_name)?;
                    store.put(&js_entry).await?;
                    Ok(())
                }
            })
            .await
            .map_err(|e| WalError::IndexedDb(format!("{:?}", e)))?;

        Ok(())
    }
}

/// WASM-compatible WAL writer that implements WalWriterTrait
/// Uses a channel to send entries to a background task running on the main thread
pub struct WasmWalWriter {
    sender: channel::Sender<WalEntry>,
}

impl WasmWalWriter {
    /// Create a new WASM WAL writer
    pub async fn new(db_name: &str) -> Result<Self, WalError> {
        let wal = IndexedDbWal::new(db_name).await.map_err(|e| WalError::IndexedDb(format!("{:?}", e)))?;

        let (sender, receiver) = channel::unbounded::<WalEntry>();

        // Spawn background task on main thread to process WAL entries
        let task = WalBackgroundTask::new(wal);
        spawn_local(async move {
            task.run(receiver).await;
        });

        Ok(Self { sender })
    }
}

#[async_trait::async_trait]
impl WalWriterTrait for WasmWalWriter {
    async fn append(&self, entry: &WalEntry) -> Result<(), WalError> {
        self.sender.send(entry.clone()).map_err(|_| WalError::IndexedDb("Channel closed".to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), WalError> {
        // Writes are async, nothing to flush
        Ok(())
    }

    async fn size(&self) -> Result<u64, WalError> {
        // Can't easily get size from background task, return 0
        Ok(0)
    }
}