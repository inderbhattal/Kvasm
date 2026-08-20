//! IndexedDB WAL implementation for WASM

use crate::serialization::js_value::{js_value_to_wal_entry, wal_entry_to_js_value};
use futures::channel::{mpsc, oneshot};
use futures::StreamExt;
use indexed_db::{Database, Factory, Transaction};
use redis_wasm_core::wal::log::{WalEntry, WalError};
use redis_wasm_core::wal::writer::WalWriterTrait;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

/// WASM WAL storage backed by an IndexedDB object store.
///
/// Entries are stored as JSON strings under auto-increment keys, so
/// `get_all` returns them in append order.
#[wasm_bindgen]
pub struct IndexedDbWal {
    store_name: String,
    db: Database,
}

#[wasm_bindgen]
impl IndexedDbWal {
    /// Open (or create) the WAL database
    pub async fn open(db_name: &str) -> Result<IndexedDbWal, JsValue> {
        let store_name = "wal_entries".to_string();
        let db = Self::open_database(db_name, &store_name).await?;

        Ok(IndexedDbWal { store_name, db })
    }

    /// Flush any pending writes (no-op: every append commits its own transaction)
    pub async fn flush(&self) -> Result<(), JsValue> {
        Ok(())
    }

    /// Clear all WAL entries
    pub async fn clear(&self) -> Result<(), JsValue> {
        let store_name = self.store_name.clone();
        self.db
            .transaction(&[&store_name])
            .rw()
            .run(move |tx: Transaction<JsValue>| async move {
                tx.object_store(&store_name)?.clear().await?;
                Ok(())
            })
            .await
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(())
    }
}

impl IndexedDbWal {
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
            // The upgrade open below blocks until every other connection to
            // this database closes — including the probe connection above,
            // which nothing else would ever close. Close it or deadlock.
            database.close();
            let factory2 = Factory::get().map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
            let store_name_owned = store_name.to_string();
            // Return the newly-opened database (which has the object store),
            // not the pre-store handle opened above.
            return factory2
                .open(db_name, version, move |event: indexed_db::VersionChangeEvent<JsValue>| {
                    let store_name = store_name_owned.clone();
                    async move {
                        event.build_object_store(&store_name).auto_increment().create()?;
                        Ok(())
                    }
                })
                .await
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
                .map(|db| db.into_manual_close());
        }

        Ok(database)
    }

    /// Append a batch of WAL entries under fresh auto-increment keys, all in
    /// one readwrite transaction. Committing per batch instead of per entry
    /// is what makes bursts of writes cheap; atomicity is unchanged — a
    /// crash loses at most a suffix of recent appends, exactly as before.
    async fn append_batch(&self, entries: &[WalEntry]) -> Result<(), WalError> {
        let js_entries: Vec<JsValue> = entries
            .iter()
            .map(|e| wal_entry_to_js_value(e).map_err(|err| WalError::IndexedDb(format!("{:?}", err))))
            .collect::<Result<_, _>>()?;
        let store_name = self.store_name.clone();

        self.db
            .transaction(&[&store_name])
            .rw()
            .run(move |tx: Transaction<JsValue>| async move {
                let store = tx.object_store(&store_name)?;
                for entry in &js_entries {
                    store.add(entry).await?;
                }
                Ok(())
            })
            .await
            .map_err(|e| WalError::IndexedDb(format!("{:?}", e)))?;

        Ok(())
    }

    /// Read all stored entries in append order
    pub async fn replay_entries(&self) -> Result<Vec<WalEntry>, WalError> {
        let store_name = self.store_name.clone();
        let raw = self.db
            .transaction(&[&store_name])
            .run(move |tx: Transaction<JsValue>| async move {
                let entries: Vec<JsValue> = tx.object_store(&store_name)?.get_all(None).await?;
                Ok(entries)
            })
            .await
            .map_err(|e| WalError::IndexedDb(format!("{:?}", e)))?;

        raw.iter()
            .map(|v| js_value_to_wal_entry(v).map_err(|e| WalError::IndexedDb(format!("{:?}", e))))
            .collect()
    }

    /// Atomically replace all stored entries with `entries` (compaction).
    /// Clear + re-add run in one readwrite transaction, so a crash can never
    /// leave a half-rewritten log.
    async fn rewrite_entries(&self, entries: &[WalEntry]) -> Result<(), WalError> {
        let js_entries: Vec<JsValue> = entries
            .iter()
            .map(|e| wal_entry_to_js_value(e).map_err(|err| WalError::IndexedDb(format!("{:?}", err))))
            .collect::<Result<_, _>>()?;
        let store_name = self.store_name.clone();

        self.db
            .transaction(&[&store_name])
            .rw()
            .run(move |tx: Transaction<JsValue>| async move {
                let store = tx.object_store(&store_name)?;
                store.clear().await?;
                for entry in &js_entries {
                    store.add(entry).await?;
                }
                Ok(())
            })
            .await
            .map_err(|e| WalError::IndexedDb(format!("{:?}", e)))?;

        Ok(())
    }

    /// Count stored entries
    async fn count_entries(&self) -> Result<u64, WalError> {
        let store_name = self.store_name.clone();
        let count = self.db
            .transaction(&[&store_name])
            .run(move |tx: Transaction<JsValue>| async move {
                let count = tx.object_store(&store_name)?.count().await?;
                Ok(count)
            })
            .await
            .map_err(|e| WalError::IndexedDb(format!("{:?}", e)))?;
        Ok(count as u64)
    }
}

/// Commands processed by the background writer task, in order.
enum WalCommand {
    Append(WalEntry),
    /// Acked only after every previously queued append has been attempted;
    /// errors if any append since the last successful rewrite failed.
    Flush(oneshot::Sender<Result<(), WalError>>),
    Count(oneshot::Sender<Result<u64, WalError>>),
    /// Replace the whole log with these entries (compaction)
    Rewrite(Vec<WalEntry>, oneshot::Sender<Result<(), WalError>>),
}

/// `WalWriterTrait` adapter for [`IndexedDbWal`].
///
/// IndexedDB handles are not `Send`, but the trait requires it, so writes are
/// queued to a background task on the JS event loop. `flush` round-trips a
/// command through that queue: it resolves once all prior appends have been
/// attempted, and errors if any of them failed (the failure is sticky until
/// a successful compaction rewrite makes the log whole again).
pub struct WasmWalWriter {
    sender: mpsc::UnboundedSender<WalCommand>,
}

impl WasmWalWriter {
    /// Open the WAL for `db_name` and start the background writer task
    pub async fn new(db_name: &str) -> Result<Self, WalError> {
        let wal = IndexedDbWal::open(db_name)
            .await
            .map_err(|e| WalError::IndexedDb(format!("{:?}", e)))?;
        Ok(Self::from_wal(wal))
    }

    /// Start the background writer task over an already-open WAL
    pub fn from_wal(wal: IndexedDbWal) -> Self {
        let (sender, mut receiver) = mpsc::unbounded::<WalCommand>();

        spawn_local(async move {
            // First append failure since the last successful rewrite. Sticky:
            // once an entry is lost the log stays incomplete until a rewrite
            // replaces it wholesale, so flushes keep reporting the failure.
            let mut append_error: Option<String> = None;
            // A non-append command drained while batching, to run after the
            // batch commits (queue order is preserved).
            let mut deferred: Option<WalCommand> = None;
            loop {
                let cmd = match deferred.take() {
                    Some(cmd) => cmd,
                    None => match receiver.next().await {
                        Some(cmd) => cmd,
                        None => break,
                    },
                };
                match cmd {
                    WalCommand::Append(entry) => {
                        // Batch every append already sitting in the queue
                        // into one IndexedDB transaction.
                        let mut batch = vec![entry];
                        while let Ok(Some(next)) = receiver.try_next() {
                            match next {
                                WalCommand::Append(entry) => batch.push(entry),
                                other => {
                                    deferred = Some(other);
                                    break;
                                }
                            }
                        }
                        if let Err(e) = wal.append_batch(&batch).await {
                            web_sys::console::error_1(&JsValue::from_str(&format!(
                                "redis-wasm: WAL write of {} entr{} failed: {}",
                                batch.len(),
                                if batch.len() == 1 { "y" } else { "ies" },
                                e
                            )));
                            append_error.get_or_insert_with(|| e.to_string());
                        }
                    }
                    WalCommand::Flush(ack) => {
                        let result = match &append_error {
                            None => Ok(()),
                            Some(msg) => Err(WalError::IndexedDb(format!(
                                "a WAL append failed; the log is incomplete until \
                                 a compaction rewrites it: {msg}"
                            ))),
                        };
                        let _ = ack.send(result);
                    }
                    WalCommand::Count(reply) => {
                        let _ = reply.send(wal.count_entries().await);
                    }
                    WalCommand::Rewrite(entries, reply) => {
                        let result = wal.rewrite_entries(&entries).await;
                        if result.is_ok() {
                            // The snapshot supersedes any lost appends.
                            append_error = None;
                        }
                        let _ = reply.send(result);
                    }
                }
            }
        });

        Self { sender }
    }
}

#[async_trait::async_trait]
impl WalWriterTrait for WasmWalWriter {
    async fn append(&self, entry: &WalEntry) -> Result<(), WalError> {
        self.sender
            .unbounded_send(WalCommand::Append(entry.clone()))
            .map_err(|_| WalError::IndexedDb("WAL writer task stopped".to_string()))
    }

    async fn flush(&self) -> Result<(), WalError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sender
            .unbounded_send(WalCommand::Flush(ack_tx))
            .map_err(|_| WalError::IndexedDb("WAL writer task stopped".to_string()))?;
        ack_rx
            .await
            .map_err(|_| WalError::IndexedDb("WAL writer task stopped".to_string()))?
    }

    async fn size(&self) -> Result<u64, WalError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .unbounded_send(WalCommand::Count(reply_tx))
            .map_err(|_| WalError::IndexedDb("WAL writer task stopped".to_string()))?;
        reply_rx
            .await
            .map_err(|_| WalError::IndexedDb("WAL writer task stopped".to_string()))?
    }

    async fn rewrite(&self, entries: &[WalEntry]) -> Result<(), WalError> {
        // Queued behind pending appends, so the rewrite lands on a log that
        // reflects everything enqueued before the snapshot was taken.
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .unbounded_send(WalCommand::Rewrite(entries.to_vec(), reply_tx))
            .map_err(|_| WalError::IndexedDb("WAL writer task stopped".to_string()))?;
        reply_rx
            .await
            .map_err(|_| WalError::IndexedDb("WAL writer task stopped".to_string()))?
    }
}
