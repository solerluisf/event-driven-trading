// adapters/persistence/journal_storage.rs
//
// Redb-backed append-only event journal (pure Rust embedded database).
// Uses separate tables for organization:
//   outbound_commands    — every command sent to a broker
//   inbound_confirmations — every response/event received from a broker
//
// Key format: "timestamp:id" ensures chronological ordering.
// Value: JSON-serialized payload for easy deserialization.
//
// The database path is read from the JOURNAL_DB_PATH env var,
// defaulting to "journal.db" in the working directory.

use redb::{Database, TableDefinition};
use chrono::Utc;

use crate::core::ports::journal_repo::IJournalRepo;
use crate::core::domain::journal::{RequestRecord, ResponseRecord};

// Table definitions
const OUTBOUND_TABLE: TableDefinition<&str, &str> = TableDefinition::new("outbound_commands");
const INBOUND_TABLE: TableDefinition<&str, &str> = TableDefinition::new("inbound_confirmations");

pub struct JournalStorage {
    db: Database,
}

impl JournalStorage {
    pub fn new() -> Self {
        let path = std::env::var("JOURNAL_DB_PATH")
            .unwrap_or_else(|_| "journal.db".into());

        let db = Database::create(&path)
            .unwrap_or_else(|e| panic!("Failed to open Redb at {}: {}", path, e));

        tracing::info!("JournalStorage opened at {} with Redb", path);

        Self { db }
    }
}

impl Default for JournalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl IJournalRepo for JournalStorage {
    fn persist_outbound(&self, record: RequestRecord) {
        let now = Utc::now().timestamp_millis();
        let key = format!("{}:{}", now, record.id);
        let payload = serde_json::to_string(&record)
            .unwrap_or_else(|_| format!(r#"{{"id":"{}"}}"#, record.id));

        let write_txn = match self.db.begin_write() {
            Ok(txn) => txn,
            Err(e) => {
                tracing::error!("journal persist_outbound transaction failed: {}", e);
                return;
            }
        };

        {
            let mut table = match write_txn.open_table(OUTBOUND_TABLE) {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("failed to open outbound_commands table: {}", e);
                    return;
                }
            };

            if let Err(e) = table.insert(key.as_str(), payload.as_str()) {
                tracing::error!("journal persist_outbound insert failed: {}", e);
                return;
            }
        }

        if let Err(e) = write_txn.commit() {
            tracing::error!("journal persist_outbound commit failed: {}", e);
        } else {
            tracing::debug!("persisted outbound command id={}", record.id);
        }
    }

    fn persist_inbound(&self, record: ResponseRecord) {
        let now = Utc::now().timestamp_millis();
        let key = format!("{}:{}", now, record.id);
        let payload = serde_json::to_string(&record)
            .unwrap_or_else(|_| format!(r#"{{"id":"{}"}}"#, record.id));

        let write_txn = match self.db.begin_write() {
            Ok(txn) => txn,
            Err(e) => {
                tracing::error!("journal persist_inbound transaction failed: {}", e);
                return;
            }
        };

        {
            let mut table = match write_txn.open_table(INBOUND_TABLE) {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("failed to open inbound_confirmations table: {}", e);
                    return;
                }
            };

            if let Err(e) = table.insert(key.as_str(), payload.as_str()) {
                tracing::error!("journal persist_inbound insert failed: {}", e);
                return;
            }
        }

        if let Err(e) = write_txn.commit() {
            tracing::error!("journal persist_inbound commit failed: {}", e);
        } else {
            tracing::debug!("persisted inbound confirmation id={}", record.id);
        }
    }

    fn replay(&self, query: String) -> Vec<ResponseRecord> {
        let read_txn = match self.db.begin_read() {
            Ok(txn) => txn,
            Err(e) => {
                tracing::error!("journal replay transaction failed: {}", e);
                return vec![];
            }
        };

        let table = match read_txn.open_table(INBOUND_TABLE) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("failed to open inbound_confirmations table: {}", e);
                return vec![];
            }
        };

        let mut results = vec![];

        // Use a full range scan to iterate through all records
        match table.range::<&str>("".."zzz") {
            Ok(iter) => {
                for entry in iter {
                    match entry {
                        Ok((_, value)) => {
                            let payload_str = value.value();
                            match serde_json::from_str::<ResponseRecord>(payload_str) {
                                Ok(record) => {
                                    if record.id.contains(&query) {
                                        results.push(record);
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("failed to deserialize response record: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("failed to read table entry: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to iterate inbound table: {}", e);
            }
        }

        results
    }
}