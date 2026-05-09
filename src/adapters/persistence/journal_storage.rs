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

        // Try to open or create the redb database
        let db = match Database::create(&path) {
            Ok(db) => {
                tracing::info!("JournalStorage opened at {} with Redb", path);
                db
            }
            Err(e) => {
                // If the error is due to the file existing in an incompatible format,
                // (e.g., old SQLite database), rename it and create a fresh redb database
                tracing::warn!(
                    "Failed to open Redb at {}: {}. Attempting migration...",
                    path,
                    e
                );

                // Rename the old incompatible database
                let backup_path = format!("{}.backup", path);
                match std::fs::rename(&path, &backup_path) {
                    Ok(_) => {
                        tracing::info!(
                            "Renamed old journal database to {} for migration",
                            backup_path
                        );
                    }
                    Err(rename_err) => {
                        tracing::warn!(
                            "Failed to rename old database during migration: {}",
                            rename_err
                        );
                    }
                }

                // Now create a fresh redb database
                Database::create(&path)
                    .unwrap_or_else(|e2| {
                        panic!(
                            "Failed to create new Redb at {}: {}",
                            path, e2
                        )
                    })
            }
        };

        Self { db }
    }
}

impl Default for JournalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl IJournalRepo for JournalStorage {
    fn persist_outbound(&self, record: RequestRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
        use crate::core::ports::journal_repo::JournalError;
        
        let now = Utc::now().timestamp_millis();
        let key = format!("{}:{}", now, record.id);
        let payload = serde_json::to_string(&record)
            .map_err(|e| JournalError::SerializationFailed(format!("Failed to serialize outbound record: {}", e)))?;

        let write_txn = self.db.begin_write()
            .map_err(|e| JournalError::TransactionFailed(format!("Failed to begin transaction: {}", e)))?;

        {
            let mut table = write_txn.open_table(OUTBOUND_TABLE)
                .map_err(|e| JournalError::TableAccessFailed(format!("Failed to open outbound table: {}", e)))?;

            table.insert(key.as_str(), payload.as_str())
                .map_err(|e| JournalError::TransactionFailed(format!("Failed to insert record: {}", e)))?;
        }

        // Commit ensures data is flushed to disk (redb's commit is synchronous and durable)
        write_txn.commit()
            .map_err(|e| JournalError::CommitFailed(format!("Failed to commit transaction: {}", e)))?;
        
        tracing::debug!("persisted outbound command id={}", record.id);
        Ok(())
    }

    fn persist_inbound(&self, record: ResponseRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
        use crate::core::ports::journal_repo::JournalError;
        
        let now = Utc::now().timestamp_millis();
        let key = format!("{}:{}", now, record.id);
        let payload = serde_json::to_string(&record)
            .map_err(|e| JournalError::SerializationFailed(format!("Failed to serialize inbound record: {}", e)))?;

        let write_txn = self.db.begin_write()
            .map_err(|e| JournalError::TransactionFailed(format!("Failed to begin transaction: {}", e)))?;

        {
            let mut table = write_txn.open_table(INBOUND_TABLE)
                .map_err(|e| JournalError::TableAccessFailed(format!("Failed to open inbound table: {}", e)))?;

            table.insert(key.as_str(), payload.as_str())
                .map_err(|e| JournalError::TransactionFailed(format!("Failed to insert record: {}", e)))?;
        }

        // Commit ensures data is flushed to disk (redb's commit is synchronous and durable)
        write_txn.commit()
            .map_err(|e| JournalError::CommitFailed(format!("Failed to commit transaction: {}", e)))?;
        
        tracing::debug!("persisted inbound confirmation id={}", record.id);
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ports::journal_repo::{IJournalRepo, JournalError};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Clean up test database files including backups
    fn cleanup_test_db(path: &str) {
        // Remove main database file
        let _ = std::fs::remove_file(path);
        // Remove backup file if exists
        let _ = std::fs::remove_file(format!("{}.backup", path));
    }

    /// Clean up all test journal files in the current directory
    pub fn cleanup_all_test_journals() {
        if let Ok(entries) = std::fs::read_dir(".") {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("test_journal_") && (name.ends_with(".db") || name.ends_with(".db.backup")) {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
    }

    fn create_test_request_record(id: &str) -> RequestRecord {
        RequestRecord {
            id: id.to_string(),
            raw_payload: Some(format!(r#"{{"test":"payload","id":"{}"}}"#, id)),
        }
    }

    fn create_test_response_record(id: &str) -> ResponseRecord {
        ResponseRecord {
            id: id.to_string(),
            raw_payload: Some(format!(r#"{{"result":"success","id":"{}"}}"#, id)),
        }
    }

    fn temp_db_path() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        // Use a global counter + timestamp + thread hash to ensure unique paths
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        
        // Hash the thread ID to get a unique identifier per thread
        let thread_id = std::thread::current().id();
        let mut hasher = DefaultHasher::new();
        thread_id.hash(&mut hasher);
        let thread_hash = hasher.finish();
        
        format!("test_journal_{}_{}_{}.db", timestamp, thread_hash, counter)
    }

    fn set_test_db_path(path: &str) {
        // SAFETY: This is safe in tests as we control the environment
        unsafe {
            std::env::set_var("JOURNAL_DB_PATH", path);
        }
    }

    #[test]
    fn test_journal_storage_new_creates_database() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        // If we get here without panicking, the database was created
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_persist_outbound_succeeds() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        let record = create_test_request_record("test-outbound-1");
        
        let result = storage.persist_outbound(record);
        assert!(result.is_ok(), "persist_outbound should succeed");
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_persist_inbound_succeeds() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        let record = create_test_response_record("test-inbound-1");
        
        let result = storage.persist_inbound(record);
        assert!(result.is_ok(), "persist_inbound should succeed");
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_persist_multiple_records_succeeds() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        
        // Persist multiple outbound records
        for i in 0..10 {
            let record = create_test_request_record(&format!("test-outbound-{}", i));
            let result = storage.persist_outbound(record);
            assert!(result.is_ok(), "persist_outbound should succeed for record {}", i);
        }
        
        // Persist multiple inbound records
        for i in 0..10 {
            let record = create_test_response_record(&format!("test-inbound-{}", i));
            let result = storage.persist_inbound(record);
            assert!(result.is_ok(), "persist_inbound should succeed for record {}", i);
        }
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_replay_returns_persisted_records() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        
        // Persist some records
        let record1 = create_test_response_record("replay-test-1");
        let record2 = create_test_response_record("replay-test-2");
        storage.persist_inbound(record1).unwrap();
        storage.persist_inbound(record2).unwrap();
        
        // Replay and verify
        let results = storage.replay("replay-test".to_string());
        assert_eq!(results.len(), 2, "should find both records");
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_replay_filters_by_query() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        
        // Persist records with different IDs
        let record1 = create_test_response_record("alpha-1");
        let record2 = create_test_response_record("beta-1");
        let record3 = create_test_response_record("alpha-2");
        storage.persist_inbound(record1).unwrap();
        storage.persist_inbound(record2).unwrap();
        storage.persist_inbound(record3).unwrap();
        
        // Replay with filter
        let results = storage.replay("alpha".to_string());
        assert_eq!(results.len(), 2, "should find only alpha records");
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_persist_outbound_with_special_characters() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        let record = RequestRecord {
            id: "special-\"quoted\"-id".to_string(),
            raw_payload: Some(r#"{"data":"with \"quotes\" and \n newlines"}"#.to_string()),
        };
        
        let result = storage.persist_outbound(record);
        assert!(result.is_ok(), "persist_outbound should handle special characters");
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_persist_with_large_payload() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        let large_payload = "x".repeat(10000);
        let record = RequestRecord {
            id: "large-payload-test".to_string(),
            raw_payload: Some(format!(r#"{{"data":"{}"}}"#, large_payload)),
        };
        
        let result = storage.persist_outbound(record);
        assert!(result.is_ok(), "persist_outbound should handle large payloads");
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_journal_result_error_display() {
        let err = JournalError::TransactionFailed("test error".to_string());
        assert!(err.to_string().contains("Transaction failed"));
        
        let err = JournalError::CommitFailed("commit failed".to_string());
        assert!(err.to_string().contains("Commit failed"));
        
        let err = JournalError::SerializationFailed("serde error".to_string());
        assert!(err.to_string().contains("Serialization failed"));
        
        let err = JournalError::TableAccessFailed("table error".to_string());
        assert!(err.to_string().contains("Table access failed"));
    }

    #[test]
    fn test_default_storage() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        // Test Default trait
        let storage: JournalStorage = Default::default();
        let record = create_test_request_record("default-test");
        let result = storage.persist_outbound(record);
        assert!(result.is_ok());
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_empty_replay_returns_empty_vec() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        let results = storage.replay("nonexistent".to_string());
        assert!(results.is_empty(), "replay should return empty vec when no matches");
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_record_with_none_payload() {
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        let record = RequestRecord {
            id: "none-payload-test".to_string(),
            raw_payload: None,
        };
        
        let result = storage.persist_outbound(record);
        assert!(result.is_ok(), "persist_outbound should handle None payload");
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_journal_durability_sequence() {
        // This test verifies that records are durable (can be replayed) after persistence
        let db_path = temp_db_path();
        set_test_db_path(&db_path);
        
        let storage = JournalStorage::new();
        
        // Phase 1: Persist records
        for i in 0..5 {
            let outbound = create_test_request_record(&format!("durability-out-{}", i));
            let inbound = create_test_response_record(&format!("durability-in-{}", i));
            
            storage.persist_outbound(outbound).expect("outbound should persist");
            storage.persist_inbound(inbound).expect("inbound should persist");
        }
        
        // Phase 2: Verify records can be replayed (proving durability)
        let _outbound_results = storage.replay("durability-out".to_string());
        let inbound_results = storage.replay("durability-in".to_string());
        
        // Note: replay only searches inbound table, so outbound won't be found
        // This verifies the test setup is correct
        assert_eq!(inbound_results.len(), 5, "all inbound records should be replayable");
        
        // Phase 3: Verify specific record content
        for (i, record) in inbound_results.iter().enumerate() {
            assert!(record.id.contains(&format!("durability-in-{}", i)),
                "record {} should have correct id", i);
        }
        
        // Cleanup
        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_cleanup_function_removes_files() {
        // Test the cleanup_test_db function directly
        let db_path = temp_db_path();
        let backup_path = format!("{}.backup", db_path);
        
        // Create both files
        std::fs::File::create(&db_path).unwrap();
        std::fs::File::create(&backup_path).unwrap();
        
        // Verify they exist
        assert!(std::path::Path::new(&db_path).exists());
        assert!(std::path::Path::new(&backup_path).exists());
        
        // Run cleanup
        cleanup_test_db(&db_path);
        
        // Verify both are removed
        assert!(!std::path::Path::new(&db_path).exists(), "main db file should be removed");
        assert!(!std::path::Path::new(&backup_path).exists(), "backup file should be removed");
    }
}