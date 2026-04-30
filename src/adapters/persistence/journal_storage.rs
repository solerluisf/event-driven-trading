// adapters/persistence/journal_storage.rs
//
// SQLite-backed append-only journal.
// Two tables:
//   outbound_commands    — every command sent to a broker
//   inbound_confirmations — every response/event received from a broker
//
// Both rows carry:
//   id          TEXT  — correlation / idempotency id
//   occurred_at TEXT  — RFC-3339 UTC timestamp
//   raw_payload TEXT  — full JSON payload
//
// The database file path is read from the JOURNAL_DB_PATH env var,
// defaulting to "journal.db" in the working directory.

use rusqlite::{Connection, params};
use std::sync::Mutex;
use chrono::Utc;

use crate::core::ports::journal_repo::IJournalRepo;
use crate::core::domain::journal::{RequestRecord, ResponseRecord};

pub struct JournalStorage {
    conn: Mutex<Connection>,
}

impl JournalStorage {
    pub fn new() -> Self {
        let path = std::env::var("JOURNAL_DB_PATH")
            .unwrap_or_else(|_| "journal.db".into());

        let conn = Connection::open(&path)
            .unwrap_or_else(|e| panic!("Failed to open journal DB at {}: {}", path, e));

        // Enable WAL mode for better concurrent write performance
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("Failed to set WAL mode");

        // Create tables if they don't exist
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS outbound_commands (
                id           TEXT NOT NULL,
                occurred_at  TEXT NOT NULL,
                raw_payload  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inbound_confirmations (
                id           TEXT NOT NULL,
                occurred_at  TEXT NOT NULL,
                raw_payload  TEXT NOT NULL
            );",
        )
        .expect("Failed to create journal tables");

        tracing::info!("JournalStorage opened at {}", path);

        Self {
            conn: Mutex::new(conn),
        }
    }
}

// Keep Default working for code that uses JournalStorage::default()
impl Default for JournalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl IJournalRepo for JournalStorage {
    fn persist_outbound(&self, record: RequestRecord) {
        let now = Utc::now().to_rfc3339();
        let payload = serde_json::to_string(&record)
            .unwrap_or_else(|_| format!(r#"{{"id":"{}"}}"#, record.id));

        let conn = self.conn.lock().unwrap();
        if let Err(e) = conn.execute(
            "INSERT INTO outbound_commands (id, occurred_at, raw_payload) VALUES (?1, ?2, ?3)",
            params![record.id, now, payload],
        ) {
            tracing::error!("journal persist_outbound failed: {}", e);
        }
    }

    fn persist_inbound(&self, record: ResponseRecord) {
        let now = Utc::now().to_rfc3339();
        let payload = serde_json::to_string(&record)
            .unwrap_or_else(|_| format!(r#"{{"id":"{}"}}"#, record.id));

        let conn = self.conn.lock().unwrap();
        if let Err(e) = conn.execute(
            "INSERT INTO inbound_confirmations (id, occurred_at, raw_payload) VALUES (?1, ?2, ?3)",
            params![record.id, now, payload],
        ) {
            tracing::error!("journal persist_inbound failed: {}", e);
        }
    }

    fn replay(&self, query: String) -> Vec<ResponseRecord> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT id FROM inbound_confirmations WHERE id LIKE ?1 ORDER BY occurred_at ASC",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("journal replay prepare failed: {}", e);
                return vec![];
            }
        };

        let pattern = format!("%{}%", query);
        let rows = stmt.query_map(params![pattern], |row| {
            Ok(ResponseRecord { id: row.get(0)?, raw_payload: None }) 
        });

        match rows {
            Ok(iter) => iter.filter_map(|r| r.ok()).collect(),
            Err(e) => {
                tracing::error!("journal replay query failed: {}", e);
                vec![]
            }
        }
    }
}