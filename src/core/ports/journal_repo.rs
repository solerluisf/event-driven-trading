// journal_repo.rs
//
// Journal repository trait for durable persistence of request/response records.
// All persistence operations return Result to ensure durability guarantees
// can be verified before acknowledging operations to clients.

use crate::core::domain::journal::{RequestRecord, ResponseRecord};

/// Errors that can occur during journal operations
#[derive(Debug, Clone)]
pub enum JournalError {
    /// Database transaction failed
    TransactionFailed(String),
    /// Serialization failed
    SerializationFailed(String),
    /// Table access failed
    TableAccessFailed(String),
    /// Commit failed - data may not be durable
    CommitFailed(String),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::TransactionFailed(msg) => write!(f, "Transaction failed: {}", msg),
            JournalError::SerializationFailed(msg) => write!(f, "Serialization failed: {}", msg),
            JournalError::TableAccessFailed(msg) => write!(f, "Table access failed: {}", msg),
            JournalError::CommitFailed(msg) => write!(f, "Commit failed: {}", msg),
        }
    }
}

impl std::error::Error for JournalError {}

pub type JournalResult<T> = std::result::Result<T, JournalError>;

/// Trait for durable journal storage.
/// 
/// IMPORTANT: All persistence methods MUST ensure data is flushed to disk
/// before returning Ok. This is critical for durability guarantees - the
/// journal is the source of truth and must be persisted before acknowledging
/// operations to clients.
pub trait IJournalRepo: Send + Sync {
    /// Persist an outbound request to the journal.
    /// 
    /// # Returns
    /// - Ok(()) if the record was successfully persisted to disk
    /// - Err(JournalError) if persistence failed
    /// 
    /// # Durability Guarantee
    /// Implementations must ensure the data is fsync'd to disk before returning Ok.
    fn persist_outbound(&self, record: RequestRecord) -> JournalResult<()>;
    
    /// Persist an inbound response to the journal.
    /// 
    /// # Returns
    /// - Ok(()) if the record was successfully persisted to disk
    /// - Err(JournalError) if persistence failed
    /// 
    /// # Durability Guarantee
    /// Implementations must ensure the data is fsync'd to disk before returning Ok.
    fn persist_inbound(&self, record: ResponseRecord) -> JournalResult<()>;
    
    /// Replay records from the journal matching the query.
    fn replay(&self, query: String) -> Vec<ResponseRecord>;
}