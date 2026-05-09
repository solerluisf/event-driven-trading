// core/ports/service_traits.rs
//
// Traits for the application services consumed by GatewayService.
// Putting these behind traits makes GatewayService testable and
// allows swapping implementations without changing core logic.

use async_trait::async_trait;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId};
use crate::core::domain::journal::{RequestRecord, ResponseRecord};
use crate::adapters::broker::broker_error::BrokerError;

/// Handles order submission, idempotency, and broker routing.
#[async_trait]
pub trait IOrderSubmissionService: Send + Sync {
    async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, BrokerError>;
    async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), BrokerError>;
    async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), BrokerError>;
    async fn query_status(&self, query: StatusQuery) -> Result<(), BrokerError>;
}

/// Enforces kill switch and rate limits before any broker call.
pub trait IRiskManagementService: Send + Sync {
    fn check(&self, broker_id: &str, tokens: u32) -> Result<(), BrokerError>;
    fn activate_kill_switch(&self);
    fn deactivate_kill_switch(&self);
}

/// Records outbound commands and inbound confirmations.
pub trait IObservabilityService: Send + Sync {
    /// Record an outbound request to the journal.
    /// 
    /// # Returns
    /// - Ok(()) if the record was successfully persisted to disk
    /// - Err(JournalError) if persistence failed
    /// 
    /// # Durability Guarantee
    /// Implementations must ensure the data is fsync'd to disk before returning Ok.
    fn record_outbound(&self, record: RequestRecord) -> crate::core::ports::journal_repo::JournalResult<()>;
    
    /// Record an inbound response to the journal.
    /// 
    /// # Returns
    /// - Ok(()) if the record was successfully persisted to disk
    /// - Err(JournalError) if persistence failed
    /// 
    /// # Durability Guarantee
    /// Implementations must ensure the data is fsync'd to disk before returning Ok.
    fn record_inbound(&self, record: ResponseRecord) -> crate::core::ports::journal_repo::JournalResult<()>;
    
    fn emit_event(&self, event: String);
}