// core/application/observability_service.rs
//
// Observability service that handles journaling, metrics, and telemetry.
// 
// CRITICAL: Journal persistence is the source of truth for all external interactions.
// All journal operations must succeed before acknowledging operations to clients.
// If journal persistence fails, the operation must fail fast to prevent data loss.

use std::sync::Arc;
use crate::core::patterns::telemetry_decorator::TelemetryDecorator;
use crate::core::ports::observability::IObservability;
use crate::core::ports::journal_repo::{IJournalRepo, JournalResult, JournalError};
use crate::core::domain::journal::{RequestRecord, ResponseRecord};

pub struct ObservabilityService {
    telemetry: TelemetryDecorator,
    observability: Arc<dyn IObservability + Send + Sync>,
    journal: Arc<dyn IJournalRepo + Send + Sync>,
}

impl ObservabilityService {
    pub fn new(
        telemetry: TelemetryDecorator,
        observability: Arc<dyn IObservability + Send + Sync>,
        journal: Arc<dyn IJournalRepo + Send + Sync>,
    ) -> Self {
        Self {
            telemetry,
            observability,
            journal,
        }
    }

    /// Record an outbound request to the journal.
    /// 
    /// # Returns
    /// - Ok(()) if the record was successfully persisted to disk
    /// - Err(JournalError) if persistence failed
    /// 
    /// # Important
    /// Callers must check this result and fail the operation if journaling fails.
    /// The journal is the source of truth - losing a journal entry means losing
    /// the ability to reconstruct what happened.
    pub fn record_outbound(&self, record: RequestRecord) -> JournalResult<()> {
        // Persist to journal first - this MUST succeed before proceeding
        self.journal.persist_outbound(record.clone())?;
        
        // Only emit telemetry after successful persistence
        self.observability.emit(format!("outbound: {}", record.id));
        self.telemetry.emit_telemetry(format!("outbound: {}", record.id));
        
        Ok(())
    }

    /// Record an inbound response to the journal.
    /// 
    /// # Returns
    /// - Ok(()) if the record was successfully persisted to disk
    /// - Err(JournalError) if persistence failed
    /// 
    /// # Important
    /// Callers must check this result. Inbound records are critical for reconciliation
    /// and must be persisted before acknowledging to upstream systems.
    pub fn record_inbound(&self, record: ResponseRecord) -> JournalResult<()> {
        // Persist to journal first - this MUST succeed
        self.journal.persist_inbound(record.clone())?;
        
        // Only emit telemetry after successful persistence
        self.observability.emit(format!("inbound: {}", record.id));
        self.telemetry.emit_telemetry(format!("inbound: {}", record.id));
        
        Ok(())
    }

    /// Emit an event without journaling (for non-critical events only).
    pub fn emit_event(&self, event: String) {
        self.observability.emit(event.clone());
        self.telemetry.emit_telemetry(event);
    }
}

/// Error type for observability operations that require durable journaling.
#[derive(Debug)]
pub enum ObservabilityError {
    JournalError(JournalError),
}

impl std::fmt::Display for ObservabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObservabilityError::JournalError(e) => write!(f, "Journal error: {}", e),
        }
    }
}

impl std::error::Error for ObservabilityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ObservabilityError::JournalError(e) => Some(e),
        }
    }
}

impl From<JournalError> for ObservabilityError {
    fn from(err: JournalError) -> Self {
        ObservabilityError::JournalError(err)
    }
}