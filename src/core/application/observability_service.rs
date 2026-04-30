// core/application/observability_service.rs

use std::sync::Arc;
use crate::core::patterns::telemetry_decorator::TelemetryDecorator;
use crate::core::ports::observability::IObservability;
use crate::core::ports::journal_repo::IJournalRepo;
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

    pub fn record_outbound(&self, record: RequestRecord) {
        self.journal.persist_outbound(record.clone());
        self.observability.emit(format!("outbound: {}", record.id));
        self.telemetry.emit_telemetry(format!("outbound: {}", record.id));
    }

    pub fn record_inbound(&self, record: ResponseRecord) {
        self.journal.persist_inbound(record.clone());
        self.observability.emit(format!("inbound: {}", record.id));
        self.telemetry.emit_telemetry(format!("inbound: {}", record.id));
    }

    pub fn emit_event(&self, event: String) {
        self.observability.emit(event.clone());
        self.telemetry.emit_telemetry(event);
    }
}