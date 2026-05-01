// core/application/service_impls.rs
//
// Implements the port traits (IOrderSubmissionService, IRiskManagementService,
// IObservabilityService) on the existing application service structs so they
// can be passed as Arc<dyn Trait> to GatewayService.

use async_trait::async_trait;

use crate::core::application::order_submission_service::OrderSubmissionService;
use crate::core::application::risk_management_service::RiskManagementService;
use crate::core::application::observability_service::ObservabilityService;

use crate::core::ports::service_traits::{
    IOrderSubmissionService,
    IRiskManagementService,
    IObservabilityService,
};

use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId};
use crate::core::domain::journal::{RequestRecord, ResponseRecord};
use crate::adapters::broker::broker_error::BrokerError;

// ── OrderSubmissionService ────────────────────────────────────────────────────

#[async_trait]
impl IOrderSubmissionService for OrderSubmissionService {
    async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, BrokerError> {
        self.submit_order(cmd).await
    }
    async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), BrokerError> {
        self.cancel_order(cmd).await
    }
    async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), BrokerError> {
        self.replace_order(cmd).await
    }
    async fn query_status(&self, query: StatusQuery) -> Result<(), BrokerError> {
        self.query_status(query).await
    }
}

// ── RiskManagementService ─────────────────────────────────────────────────────

impl IRiskManagementService for RiskManagementService {
    fn check(&self, broker_id: &str, tokens: u32) -> Result<(), BrokerError> {
        self.check(broker_id, tokens)
    }
    fn activate_kill_switch(&self) {
        self.activate_kill_switch();
    }
    fn deactivate_kill_switch(&self) {
        self.deactivate_kill_switch();
    }
}

// ── ObservabilityService ──────────────────────────────────────────────────────

impl IObservabilityService for ObservabilityService {
    fn record_outbound(&self, record: RequestRecord) {
        self.record_outbound(record);
    }
    fn record_inbound(&self, record: ResponseRecord) {
        self.record_inbound(record);
    }
    fn emit_event(&self, event: String) {
        self.emit_event(event);
    }
}