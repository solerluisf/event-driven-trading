// core/application/gateway_service.rs
//
// GatewayService now holds its dependencies behind Arc<dyn Trait>
// so they can be mocked in tests and swapped without recompiling.

use std::sync::Arc;

use crate::core::ports::service_traits::{
    IOrderSubmissionService,
    IRiskManagementService,
    IObservabilityService,
};
use crate::core::application::connection_manager::ConnectionManager;
use crate::core::domain::execution_message::{ExecutionMessage, Message};
use crate::core::domain::market_data::{MarketSubscription, MarketDataCommand};
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId};
use crate::core::domain::journal::{RequestRecord, ResponseRecord};
use crate::core::ports::market_data_port::IMarketDataPort;
use crate::adapters::broker::broker_error::BrokerError;
use tokio::sync::mpsc::Sender;

pub struct GatewayService {
    order_submission: Arc<dyn IOrderSubmissionService>,
    risk_management:  Arc<dyn IRiskManagementService>,
    observability:    Arc<dyn IObservabilityService>,
    connection_manager: ConnectionManager,
    stream_command_tx: Sender<MarketDataCommand>,
}

impl GatewayService {
    pub fn new(
        order_submission: Arc<dyn IOrderSubmissionService>,
        risk_management:  Arc<dyn IRiskManagementService>,
        observability:    Arc<dyn IObservabilityService>,
        connection_manager: ConnectionManager,
        stream_command_tx: Sender<MarketDataCommand>,
    ) -> Self {
        Self {
            order_submission,
            risk_management,
            observability,
            connection_manager,
            stream_command_tx,
        }
    }

    pub async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, BrokerError> {
        // Risk check (kill switch + rate limit) before anything else
        self.risk_management.check(&cmd.symbol, 1)?;

        // Journal the outbound intent
        self.observability.record_outbound(RequestRecord {
            id: cmd.symbol.clone(),
            raw_payload: serde_json::to_string(&cmd).ok(),
        });

        // Submit
        let execution_id = self.order_submission.submit_order(cmd).await?;

        // Journal the inbound confirmation
        self.observability.record_inbound(ResponseRecord {
            id: execution_id.0.clone(),
            raw_payload: None,
        });

        Ok(execution_id)
    }

    pub async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), BrokerError> {
        self.risk_management.check(&cmd.execution_id.0, 1)?;
        self.observability.record_outbound(RequestRecord {
            id: cmd.execution_id.0.clone(),
            raw_payload: serde_json::to_string(&cmd).ok(),
        });
        self.order_submission.cancel_order(cmd).await
    }

    pub async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), BrokerError> {
        self.risk_management.check(&cmd.execution_id.0, 1)?;
        self.observability.record_outbound(RequestRecord {
            id: cmd.execution_id.0.clone(),
            raw_payload: serde_json::to_string(&cmd).ok(),
        });
        self.order_submission.replace_order(cmd).await
    }

    pub async fn query_status(&self, query: StatusQuery) -> Result<(), BrokerError> {
        self.order_submission.query_status(query).await
    }

    pub async fn subscribe(&self, sub: MarketSubscription) -> Result<(), BrokerError> {
        self.observability.record_outbound(RequestRecord {
            id: sub.symbol.clone(),
            raw_payload: serde_json::to_string(&sub).ok(),
        });

        self.stream_command_tx
            .send(MarketDataCommand::Subscribe(sub))
            .await
            .map_err(|e| BrokerError::ConnectionFailed(format!("market data command send failed: {}", e)))
    }

    pub async fn unsubscribe(&self, sub: MarketSubscription) -> Result<(), BrokerError> {
        self.observability.record_outbound(RequestRecord {
            id: sub.symbol.clone(),
            raw_payload: serde_json::to_string(&sub).ok(),
        });

        self.stream_command_tx
            .send(MarketDataCommand::Unsubscribe(sub))
            .await
            .map_err(|e| BrokerError::ConnectionFailed(format!("market data command send failed: {}", e)))
    }

    pub fn handle_execution_message(&self, _msg: ExecutionMessage) {}
    pub fn handle_market_data(&self, _msg: Message) {}

    pub fn shutdown(&self) {
        self.risk_management.activate_kill_switch();
    }

    pub fn enter_simulation_mode(&self) {}
}

impl IMarketDataPort for GatewayService {
    fn subscribe(&self, sub: MarketSubscription) {
        let _ = self.stream_command_tx.try_send(MarketDataCommand::Subscribe(sub));
    }

    fn unsubscribe(&self, sub: MarketSubscription) {
        let _ = self.stream_command_tx.try_send(MarketDataCommand::Unsubscribe(sub));
    }
}