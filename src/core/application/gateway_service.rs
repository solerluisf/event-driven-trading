// core/application/gateway_service.rs
//
// GatewayService now holds its dependencies behind Arc<dyn Trait>
// so they can be mocked in tests and swapped without recompiling.

use std::sync::Arc;

use crate::adapters::messaging::order_lifecycle_publisher::OrderLifecyclePublisher;
use crate::core::ports::service_traits::{
    IOrderSubmissionService,
    IRiskManagementService,
    IObservabilityService,
};
use crate::core::application::connection_manager::ConnectionManager;
use crate::core::domain::execution_message::{ExecutionMessage, Message};
use crate::core::domain::market_data::{MarketSubscription, MarketDataCommand};
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId, OrderStatusResponse};
use crate::core::domain::journal::{RequestRecord, ResponseRecord};
use crate::core::ports::market_data_port::IMarketDataPort;
use crate::adapters::broker::broker_error::BrokerError;
use crate::core::domain::operation_mode::{WorkloadConfig, WorkloadType, validate_request_for_mode};
use tokio::sync::mpsc::Sender;

pub struct GatewayService {
    order_submission: Arc<dyn IOrderSubmissionService>,
    risk_management:  Arc<dyn IRiskManagementService>,
    observability:    Arc<dyn IObservabilityService>,
    connection_manager: ConnectionManager,
    stream_command_tx: Sender<MarketDataCommand>,
    order_lifecycle_publisher: OrderLifecyclePublisher,
    workload_config: WorkloadConfig,
}

impl GatewayService {
    pub fn new(
        order_submission: Arc<dyn IOrderSubmissionService>,
        risk_management:  Arc<dyn IRiskManagementService>,
        observability:    Arc<dyn IObservabilityService>,
        connection_manager: ConnectionManager,
        stream_command_tx: Sender<MarketDataCommand>,
        order_lifecycle_publisher: OrderLifecyclePublisher,
    ) -> Self {
        Self {
            order_submission,
            risk_management,
            observability,
            connection_manager,
            stream_command_tx,
            order_lifecycle_publisher,
            workload_config: WorkloadConfig::default(),
        }
    }

    /// Create a new GatewayService with workload configuration for operation mode separation
    pub fn new_with_workload_config(
        order_submission: Arc<dyn IOrderSubmissionService>,
        risk_management:  Arc<dyn IRiskManagementService>,
        observability:    Arc<dyn IObservabilityService>,
        connection_manager: ConnectionManager,
        stream_command_tx: Sender<MarketDataCommand>,
        order_lifecycle_publisher: OrderLifecyclePublisher,
        workload_config: WorkloadConfig,
    ) -> Self {
        Self {
            order_submission,
            risk_management,
            observability,
            connection_manager,
            stream_command_tx,
            order_lifecycle_publisher,
            workload_config,
        }
    }

    /// Validate that a workload type is allowed in the current configuration
    fn validate_workload(&self, workload: WorkloadType) -> Result<(), BrokerError> {
        // First check if workload is allowed in config
        if !self.workload_config.is_workload_allowed(workload) {
            return Err(BrokerError::Unknown(format!(
                "Workload {} is not allowed in current configuration",
                workload
            )));
        }

        // Then validate against operation mode
        if let Err(e) = validate_request_for_mode(
            self.workload_config.mode,
            &self.workload_config,
            workload,
        ) {
            return Err(BrokerError::Unknown(format!(
                "Workload validation failed: {}",
                e
            )));
        }

        Ok(())
    }

    pub async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, BrokerError> {
        // Validate workload is allowed
        self.validate_workload(WorkloadType::LiveTrading)?;

        // Risk check (kill switch + rate limit) before anything else
        self.risk_management.check(&cmd.symbol, 1)?;

        // Journal the outbound intent BEFORE executing.
        // This is critical: if we can't journal the intent, we must not execute.
        if let Err(e) = self.observability.record_outbound(RequestRecord {
            id: cmd.symbol.clone(),
            raw_payload: serde_json::to_string(&cmd).ok(),
            correlation_id: cmd.correlation_id.clone().or_else(|| cmd.client_order_id.clone()),
        }) {
            tracing::error!("CRITICAL: Failed to journal outbound request - refusing to execute. Error: {}", e);
            return Err(BrokerError::Unknown(format!("Journal persistence failed: {}", e)));
        }

        // Submit
        let execution_id = self.order_submission.submit_order(cmd.clone()).await?;

        // Publish order submitted event to PUB socket
        let client_order_id = cmd.client_order_id.clone();
        let symbol = cmd.symbol.clone();
        let exec_id = execution_id.0.clone();
        let event = crate::adapters::messaging::order_lifecycle_publisher::create_submitted_event(
            &exec_id,
            &symbol,
            client_order_id,
        );
        if let Err(e) = self.order_lifecycle_publisher.publish(event).await {
            tracing::warn!("failed to publish order submitted event: {}", e);
        }

        // Journal the inbound confirmation BEFORE returning success.
        // This ensures we can always reconcile what the broker told us.
        if let Err(e) = self.observability.record_inbound(ResponseRecord {
            id: execution_id.0.clone(),
            raw_payload: None,
            correlation_id: cmd.correlation_id.clone().or_else(|| cmd.client_order_id.clone()),
        }) {
            tracing::error!("CRITICAL: Failed to journal inbound confirmation for execution_id={}. Error: {}", execution_id.0, e);
            // Note: We don't fail the operation here because the broker has already executed.
            // But we log it prominently for manual reconciliation.
        }

        Ok(execution_id)
    }

    pub async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), BrokerError> {
        // Validate workload is allowed (cancel is part of LiveTrading workload)
        self.validate_workload(WorkloadType::LiveTrading)?;

        self.risk_management.check(&cmd.execution_id.0, 1)?;
        
        // Journal the outbound intent BEFORE executing
        if let Err(e) = self.observability.record_outbound(RequestRecord {
            id: cmd.execution_id.0.clone(),
            raw_payload: serde_json::to_string(&cmd).ok(),
            correlation_id: cmd.correlation_id.clone().or_else(|| Some(cmd.execution_id.0.clone())),
        }) {
            tracing::error!("CRITICAL: Failed to journal outbound cancel request - refusing to execute. Error: {}", e);
            return Err(BrokerError::Unknown(format!("Journal persistence failed: {}", e)));
        }
        
        let exec_id = cmd.execution_id.0.clone();
        
        match self.order_submission.cancel_order(cmd).await {
            Ok(()) => {
                // Publish order cancelled event to PUB socket
                let event = crate::adapters::messaging::order_lifecycle_publisher::create_cancelled_event(
                    &exec_id,
                    "unknown", // Symbol not available in CancelCmd, using placeholder
                    None,
                );
                if let Err(e) = self.order_lifecycle_publisher.publish(event).await {
                    tracing::warn!("failed to publish order cancelled event: {}", e);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), BrokerError> {
        // Validate workload is allowed (replace is part of LiveTrading workload)
        self.validate_workload(WorkloadType::LiveTrading)?;

        self.risk_management.check(&cmd.execution_id.0, 1)?;
        
        // Journal the outbound intent BEFORE executing
        if let Err(e) = self.observability.record_outbound(RequestRecord {
            id: cmd.execution_id.0.clone(),
            raw_payload: serde_json::to_string(&cmd).ok(),
            correlation_id: cmd.correlation_id.clone().or_else(|| Some(cmd.execution_id.0.clone())),
        }) {
            tracing::error!("CRITICAL: Failed to journal outbound replace request - refusing to execute. Error: {}", e);
            return Err(BrokerError::Unknown(format!("Journal persistence failed: {}", e)));
        }
        
        let exec_id = cmd.execution_id.0.clone();
        let symbol = cmd.symbol.clone();
        
        match self.order_submission.replace_order(cmd).await {
            Ok(()) => {
                // Publish order replaced event to PUB socket
                use crate::core::domain::order::{OrderLifecycleEvent, OrderLifecycleEventType};
                use serde_json::json;
                use uuid::Uuid;
                
                let event = OrderLifecycleEvent {
                    event_id: Uuid::new_v4().to_string(),
                    execution_id: exec_id,
                    client_order_id: None,
                    symbol,
                    event_type: OrderLifecycleEventType::Replaced,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    payload: json!({}),
                };
                if let Err(e) = self.order_lifecycle_publisher.publish(event).await {
                    tracing::warn!("failed to publish order replaced event: {}", e);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn query_status(&self, query: StatusQuery) -> Result<OrderStatusResponse, BrokerError> {
        // Validate workload is allowed
        self.validate_workload(WorkloadType::Query)?;

        // Note: query_status is a read-only operation, but we still track it for observability
        if let Err(e) = self.observability.record_outbound(RequestRecord {
            id: query.execution_id.0.clone(),
            raw_payload: serde_json::to_string(&query).ok(),
            correlation_id: query.correlation_id.clone().or_else(|| Some(query.execution_id.0.clone())),
        }) {
            tracing::warn!("Failed to journal outbound query_status request: {}", e);
            // Don't fail the operation for query_status since it's read-only
        }
        
        let result = self.order_submission.query_status(query.clone()).await;
        
        // Journal the response
        if let Ok(ref status_response) = result {
            if let Err(e) = self.observability.record_inbound(ResponseRecord {
                id: query.execution_id.0.clone(),
                raw_payload: serde_json::to_string(status_response).ok(),
                correlation_id: query.correlation_id.clone().or_else(|| Some(query.execution_id.0.clone())),
            }) {
                tracing::warn!("Failed to journal inbound query_status response: {}", e);
            }
        }
        
        result
    }

    pub async fn subscribe(&self, sub: MarketSubscription) -> Result<(), BrokerError> {
        // Validate workload is allowed
        self.validate_workload(WorkloadType::MarketDataStreaming)?;

        // Journal the outbound intent BEFORE executing
        if let Err(e) = self.observability.record_outbound(RequestRecord {
            id: sub.symbol.clone(),
            raw_payload: serde_json::to_string(&sub).ok(),
            correlation_id: sub.correlation_id.clone().or_else(|| Some(sub.symbol.clone())),
        }) {
            tracing::error!("CRITICAL: Failed to journal outbound subscribe request - refusing to execute. Error: {}", e);
            return Err(BrokerError::Unknown(format!("Journal persistence failed: {}", e)));
        }

        self.stream_command_tx
            .send(MarketDataCommand::Subscribe(sub))
            .await
            .map_err(|e| BrokerError::ConnectionFailed(format!("market data command send failed: {}", e)))
    }

    pub async fn unsubscribe(&self, sub: MarketSubscription) -> Result<(), BrokerError> {
        // Validate workload is allowed
        self.validate_workload(WorkloadType::MarketDataStreaming)?;

        // Journal the outbound intent BEFORE executing
        if let Err(e) = self.observability.record_outbound(RequestRecord {
            id: sub.symbol.clone(),
            raw_payload: serde_json::to_string(&sub).ok(),
            correlation_id: sub.correlation_id.clone().or_else(|| Some(sub.symbol.clone())),
        }) {
            tracing::error!("CRITICAL: Failed to journal outbound unsubscribe request - refusing to execute. Error: {}", e);
            return Err(BrokerError::Unknown(format!("Journal persistence failed: {}", e)));
        }

        self.stream_command_tx
            .send(MarketDataCommand::Unsubscribe(sub))
            .await
            .map_err(|e| BrokerError::ConnectionFailed(format!("market data command send failed: {}", e)))
    }

    /// Publish an order filled event to the order lifecycle PUB socket.
    /// This can be called when receiving fill confirmations from the broker.
    pub async fn publish_fill_event(
        &self,
        execution_id: impl Into<String>,
        symbol: impl Into<String>,
        client_order_id: Option<String>,
        filled_qty: u32,
        filled_price: f64,
    ) {
        let event = crate::adapters::messaging::order_lifecycle_publisher::create_filled_event(
            execution_id,
            symbol,
            client_order_id,
            filled_qty,
            filled_price,
        );
        if let Err(e) = self.order_lifecycle_publisher.publish(event).await {
            tracing::warn!("failed to publish order filled event: {}", e);
        }
    }

    /// Publish an order rejected event to the order lifecycle PUB socket.
    /// This can be called when receiving rejections from the broker.
    pub async fn publish_rejected_event(
        &self,
        execution_id: impl Into<String>,
        symbol: impl Into<String>,
        client_order_id: Option<String>,
        reason: impl Into<String>,
    ) {
        let event = crate::adapters::messaging::order_lifecycle_publisher::create_rejected_event(
            execution_id,
            symbol,
            client_order_id,
            reason,
        );
        if let Err(e) = self.order_lifecycle_publisher.publish(event).await {
            tracing::warn!("failed to publish order rejected event: {}", e);
        }
    }

    /// Publish a partial fill event to the order lifecycle PUB socket.
    pub async fn publish_partial_fill_event(
        &self,
        execution_id: impl Into<String>,
        symbol: impl Into<String>,
        client_order_id: Option<String>,
        filled_qty: u32,
        filled_price: f64,
        remaining_qty: u32,
    ) {
        let event = crate::adapters::messaging::order_lifecycle_publisher::create_partial_fill_event(
            execution_id,
            symbol,
            client_order_id,
            filled_qty,
            filled_price,
            remaining_qty,
        );
        if let Err(e) = self.order_lifecycle_publisher.publish(event).await {
            tracing::warn!("failed to publish order partial fill event: {}", e);
        }
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