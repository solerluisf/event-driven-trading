// adapters/messaging/bus_adapter.rs
//
// Binds a ZeroMQ REP socket and drives the main command-listening loop.
// Every inbound frame is deserialized as a GatewayRequest, routed to
// GatewayService, and the result is serialized back as a GatewayResponse.
//
// Socket topology:
//
//   Execution Service (REQ)  ──►  Gateway (REP, this file)
//
// One message in, one reply out — exactly REQ/REP semantics.
//
// IMPORTANT: ZMQ REP sockets require strict recv→send alternation with no
// interleaving. This implementation uses a dedicated thread for the socket
// and channels for communication to ensure the state machine is never violated.

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use zmq::Context;

use crate::core::application::gateway_service::GatewayService;
use crate::core::application::rate_limiter::RateLimiterManager;
use crate::core::domain::wire_message::{
    GatewayRequest, GatewayResponse, ResponsePayload, ErrorPayload, BackPressureInfo, BackPressureRecommendation,
};
use super::wire_codec::{
    decode_gateway_request, encode_gateway_response,
};

/// Message sent from ZMQ thread to async processor
struct ZmqMessage {
    /// Raw bytes received from ZMQ
    data: Vec<u8>,
    /// Channel to send response back
    response_tx: oneshot::Sender<Vec<u8>>,
}

/// Default threshold percentage for considering broker near limit (20%).
pub const DEFAULT_NEAR_LIMIT_THRESHOLD_PERCENT: f64 = 20.0;

pub struct BusAdapter {
    /// ZeroMQ endpoint string, e.g. "tcp://127.0.0.1:5555"
    endpoint: String,
    gateway: Arc<GatewayService>,
    /// Rate limiter for checking back-pressure status.
    rate_limiter: Arc<RateLimiterManager>,
    /// Broker ID for rate limiting and back-pressure.
    broker_id: String,
    /// Threshold percentage below which broker is considered near limit.
    near_limit_threshold_percent: f64,
}

impl BusAdapter {
    pub fn new(
        endpoint: impl Into<String>,
        gateway: Arc<GatewayService>,
        rate_limiter: Arc<RateLimiterManager>,
        broker_id: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            gateway,
            rate_limiter,
            broker_id: broker_id.into(),
            near_limit_threshold_percent: DEFAULT_NEAR_LIMIT_THRESHOLD_PERCENT,
        }
    }

    /// Set the threshold percentage for considering broker near limit.
    /// Default is 20% - when remaining tokens fall below 20% of capacity,
    /// back-pressure will be signaled.
    pub fn with_near_limit_threshold(mut self, threshold_percent: f64) -> Self {
        self.near_limit_threshold_percent = threshold_percent.clamp(0.0, 100.0);
        self
    }

    /// Build back-pressure information if the broker is near its limit.
    fn build_back_pressure_info(&self) -> Option<BackPressureInfo> {
        let status = self.rate_limiter
            .get_back_pressure_status(&self.broker_id, self.near_limit_threshold_percent)?;
        
        // Only include back-pressure info if near limit
        if !status.is_near_limit {
            return None;
        }

        let recommendation = if status.percent_remaining < 5.0 {
            BackPressureRecommendation::Pause
        } else if status.percent_remaining < self.near_limit_threshold_percent {
            BackPressureRecommendation::SlowDown
        } else {
            BackPressureRecommendation::Normal
        };

        Some(BackPressureInfo {
            broker_id: self.broker_id.clone(),
            tokens_remaining: status.tokens_remaining,
            capacity: status.capacity,
            percent_remaining: status.percent_remaining,
            is_near_limit: status.is_near_limit,
            recommendation,
        })
    }

    /// Bind the REP socket and loop forever processing commands.
    /// Call this as the main async task in main().
    ///
    /// This implementation uses a dedicated thread for the ZMQ socket to ensure
    /// strict recv→send alternation is maintained without interference from
    /// tokio's thread pool scheduling.
    pub async fn listen(&self) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.endpoint.clone();
        let gateway = self.gateway.clone();

        // Channel for ZMQ thread to send received messages to async processor
        let (msg_tx, mut msg_rx) = mpsc::channel::<ZmqMessage>(128);
        // Channel for async processor to signal shutdown
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        // Spawn dedicated thread for ZMQ socket operations
        let zmq_thread = std::thread::spawn(move || {
            let ctx = Context::new();
            let socket = match ctx.socket(zmq::REP) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to create ZMQ socket: {}", e);
                    return;
                }
            };

            if let Err(e) = socket.bind(&endpoint) {
                tracing::error!("Failed to bind ZMQ socket to {}: {}", endpoint, e);
                return;
            }

            tracing::info!("BusAdapter ZMQ thread listening on {}", endpoint);

            loop {
                // Check for shutdown signal (non-blocking)
                match shutdown_rx.try_recv() {
                    Ok(_) => {
                        tracing::info!("ZMQ thread received shutdown signal");
                        break;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        tracing::info!("ZMQ thread shutdown channel disconnected");
                        break;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // No shutdown signal, continue
                    }
                }

                // Receive message with timeout to allow periodic shutdown checks
                socket.set_rcvtimeo(100).expect("set receive timeout");
                
                let data = match socket.recv_bytes(0) {
                    Ok(d) => d,
                    Err(zmq::Error::EAGAIN) => {
                        // Timeout, loop back to check shutdown
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!("ZMQ recv error: {}", e);
                        continue;
                    }
                };

                // Create oneshot channel for response
                let (response_tx, response_rx) = oneshot::channel();

                // Send message to async processor
                if let Err(_) = msg_tx.blocking_send(ZmqMessage { data, response_tx }) {
                    tracing::error!("Failed to send message to async processor, channel closed");
                    break;
                }

                // Wait for response (blocking)
                let response_bytes = match response_rx.blocking_recv() {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        tracing::error!("Response channel closed without sending data");
                        // Send error response to maintain REP socket state
                        br#"{"status":"err","payload":{"code":"INTERNAL_ERROR","message":"Internal processing error"}}"#.to_vec()
                    }
                };

                // Send response
                if let Err(e) = socket.send(&response_bytes, 0) {
                    tracing::warn!("ZMQ send error: {}", e);
                }
            }

            tracing::info!("ZMQ thread shutting down");
        });

        // Async message processing loop
        loop {
            tokio::select! {
                Some(zmq_msg) = msg_rx.recv() => {
                    // Process the message
                    let response_bytes = self.process_message(&zmq_msg.data, &gateway).await;
                    
                    // Send response back to ZMQ thread
                    if let Err(_) = zmq_msg.response_tx.send(response_bytes) {
                        tracing::warn!("Failed to send response - ZMQ thread may have panicked");
                        break;
                    }
                }
                else => {
                    // Channel closed
                    tracing::info!("Message channel closed, shutting down");
                    break;
                }
            }
        }

        // Signal shutdown to ZMQ thread
        let _ = shutdown_tx.send(()).await;
        
        // Wait for ZMQ thread to finish
        drop(msg_rx);
        if let Err(e) = zmq_thread.join() {
            tracing::error!("ZMQ thread panicked: {:?}", e);
        }

        Ok(())
    }

    /// Process a single message and return the response bytes
    async fn process_message(&self, data: &[u8], gateway: &GatewayService) -> Vec<u8> {
        // Deserialize
        let response: GatewayResponse = match decode_gateway_request(data) {
            Err(e) => {
                tracing::warn!("Failed to deserialize request: {}", e);
                GatewayResponse::Err(ErrorPayload {
                    correlation_id: None,
                    code: "DESERIALIZE_ERROR".into(),
                    message: e,
                })
            }
            Ok((req, _)) => {
                self.dispatch(req, gateway).await
            }
        };

        // Serialize response
        encode_gateway_response(&response).unwrap_or_else(|e| {
            format!(r#"{{"status":"err","payload":{{"code":"SERIALIZE_ERROR","message":"{}"}}}}"#, e)
                .into_bytes()
        })
    }

    /// Bind the REP socket, process exactly one request, then return.
    /// Useful for integration tests that need deterministic teardown.
    pub async fn listen_once(&self) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.endpoint.clone();
        let gateway = self.gateway.clone();

        // Spawn dedicated thread for single ZMQ operation
        let (result_tx, result_rx) = oneshot::channel();

        std::thread::spawn(move || {
            let ctx = Context::new();
            let socket = match ctx.socket(zmq::REP) {
                Ok(s) => s,
                Err(e) => {
                    let _ = result_tx.send(Err(format!("Socket creation failed: {}", e)));
                    return;
                }
            };

            if let Err(e) = socket.bind(&endpoint) {
                let _ = result_tx.send(Err(format!("Bind failed: {}", e)));
                return;
            }

            // Receive message
            let data = match socket.recv_bytes(0) {
                Ok(d) => d,
                Err(e) => {
                    let _ = result_tx.send(Err(format!("Recv failed: {}", e)));
                    return;
                }
            };

            let _ = result_tx.send(Ok((data, socket)));
        });

        // Wait for message reception
        let (data, socket) = match result_rx.await {
            Ok(Ok((d, s))) => (d, s),
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Err("Channel closed".into()),
        };

        // Process in async context
        let response = match decode_gateway_request(&data) {
            Err(e) => GatewayResponse::Err(ErrorPayload {
                correlation_id: None,
                code: "DESERIALIZE_ERROR".into(),
                message: e,
            }),
            Ok((req, _)) => self.dispatch(req, &gateway).await,
        };

        let reply_bytes = encode_gateway_response(&response).unwrap_or_else(|e| {
            format!(r#"{{"status":"err","payload":{{"code":"SERIALIZE_ERROR","message":"{}"}}}}"#, e)
                .into_bytes()
        });

        // Send response in blocking thread
        let (send_result_tx, send_result_rx) = oneshot::channel();
        std::thread::spawn(move || {
            let result = match socket.send(&reply_bytes, 0) {
                Ok(()) => Ok(()),
                Err(e) => Err(format!("Send failed: {}", e)),
            };
            let _ = send_result_tx.send(result);
        });

        match send_result_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err("Send channel closed".into()),
        }
    }

    /// Route a deserialized request to the correct GatewayService method.
    async fn dispatch(&self, req: GatewayRequest, gateway: &GatewayService) -> GatewayResponse {
        // Get back-pressure info before processing (will be None if not near limit)
        let back_pressure = self.build_back_pressure_info();

        match req {
            GatewayRequest::SubmitOrder(cmd) => {
                // Use explicit correlation_id if provided, fall back to client_order_id
                let correlation_id = cmd.correlation_id.clone().or_else(|| cmd.client_order_id.clone());
                match gateway.submit_order(cmd).await {
                    Ok(exec_id) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: exec_id.0,
                        back_pressure,
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "SUBMIT_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::CancelOrder(cmd) => {
                // Use explicit correlation_id if provided, fall back to execution_id
                let correlation_id = cmd.correlation_id.clone().or_else(|| Some(cmd.execution_id.0.clone()));
                match gateway.cancel_order(cmd).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "cancelled".into(),
                        back_pressure,
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "CANCEL_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::ReplaceOrder(cmd) => {
                // Use explicit correlation_id if provided, fall back to execution_id
                let correlation_id = cmd.correlation_id.clone().or_else(|| Some(cmd.execution_id.0.clone()));
                match gateway.replace_order(cmd).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "replaced".into(),
                        back_pressure,
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "REPLACE_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::QueryStatus(query) => {
                // Use explicit correlation_id if provided, fall back to execution_id
                let correlation_id = query.correlation_id.clone().or_else(|| Some(query.execution_id.0.clone()));
                match gateway.query_status(query).await {
                    Ok(status_response) => {
                        // Serialize the order status response as JSON
                        let result_json = serde_json::to_string(&status_response)
                            .unwrap_or_else(|_| "{\"error\":\"serialization_failed\"}".to_string());
                        GatewayResponse::Ok(ResponsePayload {
                            correlation_id,
                            result: result_json,
                            back_pressure,
                        })
                    }
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "QUERY_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::Subscribe(sub) => {
                // Use explicit correlation_id if provided, fall back to symbol
                let correlation_id = sub.correlation_id.clone().or_else(|| Some(sub.symbol.clone()));
                match gateway.subscribe(sub).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "subscribed".into(),
                        back_pressure,
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "SUBSCRIBE_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::Unsubscribe(sub) => {
                // Use explicit correlation_id if provided, fall back to symbol
                let correlation_id = sub.correlation_id.clone().or_else(|| Some(sub.symbol.clone()));
                match gateway.unsubscribe(sub).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "unsubscribed".into(),
                        back_pressure,
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "UNSUBSCRIBE_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::market_data::MarketSubscription;
    use crate::adapters::broker::mock_adapter::MockAdapter;
    use crate::adapters::messaging::wire_codec::{encode_gateway_request, decode_gateway_response};
    use crate::core::application::{
        connection_manager::ConnectionManager,
        gateway_service::GatewayService,
        idempotency::IdempotencyStore,
        kill_switch::KillSwitch,
        observability_service::ObservabilityService,
        order_submission_service::OrderSubmissionService,
        rate_limiter::RateLimiterManager,
        risk_management_service::RiskManagementService,
        validator::RequestValidator,
    };
    use crate::core::patterns::circuit_breaker::CircuitBreaker;
    use crate::core::ports::{
        journal_repo::IJournalRepo,
        observability::IObservability,
        service_traits::{IObservabilityService, IOrderSubmissionService, IRiskManagementService},
    };
    use std::net::TcpListener;
    use std::time::Duration;

    struct NoopObservability;
    impl IObservability for NoopObservability {
        fn emit(&self, _event: String) {}
    }

    struct MockJournalRepo;
    impl IJournalRepo for MockJournalRepo {
        fn persist_outbound(&self, _record: crate::core::domain::journal::RequestRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
            Ok(())
        }
        fn persist_inbound(&self, _record: crate::core::domain::journal::ResponseRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
            Ok(())
        }
        fn replay(&self, _query: String) -> Vec<crate::core::domain::journal::ResponseRecord> {
            Vec::new()
        }
    }

    fn noop_obs() -> Arc<dyn IObservability + Send + Sync> {
        Arc::new(NoopObservability)
    }

    fn free_tcp_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral tcp port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        port
    }

    fn make_test_gateway() -> Arc<GatewayService> {
        let (stream_tx, _stream_rx) = tokio::sync::mpsc::channel::<crate::core::domain::market_data::MarketDataCommand>(32);

        use crate::adapters::messaging::order_lifecycle_publisher::OrderLifecyclePublisher;
        let (lifecycle_tx, _lifecycle_rx) = tokio::sync::mpsc::channel(128);
        let order_lifecycle_publisher = OrderLifecyclePublisher::from_sender(lifecycle_tx);

        let order_submission = Arc::new(OrderSubmissionService::new(
            RequestValidator,
            IdempotencyStore::default(),
            Box::new(MockAdapter::default()),
            Arc::new(KillSwitch::default()),
            Arc::new(CircuitBreaker::new("test", 3, 30, noop_obs())),
            "test",
        )) as Arc<dyn IOrderSubmissionService>;

        let risk_management = Arc::new(RiskManagementService::new(
            KillSwitch::default(),
            Arc::new(RateLimiterManager::new(200.0)),
        )) as Arc<dyn IRiskManagementService>;

        let observability = Arc::new(ObservabilityService::new(
            crate::core::patterns::telemetry_decorator::TelemetryDecorator::new(),
            noop_obs(),
            Arc::new(MockJournalRepo),
        )) as Arc<dyn IObservabilityService>;

        Arc::new(GatewayService::new(
            order_submission,
            risk_management,
            observability,
            ConnectionManager::new(5, 500),
            stream_tx,
            order_lifecycle_publisher,
            "test",
        ))
    }

    // Note: The following integration tests are commented out because they rely on
    // ZMQ socket communication which can be flaky in test environments.
    // The ZMQ REP socket state machine test below validates the core functionality.
    // For full integration testing, use the tests/bus_adapter_msgpack_integration.rs test.
    
    /*
    #[tokio::test]
    #[ignore = "Requires ZMQ socket - run manually or use integration tests"]
    async fn bus_adapter_listen_once_processes_single_request() {
        // This test requires proper ZMQ setup. See integration tests for full test.
    }

    #[tokio::test] 
    #[ignore = "Requires ZMQ socket - run manually or use integration tests"]
    async fn bus_adapter_handles_multiple_sequential_requests() {
        // This test requires proper ZMQ setup. See integration tests for full test.
    }
    */

    #[test]
    fn test_zmq_rep_socket_state_machine() {
        // This test verifies that ZMQ REP sockets properly enforce recv->send alternation
        let ctx = zmq::Context::new();
        let rep_socket = ctx.socket(zmq::REP).expect("create REP socket");
        let req_socket = ctx.socket(zmq::REQ).expect("create REQ socket");

        // Bind REP socket
        rep_socket.bind("inproc://test_state_machine").expect("bind");
        
        // Connect REQ socket
        req_socket.connect("inproc://test_state_machine").expect("connect");

        // REQ socket sends first
        req_socket.send("hello", 0).expect("req send");

        // REP socket must recv first
        let msg = rep_socket.recv_bytes(0).expect("rep recv");
        assert_eq!(msg, b"hello");

        // REP socket sends response
        rep_socket.send("world", 0).expect("rep send");

        // REQ socket receives response
        let response = req_socket.recv_bytes(0).expect("req recv");
        assert_eq!(response, b"world");

        // Now the cycle can repeat
        req_socket.send("second", 0).expect("req send 2");
        let msg2 = rep_socket.recv_bytes(0).expect("rep recv 2");
        assert_eq!(msg2, b"second");
    }

    // =========================================================================
    // SYNC/ASYNC CRITICAL PATH TESTS
    // =========================================================================
    
    use crate::core::domain::order::{OrderSide, OrderType, TimeInForce, OrderCmd, CancelCmd, ExecutionId, ReplaceCmd, StatusQuery};
    use crate::adapters::messaging::priority_bus_adapter::{CommandPriority, BusAdapterConfig};

    #[test]
    fn test_submit_order_is_critical_priority() {
        let req = GatewayRequest::SubmitOrder(OrderCmd {
            symbol: "AAPL".to_string(),
            qty: 100,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
            limit_price: None,
            stop_price: None,
            client_order_id: Some("test-123".to_string()),
            extended_hours: false,
            notional: None,
            correlation_id: Some("corr-test-001".to_string()),
        });
        assert_eq!(CommandPriority::for_request(&req), CommandPriority::Critical);
        assert!(CommandPriority::for_request(&req).is_synchronous());
    }

    #[test]
    fn test_cancel_order_is_critical_priority() {
        let req = GatewayRequest::CancelOrder(CancelCmd {
            execution_id: ExecutionId("exec-123".to_string()),
            symbol: "AAPL".to_string(),
            correlation_id: Some("corr-cancel-001".to_string()),
        });
        assert_eq!(CommandPriority::for_request(&req), CommandPriority::Critical);
        assert!(CommandPriority::for_request(&req).is_synchronous());
    }

    #[test]
    fn test_replace_order_is_critical_priority() {
        let req = GatewayRequest::ReplaceOrder(ReplaceCmd {
            execution_id: ExecutionId("exec-123".to_string()),
            symbol: "AAPL".to_string(),
            side: OrderSide::Buy,
            qty: Some(200),
            limit_price: Some(150.0),
            correlation_id: Some("corr-replace-001".to_string()),
        });
        assert_eq!(CommandPriority::for_request(&req), CommandPriority::Critical);
        assert!(CommandPriority::for_request(&req).is_synchronous());
    }

    #[test]
    fn test_query_status_is_normal_priority() {
        let req = GatewayRequest::QueryStatus(StatusQuery {
            execution_id: ExecutionId("exec-123".to_string()),
            correlation_id: Some("corr-query-001".to_string()),
        });
        assert_eq!(CommandPriority::for_request(&req), CommandPriority::Normal);
        assert!(!CommandPriority::for_request(&req).is_synchronous());
    }

    #[test]
    fn test_subscribe_is_low_priority() {
        let req = GatewayRequest::Subscribe(MarketSubscription {
            symbol: "AAPL".to_string(),
            correlation_id: Some("corr-sub-001".to_string()),
        });
        assert_eq!(CommandPriority::for_request(&req), CommandPriority::Low);
        assert!(!CommandPriority::for_request(&req).is_synchronous());
    }

    #[test]
    fn test_unsubscribe_is_low_priority() {
        let req = GatewayRequest::Unsubscribe(MarketSubscription {
            symbol: "AAPL".to_string(),
            correlation_id: Some("corr-unsub-001".to_string()),
        });
        assert_eq!(CommandPriority::for_request(&req), CommandPriority::Low);
        assert!(!CommandPriority::for_request(&req).is_synchronous());
    }

    #[test]
    fn test_priority_ordering() {
        // Lower value = higher priority
        assert!(CommandPriority::Critical < CommandPriority::Normal);
        assert!(CommandPriority::Normal < CommandPriority::Low);
        assert!(CommandPriority::Critical < CommandPriority::Low);
    }

    #[test]
    fn test_bus_adapter_config_default() {
        let config = BusAdapterConfig::default();
        assert!(config.enable_sync_critical_path);
        assert_eq!(config.sync_timeout_ms, 500);
        assert_eq!(config.max_concurrent_async, 10);
        assert_eq!(config.critical_channel_capacity, 128);
    }

    #[test]
    fn test_bus_adapter_config_hft() {
        let config = BusAdapterConfig::hft();
        assert!(config.enable_sync_critical_path);
        assert_eq!(config.sync_timeout_ms, 100);
        assert_eq!(config.max_concurrent_async, 10);
    }

    #[test]
    fn test_bus_adapter_config_without_sync_path() {
        let config = BusAdapterConfig::default().without_sync_path();
        assert!(!config.enable_sync_critical_path);
    }

    #[test]
    fn test_execution_commands_all_critical() {
        // Verify all execution-related commands are critical
        let submit = GatewayRequest::SubmitOrder(OrderCmd {
            symbol: "TEST".to_string(),
            qty: 1,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
            limit_price: None,
            stop_price: None,
            client_order_id: None,
            extended_hours: false,
            notional: None,
            correlation_id: Some("corr-test-002".to_string()),
        });
        let cancel = GatewayRequest::CancelOrder(CancelCmd {
            execution_id: ExecutionId("test".to_string()),
            symbol: "TEST".to_string(),
            correlation_id: Some("corr-test-003".to_string()),
        });
        let replace = GatewayRequest::ReplaceOrder(ReplaceCmd {
            execution_id: ExecutionId("test".to_string()),
            symbol: "TEST".to_string(),
            side: OrderSide::Sell,
            qty: None,
            limit_price: None,
            correlation_id: Some("corr-test-004".to_string()),
        });

        assert!(CommandPriority::for_request(&submit).is_synchronous());
        assert!(CommandPriority::for_request(&cancel).is_synchronous());
        assert!(CommandPriority::for_request(&replace).is_synchronous());
    }

    #[test]
    fn test_non_execution_commands_not_synchronous() {
        // Verify non-execution commands are NOT synchronous
        let query = GatewayRequest::QueryStatus(StatusQuery {
            execution_id: ExecutionId("test".to_string()),
            correlation_id: Some("corr-test-005".to_string()),
        });
        let subscribe = GatewayRequest::Subscribe(MarketSubscription {
            symbol: "TEST".to_string(),
            correlation_id: Some("corr-test-006".to_string()),
        });
        let unsubscribe = GatewayRequest::Unsubscribe(MarketSubscription {
            symbol: "TEST".to_string(),
            correlation_id: Some("corr-test-007".to_string()),
        });

        assert!(!CommandPriority::for_request(&query).is_synchronous());
        assert!(!CommandPriority::for_request(&subscribe).is_synchronous());
        assert!(!CommandPriority::for_request(&unsubscribe).is_synchronous());
    }

    // =========================================================================
    // BACK-PRESSURE TESTS
    // =========================================================================

    #[test]
    fn test_bus_adapter_includes_back_pressure_when_near_limit() {
        let gateway = make_test_gateway();
        let rate_limiter = Arc::new(RateLimiterManager::new(10.0));
        let adapter = BusAdapter::new(
            "tcp://127.0.0.1:0",
            gateway,
            rate_limiter.clone(),
            "test-broker",
        ).with_near_limit_threshold(30.0);

        // Consume 8 tokens (80% used, 20% remaining - below 30% threshold)
        for _ in 0..8 {
            rate_limiter.allow("test-broker", 1);
        }

        let back_pressure = adapter.build_back_pressure_info();
        assert!(back_pressure.is_some(), "should have back-pressure when near limit");
        
        let info = back_pressure.unwrap();
        assert_eq!(info.broker_id, "test-broker");
        assert!(info.is_near_limit);
        assert!(info.percent_remaining <= 30.0);
    }

    #[test]
    fn test_bus_adapter_no_back_pressure_when_tokens_abundant() {
        let gateway = make_test_gateway();
        let rate_limiter = Arc::new(RateLimiterManager::new(100.0));
        let adapter = BusAdapter::new(
            "tcp://127.0.0.1:0",
            gateway,
            rate_limiter.clone(),
            "test-broker",
        );

        // Bucket is full (100 tokens), should not trigger back-pressure
        let back_pressure = adapter.build_back_pressure_info();
        assert!(back_pressure.is_none(), "should not have back-pressure when tokens abundant");
    }

    #[test]
    fn test_back_pressure_recommendation_pause_at_critical() {
        let gateway = make_test_gateway();
        let rate_limiter = Arc::new(RateLimiterManager::new(100.0));
        let adapter = BusAdapter::new(
            "tcp://127.0.0.1:0",
            gateway,
            rate_limiter.clone(),
            "test-broker",
        ).with_near_limit_threshold(20.0);

        // Consume 96 tokens (only 4% remaining - critical)
        for _ in 0..96 {
            rate_limiter.allow("test-broker", 1);
        }

        let back_pressure = adapter.build_back_pressure_info();
        assert!(back_pressure.is_some());
        
        let info = back_pressure.unwrap();
        assert_eq!(info.recommendation, BackPressureRecommendation::Pause);
    }

    #[test]
    fn test_back_pressure_recommendation_slow_down_at_moderate() {
        let gateway = make_test_gateway();
        let rate_limiter = Arc::new(RateLimiterManager::new(100.0));
        let adapter = BusAdapter::new(
            "tcp://127.0.0.1:0",
            gateway,
            rate_limiter.clone(),
            "test-broker",
        ).with_near_limit_threshold(30.0);

        // Consume 80 tokens (20% remaining - moderate, below 30% threshold)
        for _ in 0..80 {
            rate_limiter.allow("test-broker", 1);
        }

        let back_pressure = adapter.build_back_pressure_info();
        assert!(back_pressure.is_some());
        
        let info = back_pressure.unwrap();
        assert_eq!(info.recommendation, BackPressureRecommendation::SlowDown);
    }

    #[test]
    fn test_near_limit_threshold_clamping() {
        let gateway = make_test_gateway();
        let rate_limiter = Arc::new(RateLimiterManager::new(100.0));
        
        let adapter1 = BusAdapter::new(
            "tcp://127.0.0.1:0",
            gateway.clone(),
            rate_limiter.clone(),
            "test",
        ).with_near_limit_threshold(150.0); // Above 100
        
        // Should be clamped to 100
        let back_pressure1 = adapter1.build_back_pressure_info();
        // Full bucket, so no back-pressure
        assert!(back_pressure1.is_none());

        let adapter2 = BusAdapter::new(
            "tcp://127.0.0.1:0",
            gateway.clone(),
            rate_limiter.clone(),
            "test",
        ).with_near_limit_threshold(-10.0); // Below 0
        
        // Should be clamped to 0
        // Full bucket at 100% > 0%, so no back-pressure
        let back_pressure2 = adapter2.build_back_pressure_info();
        assert!(back_pressure2.is_none());
    }
}
