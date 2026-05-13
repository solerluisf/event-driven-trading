use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use broker_gateway_service::adapters::broker::mock_adapter::MockAdapter;
use broker_gateway_service::adapters::messaging::bus_adapter::BusAdapter;
use broker_gateway_service::adapters::messaging::wire_codec::{
    decode_gateway_response, encode_gateway_request,
};
use broker_gateway_service::core::application::connection_manager::ConnectionManager;
use broker_gateway_service::core::application::gateway_service::GatewayService;
use broker_gateway_service::core::application::idempotency::IdempotencyStore;
use broker_gateway_service::core::application::kill_switch::KillSwitch;
use broker_gateway_service::core::application::observability_service::ObservabilityService;
use broker_gateway_service::core::application::order_submission_service::OrderSubmissionService;
use broker_gateway_service::core::application::rate_limiter::RateLimiterManager;
use broker_gateway_service::core::application::risk_management_service::RiskManagementService;
use broker_gateway_service::core::application::validator::RequestValidator;
use broker_gateway_service::core::domain::market_data::{MarketDataCommand, MarketSubscription};
use broker_gateway_service::core::domain::wire_message::{GatewayRequest, GatewayResponse};
use broker_gateway_service::core::patterns::circuit_breaker::CircuitBreaker;
use broker_gateway_service::core::ports::journal_repo::IJournalRepo;
use broker_gateway_service::core::ports::observability::IObservability;
use broker_gateway_service::core::ports::service_traits::{
    IObservabilityService, IOrderSubmissionService, IRiskManagementService,
};
use broker_gateway_service::adapters::messaging::order_lifecycle_publisher::OrderLifecyclePublisher;
use tokio::sync::mpsc;

struct NoopObservability;
impl IObservability for NoopObservability {
    fn emit(&self, _event: String) {}
}

struct MockJournalRepo;
impl IJournalRepo for MockJournalRepo {
    fn persist_outbound(&self, _record: broker_gateway_service::core::domain::journal::RequestRecord) -> broker_gateway_service::core::ports::journal_repo::JournalResult<()> {
        Ok(())
    }
    fn persist_inbound(&self, _record: broker_gateway_service::core::domain::journal::ResponseRecord) -> broker_gateway_service::core::ports::journal_repo::JournalResult<()> {
        Ok(())
    }
    fn replay(&self, _query: String) -> Vec<broker_gateway_service::core::domain::journal::ResponseRecord> {
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

fn make_gateway_service() -> (GatewayService, Arc<RateLimiterManager>) {
    let (stream_tx, mut stream_rx) = mpsc::channel::<MarketDataCommand>(32);
    tokio::spawn(async move {
        while stream_rx.recv().await.is_some() {}
    });

    // Create a test order lifecycle publisher
    let (lifecycle_tx, _lifecycle_rx) = mpsc::channel(128);
    let order_lifecycle_publisher = OrderLifecyclePublisher::from_sender(lifecycle_tx);

    // Create shared rate limiter for risk management
    let rate_limiter = Arc::new(RateLimiterManager::new(200.0));

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
        Arc::clone(&rate_limiter),
    )) as Arc<dyn IRiskManagementService>;

    let observability = Arc::new(ObservabilityService::new(
        broker_gateway_service::core::patterns::telemetry_decorator::TelemetryDecorator::new(),
        noop_obs(),
        Arc::new(MockJournalRepo),
    )) as Arc<dyn IObservabilityService>;

    let gateway = GatewayService::new(
        order_submission,
        risk_management,
        observability,
        ConnectionManager::new(5, 500),
        stream_tx,
        order_lifecycle_publisher,
        "test",
    );

    (gateway, rate_limiter)
}

#[tokio::test]
async fn req_rep_round_trip_uses_msgpack_wire_codec() {
    let endpoint = format!("tcp://127.0.0.1:{}", free_tcp_port());
    let (gateway, rate_limiter) = make_gateway_service();
    let gateway = Arc::new(gateway);
    let bus = BusAdapter::new(endpoint.clone(), gateway, rate_limiter, "test");

    let bus_task = tokio::spawn(async move {
        let _ = bus.listen_once().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoint_for_client = endpoint.clone();
    let response_bytes = tokio::task::spawn_blocking(move || {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::REQ).expect("create REQ socket");
        socket.set_rcvtimeo(2000).expect("set recv timeout");
        socket.set_sndtimeo(2000).expect("set send timeout");
        socket
            .connect(&endpoint_for_client)
            .expect("connect REQ socket");

        let request = GatewayRequest::Subscribe(MarketSubscription {
            symbol: "TSLA".to_string(),
            correlation_id: None,
        });
        let request_bytes = encode_gateway_request(&request).expect("encode msgpack request");
        socket.send(request_bytes, 0).expect("send request");
        socket.recv_bytes(0).expect("recv response")
    })
    .await
    .expect("client task join");

    let (response, _) = decode_gateway_response(&response_bytes).expect("decode response");
    let _ = bus_task.await;

    assert!(matches!(
        response,
        GatewayResponse::Ok(payload) if payload.result == "subscribed" && payload.back_pressure.is_none()
    ), "Expected successful response without back-pressure when tokens are abundant");
}
