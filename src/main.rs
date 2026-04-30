// main.rs

mod adapters;
mod core;
mod config;
mod infra;
mod app_tracing;

use std::sync::Arc;

use apca::Client;
use apca::ApiInfo;

use crate::adapters::broker::adapter_factory::AdapterFactory;
use crate::adapters::messaging::bus_adapter::BusAdapter;
use crate::adapters::metrics::metrics_adapter::MetricsAdapter;
use crate::adapters::persistence::journal_storage::JournalStorage;

use crate::core::domain::broker_config::BrokerConfig;

use crate::core::application::connection_manager::ConnectionManager;
use crate::core::application::gateway_service::GatewayService;
use crate::core::application::idempotency::IdempotencyStore;
use crate::core::application::kill_switch::KillSwitch;
use crate::core::application::observability_service::ObservabilityService;
use crate::core::application::order_submission_service::OrderSubmissionService;
use crate::core::application::rate_limiter::RateLimiterManager;
use crate::core::application::risk_management_service::RiskManagementService;
use crate::core::application::validator::RequestValidator;

use crate::core::patterns::circuit_breaker::CircuitBreaker;
use crate::core::patterns::telemetry_decorator::TelemetryDecorator;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    // ── Tracing ───────────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("broker_gateway_service=info".parse().unwrap()),
        )
        .init();

    // ── Shared infrastructure ─────────────────────────────────────────────────
    let metrics = Arc::new(MetricsAdapter);
    let journal = Arc::new(JournalStorage::new());
    let kill_switch = Arc::new(KillSwitch::default());
    let rate_limiter = Arc::new(RateLimiterManager::default()); // 200 req/min

    // ── Circuit breaker (per broker) ──────────────────────────────────────────
    // threshold=3 failures, cooldown=30s
    let circuit_breaker = Arc::new(CircuitBreaker::new(
        "alpaca",
        3,
        30,
        Arc::clone(&metrics) as Arc<dyn crate::core::ports::observability::IObservability + Send + Sync>,
    ));

    // ── Alpaca client ─────────────────────────────────────────────────────────
    let api_info = ApiInfo::from_env()
        .expect("missing env vars: APCA_API_KEY_ID, APCA_API_SECRET_KEY, APCA_API_BASE_URL");
    let client = Client::new(api_info);

    // ── Broker adapter ────────────────────────────────────────────────────────
    let factory = AdapterFactory::new(Some(client));
    let config = BrokerConfig { name: "alpaca".into() };
    let adapter = factory.create_adapter(config);

    // ── Application services ──────────────────────────────────────────────────
    let order_submission = OrderSubmissionService::new(
        RequestValidator,
        IdempotencyStore::default(),
        adapter,
        Arc::clone(&kill_switch),
        Arc::clone(&rate_limiter),
        Arc::clone(&circuit_breaker),
        "alpaca",
    );

    let risk_management = RiskManagementService::new(
        (*kill_switch).clone(),
        RateLimiterManager::default(),
    );

    let observability = ObservabilityService::new(
        TelemetryDecorator,
        Arc::clone(&metrics) as Arc<dyn crate::core::ports::observability::IObservability + Send + Sync>,
        Arc::clone(&journal) as Arc<dyn crate::core::ports::journal_repo::IJournalRepo + Send + Sync>,
    );

    let gateway = Arc::new(GatewayService::new(
        order_submission,
        risk_management,
        observability,
        ConnectionManager::default(),
    ));

    // ── ZeroMQ REP listener ───────────────────────────────────────────────────
    let endpoint = std::env::var("GATEWAY_ZMQ_ENDPOINT")
        .unwrap_or_else(|_| "tcp://127.0.0.1:5555".into());

    let bus = BusAdapter::new(endpoint, Arc::clone(&gateway));

    tracing::info!("Broker Gateway Service starting");

    if let Err(e) = bus.listen().await {
        tracing::error!("BusAdapter error: {}", e);
        std::process::exit(1);
    }
}