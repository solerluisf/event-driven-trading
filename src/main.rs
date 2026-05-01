// main.rs

mod adapters;
mod core;
mod config;
mod infra;
mod app_tracing; // renamed from 'tracing' to avoid clash with the tracing crate

use std::sync::Arc;
use tracing::{info, error};

use apca::Client;
use apca::ApiInfo;

use crate::config::app_config::AppConfig;

use crate::adapters::broker::adapter_factory::AdapterFactory;
use crate::adapters::broker::alpaca_stream::{AlpacaStreamConfig, self as alpaca_stream};
use crate::adapters::messaging::bus_adapter::BusAdapter;
use crate::adapters::messaging::market_data_publisher::MarketDataPublisher;
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
use crate::core::application::service_impls; // registers trait impls

use crate::core::patterns::circuit_breaker::CircuitBreaker;
use crate::core::patterns::telemetry_decorator::TelemetryDecorator;

use crate::core::ports::observability::IObservability;
use crate::core::ports::journal_repo::IJournalRepo;
use crate::core::ports::service_traits::{
    IOrderSubmissionService,
    IRiskManagementService,
    IObservabilityService,
};

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

    // ── Config ────────────────────────────────────────────────────────────────
    let cfg = AppConfig::from_env();
    info!(
        "config loaded: broker={} rep={} pub={} feed={} symbols={:?}",
        cfg.broker,
        cfg.zmq_rep_endpoint,
        cfg.zmq_pub_endpoint,
        cfg.market_data_feed,
        cfg.market_data_symbols,
    );

    // ── Shared infrastructure ─────────────────────────────────────────────────
    let metrics = Arc::new(MetricsAdapter);
    let journal = Arc::new(JournalStorage::new());
    let kill_switch = Arc::new(KillSwitch::default());
    let rate_limiter = Arc::new(RateLimiterManager::new(cfg.rate_limit_rpm));

    // ── Circuit breaker ───────────────────────────────────────────────────────
    let circuit_breaker = Arc::new(CircuitBreaker::new(
        &cfg.broker,
        cfg.cb_failure_threshold,
        cfg.cb_cooldown_secs,
        Arc::clone(&metrics) as Arc<dyn IObservability + Send + Sync>,
    ));

    // ── Broker adapter ────────────────────────────────────────────────────────
    let api_info = ApiInfo::from_env()
        .expect("missing APCA_API_KEY_ID / APCA_API_SECRET_KEY / APCA_API_BASE_URL");
    let client = Client::new(api_info);

    let factory = AdapterFactory::new(Some(client));
    let adapter = factory.create_adapter(BrokerConfig { name: cfg.broker.clone() });

    // ── Application services ──────────────────────────────────────────────────
    let order_submission = Arc::new(OrderSubmissionService::new(
        RequestValidator,
        IdempotencyStore::default(),
        adapter,
        Arc::clone(&kill_switch),
        Arc::clone(&rate_limiter),
        Arc::clone(&circuit_breaker),
        &cfg.broker,
    )) as Arc<dyn IOrderSubmissionService>;

    let risk_management = Arc::new(RiskManagementService::new(
        (*kill_switch).clone(),
        RateLimiterManager::new(cfg.rate_limit_rpm),
    )) as Arc<dyn IRiskManagementService>;

    let observability = Arc::new(ObservabilityService::new(
        TelemetryDecorator,
        Arc::clone(&metrics) as Arc<dyn IObservability + Send + Sync>,
        Arc::clone(&journal) as Arc<dyn IJournalRepo + Send + Sync>,
    )) as Arc<dyn IObservabilityService>;

    // ── Connection managers ───────────────────────────────────────────────────
    // GatewayService takes ownership of one instance (its existing API).
    // The stream gets its own Arc<ConnectionManager> for reconnect_with_backoff.
    let stream_connection_manager = Arc::new(ConnectionManager::new(
        cfg.reconnect_max_attempts,
        cfg.reconnect_base_ms,
    ));

    // ── Gateway ───────────────────────────────────────────────────────────────
    let gateway = Arc::new(GatewayService::new(
        order_submission,
        risk_management,
        observability,
        ConnectionManager::new(cfg.reconnect_max_attempts, cfg.reconnect_base_ms),
    ));

    // ── Market data PUB socket (actor) ────────────────────────────────────────
    let (publisher, publisher_handle) = MarketDataPublisher::spawn(&cfg.zmq_pub_endpoint);

    // ── Alpaca market data stream ─────────────────────────────────────────────
    let stream_config = AlpacaStreamConfig::from_env(
        cfg.market_data_feed.clone(),
        cfg.market_data_symbols.clone(),
    );
    let stream_handle = alpaca_stream::spawn(
        stream_config,
        publisher.clone(),
        Arc::clone(&stream_connection_manager),
    );

    // ── ZeroMQ REP listener ───────────────────────────────────────────────────
    let bus = BusAdapter::new(&cfg.zmq_rep_endpoint, Arc::clone(&gateway));

    info!("Broker Gateway Service started");

    // Run all three concurrently; stop if any fails
    tokio::select! {
        result = bus.listen() => {
            if let Err(e) = result {
                error!("BusAdapter error: {}", e);
                std::process::exit(1);
            }
        }
        result = publisher_handle => {
            if let Err(e) = result {
                error!("Publisher actor error: {}", e);
                std::process::exit(1);
            }
        }
        _ = stream_handle => {
            error!("Market data stream task exited unexpectedly");
            std::process::exit(1);
        }
    }
}