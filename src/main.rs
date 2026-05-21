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
use crate::adapters::messaging::market_data_publisher::{MarketDataEvent, MarketDataPublisher};
use crate::adapters::messaging::order_lifecycle_publisher::OrderLifecyclePublisher;
use crate::adapters::messaging::wire_codec::wire_codec_metrics_snapshot;
use crate::adapters::messaging::heartbeat_publisher::HeartbeatPublisher;
use crate::adapters::messaging::kill_switch_subscriber::KillSwitchSubscriber;
use crate::adapters::messaging::mode_subscriber::ModeSubscriber;
use crate::adapters::messaging::circuit_breaker_publisher::CircuitBreakerPublisher;
use crate::adapters::messaging::orchestration_handler::OrchestrationHandler;
use crate::adapters::metrics::metrics_adapter::MetricsAdapter;
use crate::adapters::persistence::journal_storage::JournalStorage;
use crate::core::application::event_reactor::EventReactor;
use crate::core::domain::market_data::MarketDataCommand;
use tokio::sync::mpsc;

use crate::core::domain::broker_config::BrokerConfig;
use crate::core::domain::operation_mode::OperationMode;

use crate::core::application::connection_manager::ConnectionManager;
use crate::core::application::gateway_service::GatewayService;
use crate::core::application::idempotency::IdempotencyStore;
use crate::core::application::kill_switch::KillSwitch;
use crate::core::application::observability_service::ObservabilityService;
use crate::core::application::order_submission_service::OrderSubmissionService;
use crate::core::application::rate_limiter::RateLimiterManager;
use crate::core::application::risk_management_service::RiskManagementService;
use crate::core::application::validator::RequestValidator;
 // registers trait impls

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

    // Validate configuration
    if let Err(e) = cfg.validate() {
        error!("Configuration validation failed: {}", e);
        std::process::exit(1);
    }

    info!(
        "config loaded: broker={} rep={} pub={} feed={} symbols={:?}",
        cfg.broker,
        cfg.zmq_rep_endpoint,
        cfg.zmq_pub_endpoint,
        cfg.market_data_feed,
        cfg.market_data_symbols,
    );
    info!(
        "operation mode: {} (workload_isolation={}, max_concurrent_bulk={}, per_workload_rate_limit={})",
        cfg.operation_mode,
        cfg.workload_config.isolate_bulk_operations,
        cfg.workload_config.max_concurrent_bulk,
        cfg.workload_config.per_workload_rate_limiting,
    );
    info!("wire codec: MessagePack only (JSON fallback removed)");

    // ── Shared infrastructure ─────────────────────────────────────────────────
    let metrics = Arc::new(MetricsAdapter::default());
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
    let adapter: Box<dyn crate::core::ports::execution_port::IExecutionPort<Error = crate::adapters::broker::broker_error::BrokerError>> = 
        if cfg.operation_mode == OperationMode::Offline {
            info!("Operation mode is OFFLINE - using mock adapter without broker connectivity");
            Box::new(crate::adapters::broker::mock_adapter::MockAdapter::default())
        } else {
            let api_info = ApiInfo::from_env()
                .expect("missing APCA_API_KEY_ID / APCA_API_SECRET_KEY / APCA_API_BASE_URL");
            let client = Client::new(api_info);

            let factory = AdapterFactory::new(Some(client));
            factory.create_adapter(BrokerConfig { name: cfg.broker.clone() })
                .expect("Failed to create broker adapter - Alpaca client may already be consumed")
        };

    // ── Application services ──────────────────────────────────────────────────
    let order_submission = Arc::new(OrderSubmissionService::new(
        RequestValidator,
        IdempotencyStore::default(),
        adapter,
        Arc::clone(&kill_switch),
        Arc::clone(&circuit_breaker),
        &cfg.broker,
    )) as Arc<dyn IOrderSubmissionService>;

    let risk_management = Arc::new(RiskManagementService::new(
        (*kill_switch).clone(),
        Arc::clone(&rate_limiter),
    )) as Arc<dyn IRiskManagementService>;

    let observability = Arc::new(ObservabilityService::new(
        TelemetryDecorator::new(),
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

    // ── Market data command channel ──────────────────────────────────────────
    let (stream_command_tx, stream_command_rx) = mpsc::channel::<MarketDataCommand>(32);

    // ── Market data PUB socket (actor) ────────────────────────────────────────
    let (publisher, publisher_handle) = MarketDataPublisher::spawn(&cfg.zmq_pub_endpoint);

    // ── Order lifecycle PUB socket (actor) ────────────────────────────────────
    let (order_lifecycle_publisher, order_lifecycle_handle) = OrderLifecyclePublisher::spawn(&cfg.zmq_order_lifecycle_endpoint);

    // ── Gateway ───────────────────────────────────────────────────────────────
    let gateway = Arc::new(GatewayService::new_with_workload_config(
        order_submission,
        risk_management,
        Arc::clone(&observability),
        ConnectionManager::new(cfg.reconnect_max_attempts, cfg.reconnect_base_ms),
        stream_command_tx.clone(),
        order_lifecycle_publisher,
        cfg.workload_config.clone(),
        &cfg.broker,
    ));

    // ── Event reactor for symbol-specific market data handling ────────────────
    let (reactor_tx, reactor_rx) = mpsc::channel::<MarketDataEvent>(128);
    let _event_reactor = EventReactor::spawn(
        reactor_rx,
        Arc::clone(&observability),
        Arc::clone(&kill_switch),
    );

    // ── Alpaca market data stream ─────────────────────────────────────────────
    // Skip market data stream in offline mode
    let stream_handle: Option<tokio::task::JoinHandle<()>> = if cfg.operation_mode == OperationMode::Offline {
        info!("Operation mode is OFFLINE - skipping market data stream");
        // Create a dummy task that does nothing
        Some(tokio::spawn(async move {
            // Keep the stream_command_rx alive but don't use it
            drop(stream_command_rx);
        }))
    } else {
        let stream_config = match AlpacaStreamConfig::from_env(
            cfg.market_data_feed.clone(),
            cfg.market_data_symbols.clone(),
        ) {
            Ok(config) => config,
            Err(e) => {
                error!("Failed to load Alpaca stream configuration: {}", e);
                std::process::exit(1);
            }
        };
        Some(alpaca_stream::spawn(
            stream_config,
            publisher.clone(),
            reactor_tx.clone(),
            Arc::clone(&stream_connection_manager),
            stream_command_rx,
            Arc::clone(&observability),
        ))
    };

    // ── Orchestrator integration ────────────────────────────────────────────────

    // Heartbeat publisher — sends GatewayHealthSnapshot every 5s
    let (heartbeat_tx, heartbeat_handle) = HeartbeatPublisher::spawn(&cfg.health_pub_endpoint);

    // Kill-switch subscriber (from Orchestrator broadcasts)
    let kill_switch_subscriber = KillSwitchSubscriber::spawn(
        &cfg.orchestrator_events_endpoint,
        Arc::clone(&kill_switch),
    );

    // Mode subscriber (from Orchestrator broadcasts)
    let mode_subscriber = ModeSubscriber::spawn(&cfg.orchestrator_events_endpoint);

    // Circuit breaker publisher
    let (_cb_tx, cb_handle) = CircuitBreakerPublisher::spawn(&cfg.circuit_breaker_pub_endpoint);

    // Orchestration command handler
    let (_orchestration_handler, orchestration_handle) = OrchestrationHandler::spawn(
        &cfg.orchestrator_control_endpoint,
        Arc::clone(&kill_switch),
        Arc::clone(&circuit_breaker),
        Arc::clone(&rate_limiter),
        cfg.broker.clone(),
    );

    // Health heartbeat task — collects snapshot and sends to publisher
    let health_snapshot_task = {
        let kill_switch = Arc::clone(&kill_switch);
        let circuit_breaker = Arc::clone(&circuit_breaker);
        let rate_limiter = Arc::clone(&rate_limiter);
        let broker_id = cfg.broker.clone();
        let symbols = cfg.market_data_symbols.clone();
        tokio::spawn(async move {
            use crate::core::domain::gateway_health::GatewayHealthSnapshot;
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let cb_state = if circuit_breaker.is_open() {
                    "open"
                } else {
                    "closed"
                };
                let snapshot = GatewayHealthSnapshot::new("broker_gateway")
                    .with_kill_switch(kill_switch.is_enabled())
                    .with_operation_mode(cfg.operation_mode.to_string())
                    .with_circuit_breaker_state(cb_state)
                    .with_rate_limiter(
                        rate_limiter.tokens_remaining(&broker_id),
                        rate_limiter.get_capacity(&broker_id),
                    )
                    .with_symbols(symbols.clone());
                let _ = heartbeat_tx.send(snapshot).await;
            }
        })
    };

    // ── ZeroMQ REP listener ───────────────────────────────────────────────────
    let bus = BusAdapter::new(
        &cfg.zmq_rep_endpoint,
        Arc::clone(&gateway),
        Arc::clone(&rate_limiter),
        &cfg.broker,
    );

    info!("Broker Gateway Service started");
    let initial_wire_metrics = wire_codec_metrics_snapshot();
    info!(
        "wire codec counters initialized: decode_msgpack_total={} decode_error_total={} encode_error_total={}",
        initial_wire_metrics.decode_msgpack_total,
        initial_wire_metrics.decode_error_total,
        initial_wire_metrics.encode_error_total
    );

    // Run all tasks concurrently; stop if any fails
    tokio::select! {
        result = bus.listen() => {
            if let Err(e) = result {
                error!("BusAdapter error: {}", e);
                std::process::exit(1);
            }
        }
        result = publisher_handle => {
            if let Err(e) = result {
                error!("Market data Publisher actor error: {}", e);
                std::process::exit(1);
            }
        }
        result = order_lifecycle_handle => {
            if let Err(e) = result {
                error!("Order lifecycle Publisher actor error: {}", e);
                std::process::exit(1);
            }
        }
        _ = async {
            if let Some(handle) = stream_handle {
                handle.await.ok();
            }
        } => {
            error!("Market data stream task exited unexpectedly");
            std::process::exit(1);
        }
        _ = heartbeat_handle => {
            error!("Heartbeat publisher exited");
        }
        _ = kill_switch_subscriber => {
            error!("Kill switch subscriber exited");
        }
        _ = mode_subscriber => {
            error!("Mode subscriber exited");
        }
        _ = cb_handle => {
            error!("Circuit breaker publisher exited");
        }
        _ = orchestration_handle => {
            error!("Orchestration handler exited");
        }
        _ = health_snapshot_task => {
            error!("Health snapshot task exited");
        }
    }
}