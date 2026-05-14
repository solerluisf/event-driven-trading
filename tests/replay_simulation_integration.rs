// tests/replay_simulation_integration.rs
//
// Integration tests for replay/simulation mode
// Verifies that journal records can be replayed instead of live broker calls

use std::sync::Arc;
use broker_gateway_service::core::domain::broker_config::BrokerConfig;
use broker_gateway_service::core::domain::data_source::{DataSource, ExecutionConfig};
use broker_gateway_service::core::domain::journal::{RequestRecord, ResponseRecord};
use broker_gateway_service::core::domain::order::{OrderCmd, OrderSide, OrderType, TimeInForce, CancelCmd, ExecutionId};
use broker_gateway_service::core::ports::journal_repo::{IJournalRepo, JournalResult};
use broker_gateway_service::core::ports::execution_port::IExecutionPort;
use broker_gateway_service::adapters::broker::adapter_factory::AdapterFactory;
use broker_gateway_service::adapters::broker::replay_adapter::{ReplayBrokerAdapter, ReplayConfig};

struct MockJournal {
    responses: Vec<ResponseRecord>,
}

impl MockJournal {
    fn with_responses(responses: Vec<ResponseRecord>) -> Self {
        Self { responses }
    }
}

impl IJournalRepo for MockJournal {
    fn persist_outbound(&self, _record: RequestRecord) -> JournalResult<()> {
        Ok(())
    }

    fn persist_inbound(&self, _record: ResponseRecord) -> JournalResult<()> {
        Ok(())
    }

    fn replay(&self, _query: String) -> Vec<ResponseRecord> {
        self.responses.clone()
    }
}

fn create_order_cmd(symbol: &str) -> OrderCmd {
    OrderCmd {
        symbol: symbol.to_string(),
        qty: 100,
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::Day,
        limit_price: None,
        stop_price: None,
        client_order_id: Some("test-123".to_string()),
        extended_hours: false,
        notional: None,
        correlation_id: None,
    }
}

fn create_test_response(id: &str, execution_id: &str) -> ResponseRecord {
    ResponseRecord {
        id: id.to_string(),
        raw_payload: Some(format!(r#"{{"execution_id": "{}"}}"#, execution_id)),
        correlation_id: None,
    }
}

#[test]
fn test_replay_adapter_replays_journal_records() {
    let responses = vec![
        create_test_response("resp-1", "exec-abc-123"),
        create_test_response("resp-2", "exec-def-456"),
    ];
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let config = ReplayConfig::default();
    let adapter = ReplayBrokerAdapter::new(journal, config);
    
    assert!(adapter.has_responses());
    assert_eq!(adapter.response_count(), 2);
}

#[test]
fn test_factory_creates_replay_adapter_from_config() {
    let responses = vec![
        create_test_response("1", "exec-1"),
        create_test_response("2", "exec-2"),
    ];
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let factory = AdapterFactory::with_journal(None, journal);
    
    let exec_config = ExecutionConfig {
        data_source: DataSource::replay("test-query"),
        verbose_logging: true,
        timeout_ms: 30000,
    };
    let broker_config = BrokerConfig { name: "alpaca".to_string() };
    
    let adapter = factory.create_adapter_from_config(&exec_config, broker_config);
    assert!(adapter.is_ok());
}

#[test]
fn test_factory_fails_replay_without_journal() {
    let factory = AdapterFactory::new(None); // No journal provided
    
    let exec_config = ExecutionConfig::paper_with_replay("test");
    let broker_config = BrokerConfig { name: "alpaca".to_string() };
    
    let result = factory.create_adapter_from_config(&exec_config, broker_config);
    assert!(result.is_err());
}

#[test]
fn test_data_source_live_requires_connectivity() {
    let live = DataSource::live("alpaca");
    assert!(live.requires_connectivity());
    assert!(!live.uses_journal());
    assert!(live.is_live());
}

#[test]
fn test_data_source_replay_uses_journal() {
    let replay = DataSource::replay("2024-01-15");
    assert!(!replay.requires_connectivity());
    assert!(replay.uses_journal());
    assert!(replay.is_replay());
}

#[test]
fn test_data_source_mock_neither_connectivity_nor_journal() {
    let mock = DataSource::mock("test-scenario");
    assert!(!mock.requires_connectivity());
    assert!(!mock.uses_journal());
    assert!(mock.is_mock());
}

#[test]
fn test_replay_config_builder() {
    let config = ReplayConfig::all_records()
        .with_looping()
        .with_latency(100);
    
    assert!(config.loop_replay);
    assert_eq!(config.artificial_latency_ms, 100);
}

#[test]
fn test_replay_config_for_symbol() {
    let config = ReplayConfig::for_symbol("AAPL");
    assert_eq!(config.query, "AAPL");
}

#[test]
fn test_replay_config_from_date() {
    let config = ReplayConfig::from_date("2024-01-15");
    assert_eq!(config.query, "2024-01-15");
}

#[test]
fn test_execution_config_live_trading() {
    let config = ExecutionConfig::live("alpaca");
    assert!(config.is_live());
    assert_eq!(config.timeout_ms, 30000);
}

#[test]
fn test_execution_config_paper_with_replay() {
    let config = ExecutionConfig::paper_with_replay("2024-01-15");
    assert!(!config.is_live());
    assert!(config.data_source.is_replay());
    assert!(config.verbose_logging);
}

#[test]
fn test_execution_config_backtest() {
    let config = ExecutionConfig::backtest("bull_market_scenario");
    assert!(config.data_source.is_mock());
    assert!(config.verbose_logging);
}

#[test]
fn test_replay_adapter_position_tracking() {
    let responses: Vec<_> = (0..10)
        .map(|i| create_test_response(&format!("resp-{}", i), &format!("exec-{}", i)))
        .collect();
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
    
    assert_eq!(adapter.response_count(), 10);
    assert_eq!(adapter.current_position(), 0);
}

#[test]
fn test_replay_adapter_reset() {
    let responses = vec![
        create_test_response("1", "exec-1"),
        create_test_response("2", "exec-2"),
    ];
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
    
    // Position should start at 0
    assert_eq!(adapter.current_position(), 0);
    
    // Reset should still be at 0
    adapter.reset();
    assert_eq!(adapter.current_position(), 0);
}

#[tokio::test]
async fn test_replay_adapter_returns_execution_ids() {
    let responses = vec![
        create_test_response("resp-1", "exec-abc-123"),
    ];
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
    
    let cmd = create_order_cmd("AAPL");
    let result = adapter.submit_order(cmd).await;
    
    assert!(result.is_ok());
    assert_eq!(result.unwrap().0, "exec-abc-123");
}

#[tokio::test]
async fn test_replay_adapter_handles_cancel() {
    let responses = vec![
        create_test_response("cancel-resp", "cancelled"),
    ];
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
    
    let cancel_cmd = CancelCmd {
        execution_id: ExecutionId("test-order".to_string()),
        symbol: "TEST".to_string(),
        correlation_id: None,
    };
    
    let result = adapter.cancel_order(cancel_cmd).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_replay_adapter_handles_replace() {
    let responses = vec![
        create_test_response("replace-resp", "replaced"),
    ];
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
    
    use broker_gateway_service::core::domain::order::ReplaceCmd;
    
    let replace_cmd = ReplaceCmd {
        execution_id: ExecutionId("test-order".to_string()),
        symbol: "AAPL".to_string(),
        side: OrderSide::Buy,
        qty: Some(200),
        limit_price: Some(150.0),
        correlation_id: None,
    };
    
    let result = adapter.replace_order(replace_cmd).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_replay_adapter_handles_query() {
    let responses = vec![
        create_test_response("query-resp", "status"),
    ];
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
    
    use broker_gateway_service::core::domain::order::StatusQuery;
    
    let query = StatusQuery {
        execution_id: ExecutionId("test-order".to_string()),
        correlation_id: None,
    };
    
    let result = adapter.query_status(query).await;
    assert!(result.is_ok());
}

#[test]
fn test_hybrid_data_source_uses_primary() {
    let primary = Box::new(DataSource::live("alpaca"));
    let fallback = Box::new(DataSource::mock("fallback"));
    let hybrid = DataSource::Hybrid {
        primary,
        fallback,
        error_threshold: 3,
    };
    
    assert!(hybrid.is_hybrid());
    assert!(hybrid.requires_connectivity()); // Primary is live
}

#[test]
fn test_hybrid_with_replay_fallback() {
    let primary = Box::new(DataSource::live("alpaca"));
    let fallback = Box::new(DataSource::replay("backup"));
    let hybrid = DataSource::Hybrid {
        primary,
        fallback,
        error_threshold: 5,
    };
    
    assert!(hybrid.uses_journal()); // Fallback uses journal
}

#[test]
fn test_replay_config_error_injection() {
    let config = ReplayConfig::default()
        .with_error_injection(0.5);
    
    assert!(config.inject_errors);
    assert_eq!(config.error_rate, 0.5);
}

#[test]
fn test_replay_config_error_rate_clamping() {
    // Should clamp to 1.0
    let config = ReplayConfig::default().with_error_injection(2.0);
    assert_eq!(config.error_rate, 1.0);
    
    // Should clamp to 0.0 (but injection still enabled)
    let config = ReplayConfig::default().with_error_injection(-0.5);
    assert_eq!(config.error_rate, 0.0);
}

#[test]
fn test_replay_adapter_refresh() {
    let responses = vec![
        create_test_response("1", "exec-1"),
    ];
    
    let journal = Arc::new(MockJournal::with_responses(responses));
    let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
    
    assert_eq!(adapter.response_count(), 1);
    
    // Refresh should work (even if no new data)
    adapter.refresh();
    assert_eq!(adapter.response_count(), 1);
}

#[test]
fn test_data_source_display_formats() {
    assert_eq!(DataSource::live("alpaca").to_string(), "live:alpaca");
    assert_eq!(DataSource::mock("test").to_string(), "mock:test");
    assert_eq!(DataSource::replay("AAPL").to_string(), "replay:AAPL");
    
    let looping = DataSource::Replay {
        query: "2024-01-15".to_string(),
        loop_replay: true,
        latency_ms: 0,
    };
    assert!(looping.to_string().contains("loop"));
    
    let with_latency = DataSource::Replay {
        query: "test".to_string(),
        loop_replay: false,
        latency_ms: 100,
    };
    assert!(with_latency.to_string().contains("100ms"));
}

#[test]
fn test_execution_config_builder() {
    let config = ExecutionConfig::live("alpaca")
        .with_verbose_logging()
        .with_timeout(5000);
    
    assert!(config.verbose_logging);
    assert_eq!(config.timeout_ms, 5000);
}

#[test]
fn test_replay_extracts_id_from_different_fields() {
    let journal = Arc::new(MockJournal::with_responses(vec![]));
    let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
    
    // Test with execution_id field
    let response1 = ResponseRecord {
        id: "test".to_string(),
        raw_payload: Some(r#"{"execution_id": "exec-123"}"#.to_string()),
        correlation_id: None,
    };
    let id1 = adapter.extract_execution_id(&response1).unwrap();
    assert_eq!(id1.0, "exec-123");
    
    // Test with id field (fallback)
    let response2 = ResponseRecord {
        id: "test".to_string(),
        raw_payload: Some(r#"{"id": "order-456"}"#.to_string()),
        correlation_id: None,
    };
    let id2 = adapter.extract_execution_id(&response2).unwrap();
    assert_eq!(id2.0, "order-456");
    
    // Test with no payload (uses record id)
    let response3 = ResponseRecord {
        id: "record-789".to_string(),
        raw_payload: None,
        correlation_id: None,
    };
    let id3 = adapter.extract_execution_id(&response3).unwrap();
    assert!(id3.0.contains("record-789"));
}
