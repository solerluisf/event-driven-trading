// adapters/broker/replay_adapter.rs
//
// Broker adapter that replays historical responses from the journal
// instead of making live broker API calls. Used for backtesting and simulation.

use async_trait::async_trait;
use std::sync::{Arc, Mutex};

use crate::core::ports::execution_port::IExecutionPort;
use crate::core::ports::journal_repo::IJournalRepo;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId, OrderSide, OrderStatusResponse};
use crate::adapters::broker::broker_error::BrokerError;
use crate::core::domain::journal::ResponseRecord;

/// Configuration for replay mode
#[derive(Clone, Debug)]
pub struct ReplayConfig {
    /// Filter query for journal replay (e.g., date range, symbol, order type)
    pub query: String,
    /// Whether to loop through responses repeatedly
    pub loop_replay: bool,
    /// Delay between responses to simulate network latency
    pub artificial_latency_ms: u64,
    /// Whether to inject random errors to test error handling
    pub inject_errors: bool,
    /// Error injection rate (0.0 - 1.0)
    pub error_rate: f64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            query: String::new(),
            loop_replay: false,
            artificial_latency_ms: 0,
            inject_errors: false,
            error_rate: 0.0,
        }
    }
}

impl ReplayConfig {
    /// Create a config for replaying all records
    pub fn all_records() -> Self {
        Self {
            query: String::new(),
            ..Default::default()
        }
    }

    /// Create a config for replaying records from a specific date
    pub fn from_date(date: impl Into<String>) -> Self {
        Self {
            query: date.into(),
            ..Default::default()
        }
    }

    /// Create a config for replaying records for a specific symbol
    pub fn for_symbol(symbol: impl Into<String>) -> Self {
        Self {
            query: symbol.into(),
            ..Default::default()
        }
    }

    /// Enable looping mode
    pub fn with_looping(mut self) -> Self {
        self.loop_replay = true;
        self
    }

    /// Add artificial latency
    pub fn with_latency(mut self, ms: u64) -> Self {
        self.artificial_latency_ms = ms;
        self
    }

    /// Enable error injection for testing
    pub fn with_error_injection(mut self, rate: f64) -> Self {
        self.inject_errors = true;
        self.error_rate = rate.clamp(0.0, 1.0);
        self
    }
}

/// Error types specific to replay mode
#[derive(Debug, Clone)]
pub enum ReplayError {
    /// No matching records found in journal
    NoMatchingRecords(String),
    /// All records have been replayed (when not looping)
    EndOfReplay,
    /// Failed to deserialize response
    DeserializationFailed(String),
    /// Error injection for testing
    InjectedError(String),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::NoMatchingRecords(q) => write!(f, "No journal records match query: {}", q),
            ReplayError::EndOfReplay => write!(f, "End of replay reached"),
            ReplayError::DeserializationFailed(msg) => write!(f, "Failed to deserialize: {}", msg),
            ReplayError::InjectedError(msg) => write!(f, "Injected test error: {}", msg),
        }
    }
}

impl std::error::Error for ReplayError {}

/// State for the replay adapter
struct ReplayState {
    /// All loaded responses
    responses: Vec<ResponseRecord>,
    /// Current position in the response list
    position: usize,
    /// Whether we've reached the end
    end_reached: bool,
}

/// Broker adapter that replays historical responses from journal
pub struct ReplayBrokerAdapter {
    journal: Arc<dyn IJournalRepo>,
    config: ReplayConfig,
    state: Mutex<ReplayState>,
}

impl ReplayBrokerAdapter {
    /// Create a new replay adapter
    pub fn new(journal: Arc<dyn IJournalRepo>, config: ReplayConfig) -> Self {
        // Load responses from journal
        let responses = journal.replay(config.query.clone());
        
        tracing::info!(
            "ReplayBrokerAdapter initialized with {} records for query: '{}'",
            responses.len(),
            config.query
        );

        Self {
            journal,
            config,
            state: Mutex::new(ReplayState {
                responses,
                position: 0,
                end_reached: false,
            }),
        }
    }

    /// Check if there are any responses loaded
    pub fn has_responses(&self) -> bool {
        !self.state.lock().unwrap().responses.is_empty()
    }

    /// Get the number of available responses
    pub fn response_count(&self) -> usize {
        self.state.lock().unwrap().responses.len()
    }

    /// Get the current position in the replay
    pub fn current_position(&self) -> usize {
        self.state.lock().unwrap().position
    }

    /// Reset replay to beginning
    pub fn reset(&self) {
        let mut state = self.state.lock().unwrap();
        state.position = 0;
        state.end_reached = false;
        tracing::debug!("Replay reset to position 0");
    }

    /// Get the next response from the journal
    async fn get_next_response(&self) -> Result<ResponseRecord, ReplayError> {
        // Apply artificial latency if configured
        if self.config.artificial_latency_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.config.artificial_latency_ms)).await;
        }

        // Check for error injection
        if self.config.inject_errors && rand::random::<f64>() < self.config.error_rate {
            return Err(ReplayError::InjectedError(
                "Randomly injected error for testing".to_string()
            ));
        }

        let mut state = self.state.lock().unwrap();

        if state.responses.is_empty() {
            return Err(ReplayError::NoMatchingRecords(self.config.query.clone()));
        }

        if state.end_reached && !self.config.loop_replay {
            return Err(ReplayError::EndOfReplay);
        }

        // Get the next response
        let response = state.responses[state.position].clone();
        
        // Advance position
        state.position += 1;
        
        // Check if we need to loop or end
        if state.position >= state.responses.len() {
            if self.config.loop_replay {
                state.position = 0;
                tracing::debug!("Replay looped back to beginning");
            } else {
                state.end_reached = true;
            }
        }

        Ok(response)
    }

    /// Extract execution ID from a response record
    pub fn extract_execution_id(&self, record: &ResponseRecord) -> Result<ExecutionId, BrokerError> {
        // Try to extract from the raw payload if present
        if let Some(ref payload) = record.raw_payload {
            // Try to parse as JSON and extract execution_id
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
                if let Some(id) = json.get("execution_id").and_then(|v| v.as_str()) {
                    return Ok(ExecutionId(id.to_string()));
                }
                if let Some(id) = json.get("id").and_then(|v| v.as_str()) {
                    return Ok(ExecutionId(id.to_string()));
                }
            }
        }
        
        // Fallback: generate from record id
        Ok(ExecutionId(format!("replay-{}", record.id)))
    }

    /// Refresh responses from journal (useful if journal is being written to during replay)
    pub fn refresh(&self) {
        let new_responses = self.journal.replay(self.config.query.clone());
        let mut state = self.state.lock().unwrap();
        state.responses = new_responses;
        tracing::debug!("Replay responses refreshed, {} records available", state.responses.len());
    }
}

#[async_trait]
impl IExecutionPort for ReplayBrokerAdapter {
    type Error = BrokerError;

    async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, Self::Error> {
        tracing::info!(
            "Replay: submit_order for {} (not calling live broker)",
            cmd.symbol
        );

        let response = self.get_next_response().await.map_err(|e| {
            BrokerError::Unknown(format!("Replay error: {}", e))
        })?;

        self.extract_execution_id(&response)
    }

    async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), Self::Error> {
        tracing::info!(
            "Replay: cancel_order for {} (not calling live broker)",
            cmd.execution_id.0
        );

        let _response = self.get_next_response().await.map_err(|e| {
            BrokerError::Unknown(format!("Replay error: {}", e))
        })?;

        Ok(())
    }

    async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), Self::Error> {
        tracing::info!(
            "Replay: replace_order for {} (not calling live broker)",
            cmd.execution_id.0
        );

        let _response = self.get_next_response().await.map_err(|e| {
            BrokerError::Unknown(format!("Replay error: {}", e))
        })?;

        Ok(())
    }

    async fn query_status(&self, query: StatusQuery) -> Result<OrderStatusResponse, Self::Error> {
        tracing::info!(
            "Replay: query_status for {} (not calling live broker)",
            query.execution_id.0
        );

        let response = self.get_next_response().await.map_err(|e| {
            BrokerError::Unknown(format!("Replay error: {}", e))
        })?;

        // Try to parse order status from the response payload
        if let Some(ref payload) = response.raw_payload {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
                let status = json.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let symbol = json.get("symbol")
                    .and_then(|v| v.as_str())
                    .unwrap_or("UNKNOWN");
                let side_str = json.get("side")
                    .and_then(|v| v.as_str())
                    .unwrap_or("buy");
                let qty = json.get("qty")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(100) as u32;

                let side = match side_str.to_lowercase().as_str() {
                    "sell" => OrderSide::Sell,
                    _ => OrderSide::Buy,
                };

                return Ok(OrderStatusResponse::new(
                    query.execution_id.0.clone(),
                    status,
                    symbol,
                    side,
                    qty,
                ));
            }
        }

        // Return a default response if parsing fails
        Ok(OrderStatusResponse::new(
            query.execution_id.0.clone(),
            "new",
            "REPLAY",
            OrderSide::Buy,
            100,
        ))
    }
}

impl Default for ReplayBrokerAdapter {
    fn default() -> Self {
        // Create a dummy journal that returns empty results
        struct DummyJournal;
        impl IJournalRepo for DummyJournal {
            fn persist_outbound(&self, _record: crate::core::domain::journal::RequestRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
                Ok(())
            }
            fn persist_inbound(&self, _record: ResponseRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
                Ok(())
            }
            fn replay(&self, _query: String) -> Vec<ResponseRecord> {
                vec![]
            }
        }
        
        Self::new(Arc::new(DummyJournal), ReplayConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::journal::{RequestRecord, ResponseRecord};
    use crate::core::ports::journal_repo::{IJournalRepo, JournalResult};

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

    fn create_test_response(id: &str, payload: Option<&str>) -> ResponseRecord {
        ResponseRecord {
            id: id.to_string(),
            raw_payload: payload.map(|s| s.to_string()),
            correlation_id: None,
        }
    }

    #[test]
    fn test_replay_adapter_initializes_with_responses() {
        let responses = vec![
            create_test_response("1", Some(r#"{"execution_id": "exec-1"}"#)),
            create_test_response("2", Some(r#"{"execution_id": "exec-2"}"#)),
        ];
        
        let journal = Arc::new(MockJournal::with_responses(responses));
        let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
        
        assert!(adapter.has_responses());
        assert_eq!(adapter.response_count(), 2);
    }

    #[test]
    fn test_replay_adapter_handles_empty_journal() {
        let journal = Arc::new(MockJournal::with_responses(vec![]));
        let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
        
        assert!(!adapter.has_responses());
        assert_eq!(adapter.response_count(), 0);
    }

    #[test]
    fn test_replay_position_tracking() {
        let responses = vec![
            create_test_response("1", None),
            create_test_response("2", None),
            create_test_response("3", None),
        ];
        
        let journal = Arc::new(MockJournal::with_responses(responses));
        let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
        
        assert_eq!(adapter.current_position(), 0);
    }

    #[test]
    fn test_replay_reset() {
        let responses = vec![
            create_test_response("1", None),
            create_test_response("2", None),
        ];
        
        let journal = Arc::new(MockJournal::with_responses(responses));
        let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
        
        // Position should start at 0
        assert_eq!(adapter.current_position(), 0);
        
        // Reset should work
        adapter.reset();
        assert_eq!(adapter.current_position(), 0);
    }

    #[test]
    fn test_replay_config_default() {
        let config = ReplayConfig::default();
        assert!(config.query.is_empty());
        assert!(!config.loop_replay);
        assert_eq!(config.artificial_latency_ms, 0);
        assert!(!config.inject_errors);
        assert_eq!(config.error_rate, 0.0);
    }

    #[test]
    fn test_replay_config_builder() {
        let config = ReplayConfig::for_symbol("AAPL")
            .with_looping()
            .with_latency(100)
            .with_error_injection(0.1);
        
        assert_eq!(config.query, "AAPL");
        assert!(config.loop_replay);
        assert_eq!(config.artificial_latency_ms, 100);
        assert!(config.inject_errors);
        assert_eq!(config.error_rate, 0.1);
    }

    #[test]
    fn test_extract_execution_id_from_json() {
        let journal = Arc::new(MockJournal::with_responses(vec![]));
        let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
        
        let response = create_test_response("test", Some(r#"{"execution_id": "abc-123"}"#));
        let id = adapter.extract_execution_id(&response).unwrap();
        assert_eq!(id.0, "abc-123");
    }

    #[test]
    fn test_extract_execution_id_from_id_field() {
        let journal = Arc::new(MockJournal::with_responses(vec![]));
        let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
        
        let response = create_test_response("test", Some(r#"{"id": "xyz-789"}"#));
        let id = adapter.extract_execution_id(&response).unwrap();
        assert_eq!(id.0, "xyz-789");
    }

    #[test]
    fn test_extract_execution_id_fallback() {
        let journal = Arc::new(MockJournal::with_responses(vec![]));
        let adapter = ReplayBrokerAdapter::new(journal, ReplayConfig::default());
        
        let response = create_test_response("test-id", None);
        let id = adapter.extract_execution_id(&response).unwrap();
        assert!(id.0.contains("test-id"));
        assert!(id.0.starts_with("replay-"));
    }

    #[test]
    fn test_replay_error_display() {
        let err = ReplayError::NoMatchingRecords("test-query".to_string());
        assert!(err.to_string().contains("test-query"));
        
        let err = ReplayError::EndOfReplay;
        assert!(err.to_string().contains("End of replay"));
        
        let err = ReplayError::DeserializationFailed("bad json".to_string());
        assert!(err.to_string().contains("bad json"));
        
        let err = ReplayError::InjectedError("test".to_string());
        assert!(err.to_string().contains("test"));
    }
}
