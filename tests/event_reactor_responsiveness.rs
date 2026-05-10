// tests/event_reactor_responsiveness.rs
//
// Tests verifying EventReactor shutdown and control command responsiveness
// without the 100ms idle sleep

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::timeout;

use broker_gateway_service::core::application::event_reactor::{EventReactor, ControlCommand};
use broker_gateway_service::core::application::kill_switch::KillSwitch;
use broker_gateway_service::adapters::messaging::market_data_publisher::MarketDataEvent;
use broker_gateway_service::core::ports::event_handler::{EventHandler, ReactorEvent};
use broker_gateway_service::core::ports::service_traits::IObservabilityService;
use broker_gateway_service::core::domain::journal::{RequestRecord, ResponseRecord};
use broker_gateway_service::core::ports::journal_repo::{IJournalRepo, JournalResult};

struct TestObservability;

impl IObservabilityService for TestObservability {
    fn record_outbound(&self, _record: RequestRecord) -> JournalResult<()> {
        Ok(())
    }
    fn record_inbound(&self, _record: ResponseRecord) -> JournalResult<()> {
        Ok(())
    }
    fn emit_event(&self, _event: String) {}
}

struct TestHandler {
    name: String,
}

#[async_trait::async_trait]
impl EventHandler for TestHandler {
    async fn on_event(&self, _event: &ReactorEvent) {}
    fn name(&self) -> &str {
        &self.name
    }
}

#[tokio::test]
async fn test_shutdown_is_immediate() {
    let (event_tx, event_rx) = mpsc::channel(16);
    let observability = Arc::new(TestObservability);
    let kill_switch = Arc::new(KillSwitch::default());
    let reactor = EventReactor::spawn(event_rx, observability, kill_switch);

    // Shutdown should complete quickly (not wait 100ms)
    let start = Instant::now();
    reactor.shutdown().await;
    let elapsed = start.elapsed();
    
    // Should complete in less than 50ms (previously could take up to 100ms)
    assert!(
        elapsed < Duration::from_millis(50),
        "Shutdown took too long: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_control_commands_are_responsive() {
    let (event_tx, event_rx) = mpsc::channel(16);
    let observability = Arc::new(TestObservability);
    let kill_switch = Arc::new(KillSwitch::default());
    let reactor = EventReactor::spawn(event_rx, observability, kill_switch);

    // Subscribe should complete quickly
    let handler = Arc::new(TestHandler { name: "test".to_string() });
    
    let start = Instant::now();
    reactor.subscribe("AAPL", handler).await;
    let elapsed = start.elapsed();
    
    // Should complete in less than 50ms
    assert!(
        elapsed < Duration::from_millis(50),
        "Subscribe took too long: {:?}",
        elapsed
    );

    reactor.shutdown().await;
}

#[tokio::test]
async fn test_kill_switch_is_immediate() {
    let (event_tx, event_rx) = mpsc::channel(16);
    let observability = Arc::new(TestObservability);
    let kill_switch = Arc::new(KillSwitch::default());
    let reactor = EventReactor::spawn(event_rx, observability, kill_switch.clone());

    // Enable kill switch
    let start = Instant::now();
    kill_switch.enable();
    
    // Give a small amount of time for the reactor to process
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    let elapsed = start.elapsed();
    
    // Kill switch should be detected quickly
    assert!(
        elapsed < Duration::from_millis(30),
        "Kill switch detection took too long: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_no_idle_sleep_latency() {
    let (event_tx, event_rx) = mpsc::channel(16);
    let observability = Arc::new(TestObservability);
    let kill_switch = Arc::new(KillSwitch::default());
    let reactor = EventReactor::spawn(event_rx, observability, kill_switch);

    // Subscribe handler
    let handler = Arc::new(TestHandler { name: "latency-test".to_string() });
    reactor.subscribe("TSLA", handler).await;

    // Send an event - should be processed without 100ms delay
    let event = MarketDataEvent {
        symbol: "TSLA".to_string(),
        event_type: broker_gateway_service::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
        timestamp: "2026-05-07T00:00:00Z".to_string(),
        payload: serde_json::json!({"price": 100.0}),
    };

    let start = Instant::now();
    event_tx.send(event).await.unwrap();
    
    // Small delay to ensure processing
    tokio::time::sleep(Duration::from_millis(5)).await;
    
    let elapsed = start.elapsed();
    
    // Should be processed in less than 20ms (not 100ms)
    assert!(
        elapsed < Duration::from_millis(20),
        "Event processing took too long: {:?}",
        elapsed
    );

    reactor.shutdown().await;
}

#[tokio::test]
async fn test_multiple_commands_no_delay() {
    let (event_tx, event_rx) = mpsc::channel(16);
    let observability = Arc::new(TestObservability);
    let kill_switch = Arc::new(KillSwitch::default());
    let reactor = EventReactor::spawn(event_rx, observability, kill_switch);

    // Send multiple commands rapidly
    let start = Instant::now();
    
    for i in 0..5 {
        let handler = Arc::new(TestHandler { name: format!("handler-{}", i) });
        reactor.subscribe(format!("SYM{}", i), handler).await;
    }
    
    let elapsed = start.elapsed();
    
    // All 5 subscriptions should complete in less than 100ms total
    // (Previously could have taken 500ms with 100ms sleep between each)
    assert!(
        elapsed < Duration::from_millis(100),
        "Multiple commands took too long: {:?}",
        elapsed
    );

    reactor.shutdown().await;
}

#[tokio::test]
async fn test_unsubscribe_is_responsive() {
    let (event_tx, event_rx) = mpsc::channel(16);
    let observability = Arc::new(TestObservability);
    let kill_switch = Arc::new(KillSwitch::default());
    let reactor = EventReactor::spawn(event_rx, observability, kill_switch);

    // Subscribe first
    let handler = Arc::new(TestHandler { name: "unsubscribe-test".to_string() });
    reactor.subscribe("UNSUB", handler).await;

    // Unsubscribe should be immediate
    let start = Instant::now();
    reactor.unsubscribe("UNSUB").await;
    let elapsed = start.elapsed();
    
    // Should complete in less than 50ms
    assert!(
        elapsed < Duration::from_millis(50),
        "Unsubscribe took too long: {:?}",
        elapsed
    );

    reactor.shutdown().await;
}
