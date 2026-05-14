use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::core::application::kill_switch::KillSwitch;
use crate::adapters::messaging::market_data_publisher::MarketDataEvent;
use crate::core::ports::event_handler::{EventHandler, ReactorEvent};
use crate::core::ports::service_traits::IObservabilityService;

const CHANNEL_CAP: usize = 256;

// ── Control commands sent to a running reactor ─────────────────────────────

pub enum ControlCommand {
    /// Register a new handler for a symbol at runtime.
    Subscribe {
        symbol:  String,
        handler: Arc<dyn EventHandler>,
        ack:     oneshot::Sender<()>,
    },
    /// Remove all handlers for a symbol and drop its task.
    Unsubscribe { symbol: String },
    /// Drain all channels and exit cleanly.
    Shutdown,
}

// ── Per-symbol worker state ────────────────────────────────────────────────

struct SymbolWorker {
    handler: Arc<dyn EventHandler>,
    tx:      mpsc::Sender<ReactorEvent>,
    handle:  JoinHandle<()>,
}

// ── The reactor itself ─────────────────────────────────────────────────────

pub struct EventReactor {
    control_tx: mpsc::Sender<ControlCommand>,
    handle:     JoinHandle<()>,
}

impl EventReactor {
    /// Spawn the reactor. `event_rx` receives normalized MarketDataEvents from
    /// the market data adapter and fans them out to symbol-specific handlers.
    pub fn spawn(
        mut event_rx: mpsc::Receiver<MarketDataEvent>,
        obs:          Arc<dyn IObservabilityService>,
        kill_switch:  Arc<KillSwitch>,
    ) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel::<ControlCommand>(64);

        let handle = tokio::spawn(async move {
            let mut workers: HashMap<String, Vec<SymbolWorker>> = HashMap::new();
            let mut seq: u64 = 0;
            let mut shutdown_requested = false;

            loop {
                // Check for shutdown conditions before blocking
                if shutdown_requested || kill_switch.is_enabled() {
                    info!("EventReactor: shutting down (requested={}, kill_switch={})", 
                        shutdown_requested, kill_switch.is_enabled());
                    break;
                }

                tokio::select! {
                    biased; // Prioritize control commands over events
                    
                    Some(cmd) = control_rx.recv() => {
                        match cmd {
                            ControlCommand::Subscribe { symbol, handler, ack } => {
                                info!("EventReactor: subscribing handler '{}' for {}", handler.name(), symbol);
                                let entry = workers.entry(symbol.clone()).or_insert_with(Vec::new);
                                let (tx, rx) = mpsc::channel(CHANNEL_CAP);
                                let handle = spawn_handler_task(symbol.clone(), handler.clone(), rx, obs.clone());
                                entry.push(SymbolWorker { handler, tx, handle });
                                let _ = ack.send(());
                            }

                            ControlCommand::Unsubscribe { symbol } => {
                                if let Some(worker_list) = workers.remove(&symbol) {
                                    for worker in worker_list {
                                        worker.handle.abort();
                                    }
                                    info!("EventReactor: removed all handlers for {}", symbol);
                                }
                            }

                            ControlCommand::Shutdown => {
                                info!("EventReactor: shutdown command received");
                                shutdown_requested = true;
                            }
                        }
                    }

                    Some(event) = event_rx.recv() => {
                        if kill_switch.is_enabled() {
                            info!("EventReactor: kill switch triggered, dropping event and exiting");
                            break;
                        }
                        
                        seq += 1;
                        let symbol = event.symbol.clone();
                        let envelope = ReactorEvent {
                            seq_no:       seq,
                            ingestion_ts: std::time::Instant::now(),
                            source:       event.source.clone(),
                            inner:        event.clone(),
                        };

                        if let Some(worker_list) = workers.get(&symbol) {
                            for worker in worker_list {
                                // Use send() instead of try_send() to wait for channel space.
                                // This prevents silent event drops which can cause stale positions.
                                match tokio::time::timeout(
                                    std::time::Duration::from_millis(100),
                                    worker.tx.send(envelope.clone())
                                ).await {
                                    Ok(Ok(())) => {}
                                    Ok(Err(_)) => {
                                        error!("EventReactor: channel closed for {}", symbol);
                                    }
                                    Err(_) => {
                                        warn!(
                                            "EventReactor: channel full for {}, send timed out (handler: {})",
                                            symbol,
                                            worker.handler.name(),
                                        );
                                        obs.emit_event(format!("reactor.drop.{}", symbol));
                                    }
                                }
                            }
                        }
                    }
                    
                    else => {
                        // All channels closed, exit cleanly
                        info!("EventReactor: all channels closed, exiting");
                        break;
                    }
                }
            }

            for (sym, worker_list) in workers {
                info!("EventReactor: aborting worker(s) for {}", sym);
                for worker in worker_list {
                    worker.handle.abort();
                }
            }
        });

        EventReactor { control_tx, handle }
    }

    /// Register a handler for a symbol from outside the reactor task.
    pub async fn subscribe(&self, symbol: impl Into<String>, handler: Arc<dyn EventHandler>) {
        let (ack_tx, ack_rx) = oneshot::channel();
        let _ = self.control_tx.send(ControlCommand::Subscribe {
            symbol:  symbol.into(),
            handler,
            ack:      ack_tx,
        }).await;
        let _ = ack_rx.await;
    }

    /// Remove all handlers for a symbol.
    pub async fn unsubscribe(&self, symbol: impl Into<String>) {
        let _ = self.control_tx.send(ControlCommand::Unsubscribe {
            symbol: symbol.into(),
        }).await;
    }

    /// Graceful shutdown.
    pub async fn shutdown(self) {
        let _ = self.control_tx.send(ControlCommand::Shutdown).await;
        let _ = self.handle.await;
    }
}

// ── Per-symbol handler task ────────────────────────────────────────────────

fn spawn_handler_task(
    symbol: String,
    handler: Arc<dyn EventHandler>,
    mut rx: mpsc::Receiver<ReactorEvent>,
    obs:    Arc<dyn IObservabilityService>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let latency_ms = event.ingestion_ts.elapsed().as_secs_f64() * 1000.0;
            obs.emit_event(format!("reactor.latency_ms.{}={:.3}", symbol, latency_ms));

            handler.on_event(&event).await;
        }

        info!("EventReactor: handler task for {} ended", symbol);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};

    struct TestObservability;

    impl IObservabilityService for TestObservability {
        fn record_outbound(&self, _record: crate::core::domain::journal::RequestRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
            Ok(())
        }
        fn record_inbound(&self, _record: crate::core::domain::journal::ResponseRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
            Ok(())
        }
        fn emit_event(&self, _event: String) {}
    }

    struct TestHandler {
        done_tx: Mutex<Option<oneshot::Sender<u64>>>,
    }

    #[async_trait]
    impl EventHandler for TestHandler {
        async fn on_event(&self, event: &ReactorEvent) {
            if let Some(tx) = self.done_tx.lock().unwrap().take() {
                let _ = tx.send(event.seq_no);
            }
        }

        fn name(&self) -> &str {
            "test-handler"
        }
    }

    #[tokio::test]
    async fn event_reactor_dispatches_symbol_specific_events() {
        let (event_tx, event_rx) = mpsc::channel::<MarketDataEvent>(16);
        let observability = Arc::new(TestObservability);
        let kill_switch = Arc::new(KillSwitch::default());
        let reactor = EventReactor::spawn(event_rx, observability, kill_switch.clone());

        let (done_tx, done_rx) = oneshot::channel();
        let handler = Arc::new(TestHandler {
            done_tx: Mutex::new(Some(done_tx)),
        });

        reactor.subscribe("AAPL", handler).await;

        let event = MarketDataEvent {
            symbol: "AAPL".to_string(),
            event_type: crate::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
            timestamp: "2026-05-07T00:00:00Z".to_string(),
            source: "alpaca".to_string(),
            payload: serde_json::json!({"price": 100}),
        };

        event_tx.send(event).await.unwrap();

        let received_seq = tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("handler did not receive event")
            .expect("handler send failed");

        assert_eq!(received_seq, 1);

        reactor.shutdown().await;
    }

    /// Test that source field from MarketDataEvent is correctly propagated to ReactorEvent
    #[tokio::test]
    async fn event_reactor_propagates_source_field() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct SourceCapturingHandler {
            captured_source: Mutex<Option<String>>,
            done_tx: Mutex<Option<oneshot::Sender<()>>>,
        }

        #[async_trait]
        impl EventHandler for SourceCapturingHandler {
            async fn on_event(&self, event: &ReactorEvent) {
                *self.captured_source.lock().unwrap() = Some(event.source.clone());
                if let Some(tx) = self.done_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
            }

            fn name(&self) -> &str {
                "source-capturing-handler"
            }
        }

        let (event_tx, event_rx) = mpsc::channel::<MarketDataEvent>(16);
        let observability = Arc::new(TestObservability);
        let kill_switch = Arc::new(KillSwitch::default());
        let reactor = EventReactor::spawn(event_rx, observability, kill_switch.clone());

        let (done_tx, done_rx) = oneshot::channel();
        let handler = Arc::new(SourceCapturingHandler {
            captured_source: Mutex::new(None),
            done_tx: Mutex::new(Some(done_tx)),
        });

        reactor.subscribe("AAPL", handler.clone()).await;

        // Send event with a custom source (not "alpaca")
        let event = MarketDataEvent {
            symbol: "AAPL".to_string(),
            event_type: crate::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
            timestamp: "2026-05-07T00:00:00Z".to_string(),
            source: "fix_gateway".to_string(),
            payload: serde_json::json!({"price": 100}),
        };

        event_tx.send(event).await.unwrap();

        // Wait for handler to receive event
        let _ = tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("handler did not receive event")
            .expect("handler send failed");

        // Verify that the source was correctly propagated
        let captured = handler.captured_source.lock().unwrap();
        assert!(captured.is_some(), "Source should have been captured");
        assert_eq!(captured.as_ref().unwrap(), "fix_gateway", "Source should be 'fix_gateway', not hardcoded 'alpaca'");

        reactor.shutdown().await;
    }

    /// Test that different sources are handled correctly for multiple events
    #[tokio::test]
    async fn event_reactor_handles_multiple_sources() {
        struct MultiSourceHandler {
            sources: Mutex<Vec<String>>,
            done_tx: Mutex<Option<oneshot::Sender<Vec<String>>>>,
            expected_count: usize,
        }

        #[async_trait]
        impl EventHandler for MultiSourceHandler {
            async fn on_event(&self, event: &ReactorEvent) {
                let mut sources = self.sources.lock().unwrap();
                sources.push(event.source.clone());
                if sources.len() >= self.expected_count {
                    if let Some(tx) = self.done_tx.lock().unwrap().take() {
                        let sources_copy = sources.clone();
                        let _ = tx.send(sources_copy);
                    }
                }
            }

            fn name(&self) -> &str {
                "multi-source-handler"
            }
        }

        let (event_tx, event_rx) = mpsc::channel::<MarketDataEvent>(16);
        let observability = Arc::new(TestObservability);
        let kill_switch = Arc::new(KillSwitch::default());
        let reactor = EventReactor::spawn(event_rx, observability, kill_switch.clone());

        let (done_tx, done_rx) = oneshot::channel();
        let handler = Arc::new(MultiSourceHandler {
            sources: Mutex::new(Vec::new()),
            done_tx: Mutex::new(Some(done_tx)),
            expected_count: 3,
        });

        reactor.subscribe("AAPL", handler.clone()).await;

        // Send events with different sources
        let sources = vec!["alpaca", "fix_gateway", "replay"];
        for source in &sources {
            let event = MarketDataEvent {
                symbol: "AAPL".to_string(),
                event_type: crate::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
                timestamp: "2026-05-07T00:00:00Z".to_string(),
                source: source.to_string(),
                payload: serde_json::json!({"price": 100}),
            };
            event_tx.send(event).await.unwrap();
        }

        // Wait for all events to be processed
        let received_sources = tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("handler did not receive all events")
            .expect("handler send failed");

        // Verify that all sources were correctly propagated
        assert_eq!(received_sources, sources, "All sources should be correctly propagated");

        reactor.shutdown().await;
    }

    /// Test that events are not silently dropped when channel is full.
    /// Uses a slow handler to create backpressure and verifies all events are delivered.
    #[tokio::test]
    async fn event_reactor_does_not_drop_events_under_backpressure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct SlowHandler {
            count: AtomicUsize,
            expected_count: usize,
            done_tx: Mutex<Option<oneshot::Sender<usize>>>,
        }

        #[async_trait]
        impl EventHandler for SlowHandler {
            async fn on_event(&self, _event: &ReactorEvent) {
                // Simulate slow processing to create backpressure
                tokio::time::sleep(Duration::from_millis(10)).await;
                let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.expected_count {
                    if let Some(tx) = self.done_tx.lock().unwrap().take() {
                        let _ = tx.send(count);
                    }
                }
            }

            fn name(&self) -> &str {
                "slow-handler"
            }
        }

        let (event_tx, event_rx) = mpsc::channel::<MarketDataEvent>(16);
        let observability = Arc::new(TestObservability);
        let kill_switch = Arc::new(KillSwitch::default());
        let reactor = EventReactor::spawn(event_rx, observability, kill_switch.clone());

        let (done_tx, done_rx) = oneshot::channel();
        let event_count = 20; // Send more events than channel capacity (256)
        let handler = Arc::new(SlowHandler {
            count: AtomicUsize::new(0),
            expected_count: event_count,
            done_tx: Mutex::new(Some(done_tx)),
        });

        reactor.subscribe("AAPL", handler.clone()).await;

        // Send many events rapidly to create backpressure
        for i in 0..event_count {
            let event = MarketDataEvent {
                symbol: "AAPL".to_string(),
                event_type: crate::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
                timestamp: "2026-05-07T00:00:00Z".to_string(),
                source: "test".to_string(),
                payload: serde_json::json!({"seq": i}),
            };
            event_tx.send(event).await.unwrap();
        }

        // Wait for all events to be processed with generous timeout
        let received_count = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("handler did not receive all events - events may have been dropped")
            .expect("handler send failed");

        assert_eq!(
            received_count, event_count,
            "All {} events should be received, but only {} were processed. Events were silently dropped!",
            event_count, received_count
        );

        // Verify final count
        let final_count = handler.count.load(Ordering::SeqCst);
        assert_eq!(
            final_count, event_count,
            "Final count should be {}, but was {}",
            event_count, final_count
        );

        reactor.shutdown().await;
    }
}
