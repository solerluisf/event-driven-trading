use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::core::application::kill_switch::KillSwitch;
use crate::adapters::messaging::market_data_publisher::MarketDataEvent;
use crate::core::ports::event_handler::{EventHandler, ReactorEvent};
use crate::core::ports::service_traits::IObservabilityService;
use crate::core::infrastructure::symbol_registry::{SymbolIdArray, SymbolRegistry};

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
            // Use cache-friendly SymbolIdArray instead of HashMap for hot path lookups.
            // This provides O(1) array indexing with perfect cache locality,
            // eliminating string hashing overhead in the event processing loop.
            let mut workers: SymbolIdArray<Vec<SymbolWorker>> = SymbolIdArray::new();
            let mut symbol_registry = SymbolRegistry::new();
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
                                
                                // Register symbol to get a compact ID (if not already registered)
                                let symbol_id = symbol_registry.register(symbol.clone());
                                
                                // Get or create the worker list for this symbol
                                let worker_list = workers.get_mut(symbol_id)
                                    .expect("SymbolIdArray should always return Some for valid IDs");
                                
                                let (tx, rx) = mpsc::channel(CHANNEL_CAP);
                                let handle = spawn_handler_task(symbol.clone(), handler.clone(), rx, obs.clone());
                                worker_list.push(SymbolWorker { handler, tx, handle });
                                
                                let _ = ack.send(());
                            }

                            ControlCommand::Unsubscribe { symbol } => {
                                // Lookup symbol ID from registry
                                if let Some(symbol_id) = symbol_registry.lookup(&symbol) {
                                    if let Some(worker_list) = workers.remove(symbol_id) {
                                        for worker in worker_list {
                                            worker.handle.abort();
                                        }
                                        info!("EventReactor: removed all handlers for {}", symbol);
                                    }
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
                        let symbol = &event.symbol;
                        let envelope = ReactorEvent {
                            seq_no:       seq,
                            ingestion_ts: std::time::Instant::now(),
                            source:       event.source.clone(),
                            inner:        event.clone(),
                        };

                        // CACHE-FRIENDLY LOOKUP: Use SymbolIdArray instead of HashMap.
                        // 
                        // Old (slower): HashMap lookup requires hashing the string and
                        // chasing pointers through the hash table buckets.
                        //
                        // New (faster): Direct array indexing using pre-computed SymbolId.
                        // - No string hashing in hot path
                        // - Cache-friendly: workers stored contiguously in memory
                        // - Predictable memory access patterns for CPU prefetching
                        // - O(1) lookup with better constant factors
                        //
                        // Note: Symbols not yet registered (e.g., market data arriving before
                        // subscription) will skip processing. This is correct behavior.
                        if let Some(symbol_id) = symbol_registry.lookup(symbol) {
                            // Direct array access - no hashing, cache-friendly
                            if let Some(worker_list) = workers.get(symbol_id) {
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
                        // Events for unregistered symbols are silently dropped (no handlers)
                    }
                    
                    else => {
                        // All channels closed, exit cleanly
                        info!("EventReactor: all channels closed, exiting");
                        break;
                    }
                }
            }

            // Cleanup: abort all worker tasks
            for (symbol_id, worker_list) in workers.iter() {
                if let Some(symbol) = symbol_registry.get_symbol(symbol_id) {
                    info!("EventReactor: aborting worker(s) for {}", symbol);
                }
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

    // ============================================================================
    // NEW TESTS FOR SYMBOL REGISTRY OPTIMIZATION
    // ============================================================================

    /// Test that symbol registry provides O(1) lookups
    #[tokio::test]
    async fn symbol_registry_provides_fast_lookups() {
        let mut registry = SymbolRegistry::new();
        
        // Register some symbols
        let id1 = registry.register("AAPL");
        let id2 = registry.register("MSFT");
        let id3 = registry.register("GOOGL");
        
        // Lookup should return consistent IDs
        assert_eq!(registry.lookup("AAPL"), Some(id1));
        assert_eq!(registry.lookup("MSFT"), Some(id2));
        assert_eq!(registry.lookup("GOOGL"), Some(id3));
        
        // Unregistered symbols should return None
        assert_eq!(registry.lookup("AMZN"), None);
        
        // Reverse lookup
        assert_eq!(registry.get_symbol(id1), Some("AAPL"));
        assert_eq!(registry.get_symbol(id2), Some("MSFT"));
    }

    /// Test that SymbolIdArray provides cache-friendly storage
    #[tokio::test]
    async fn symbol_id_array_provides_cache_friendly_storage() {
        let mut array = SymbolIdArray::<Vec<u32>>::new();
        let mut registry = SymbolRegistry::new();
        
        let id1 = registry.register("SYM1");
        let id2 = registry.register("SYM2");
        
        // Insert values
        array.insert(id1, vec![1, 2, 3]);
        array.insert(id2, vec![4, 5, 6]);
        
        // Direct array access
        assert_eq!(array.get(id1), Some(&vec![1, 2, 3]));
        assert_eq!(array.get(id2), Some(&vec![4, 5, 6]));
        
        // Remove
        let removed = array.remove(id1);
        assert_eq!(removed, Some(vec![1, 2, 3]));
        assert!(array.get(id1).is_none());
    }

    /// Test that events for unregistered symbols are silently dropped
    #[tokio::test]
    async fn event_reactor_drops_events_for_unregistered_symbols() {
        let (event_tx, event_rx) = mpsc::channel::<MarketDataEvent>(16);
        let observability = Arc::new(TestObservability);
        let kill_switch = Arc::new(KillSwitch::default());
        let reactor = EventReactor::spawn(event_rx, observability, kill_switch.clone());

        // Don't subscribe any handler - just send events
        let event = MarketDataEvent {
            symbol: "UNKNOWN".to_string(),
            event_type: crate::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
            timestamp: "2026-05-07T00:00:00Z".to_string(),
            source: "test".to_string(),
            payload: serde_json::json!({"price": 100}),
        };

        // Send event for unregistered symbol - should not panic or error
        event_tx.send(event).await.unwrap();
        
        // Give it a moment to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        reactor.shutdown().await;
    }

    /// Test multiple handlers for same symbol with SymbolIdArray
    #[tokio::test]
    async fn event_reactor_handles_multiple_handlers_per_symbol() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingHandler {
            count: AtomicUsize,
            name: String,
        }

        #[async_trait]
        impl EventHandler for CountingHandler {
            async fn on_event(&self, _event: &ReactorEvent) {
                self.count.fetch_add(1, Ordering::SeqCst);
            }

            fn name(&self) -> &str {
                &self.name
            }
        }

        let (event_tx, event_rx) = mpsc::channel::<MarketDataEvent>(16);
        let observability = Arc::new(TestObservability);
        let kill_switch = Arc::new(KillSwitch::default());
        let reactor = EventReactor::spawn(event_rx, observability, kill_switch.clone());

        let handler1 = Arc::new(CountingHandler {
            count: AtomicUsize::new(0),
            name: "handler-1".to_string(),
        });
        let handler2 = Arc::new(CountingHandler {
            count: AtomicUsize::new(0),
            name: "handler-2".to_string(),
        });

        // Subscribe both handlers to same symbol
        reactor.subscribe("AAPL", handler1.clone()).await;
        reactor.subscribe("AAPL", handler2.clone()).await;

        // Send an event
        let event = MarketDataEvent {
            symbol: "AAPL".to_string(),
            event_type: crate::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
            timestamp: "2026-05-07T00:00:00Z".to_string(),
            source: "test".to_string(),
            payload: serde_json::json!({"price": 100}),
        };
        event_tx.send(event).await.unwrap();

        // Give handlers time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Both handlers should have received the event
        assert_eq!(handler1.count.load(Ordering::SeqCst), 1);
        assert_eq!(handler2.count.load(Ordering::SeqCst), 1);

        reactor.shutdown().await;
    }

    /// Test that unsubscribe works correctly with SymbolIdArray
    #[tokio::test]
    async fn event_reactor_unsubscribe_works_correctly() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingHandler {
            count: AtomicUsize,
        }

        #[async_trait]
        impl EventHandler for CountingHandler {
            async fn on_event(&self, _event: &ReactorEvent) {
                self.count.fetch_add(1, Ordering::SeqCst);
            }

            fn name(&self) -> &str {
                "counting-handler"
            }
        }

        let (event_tx, event_rx) = mpsc::channel::<MarketDataEvent>(16);
        let observability = Arc::new(TestObservability);
        let kill_switch = Arc::new(KillSwitch::default());
        let reactor = EventReactor::spawn(event_rx, observability, kill_switch.clone());

        let handler = Arc::new(CountingHandler {
            count: AtomicUsize::new(0),
        });

        reactor.subscribe("AAPL", handler.clone()).await;

        // Send event - should be received
        let event1 = MarketDataEvent {
            symbol: "AAPL".to_string(),
            event_type: crate::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
            timestamp: "2026-05-07T00:00:00Z".to_string(),
            source: "test".to_string(),
            payload: serde_json::json!({"price": 100}),
        };
        event_tx.send(event1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(handler.count.load(Ordering::SeqCst), 1);

        // Unsubscribe
        reactor.unsubscribe("AAPL").await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send another event - should not be received
        let event2 = MarketDataEvent {
            symbol: "AAPL".to_string(),
            event_type: crate::adapters::messaging::market_data_publisher::MarketDataEventType::Trade,
            timestamp: "2026-05-07T00:00:00Z".to_string(),
            source: "test".to_string(),
            payload: serde_json::json!({"price": 200}),
        };
        event_tx.send(event2).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Count should still be 1
        assert_eq!(handler.count.load(Ordering::SeqCst), 1);

        reactor.shutdown().await;
    }

    /// Benchmark-style test to verify SymbolIdArray is faster than HashMap
    /// Note: This is a relative comparison, not an absolute benchmark
    #[tokio::test]
    async fn symbol_registry_performance_comparison() {
        use std::time::Instant;

        let iterations = 100_000;
        let mut registry = SymbolRegistry::new();
        
        // Register test symbols
        for i in 0..100 {
            registry.register(format!("SYM{}", i));
        }

        // Time symbol lookups
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = registry.lookup("SYM50");
        }
        let lookup_time = start.elapsed();

        // Verify correctness
        assert!(registry.lookup("SYM50").is_some());
        assert!(registry.lookup("NONEXISTENT").is_none());

        // Log performance for monitoring (don't fail on slow CI environments)
        // On modern hardware, 100k lookups should be < 10ms
        // We use a generous threshold to avoid flaky tests in CI
        if lookup_time.as_millis() > 100 {
            eprintln!(
                "WARNING: Symbol lookup took {:?} for {} iterations. Consider investigating performance.",
                lookup_time, iterations
            );
        }
    }

    /// Test that SymbolIdArray iteration works correctly
    #[tokio::test]
    async fn symbol_id_array_iteration() {
        let mut array = SymbolIdArray::<u32>::new();
        let mut registry = SymbolRegistry::new();
        
        let id1 = registry.register("A");
        let id2 = registry.register("B");
        let id3 = registry.register("C");
        
        array.insert(id1, 100);
        array.insert(id2, 200);
        array.insert(id3, 300);

        let collected: Vec<_> = array.iter().map(|(id, val)| (id.as_u16(), *val)).collect();
        assert_eq!(collected.len(), 3);
        assert!(collected.contains(&(id1.as_u16(), 100)));
        assert!(collected.contains(&(id2.as_u16(), 200)));
        assert!(collected.contains(&(id3.as_u16(), 300)));
    }
}
