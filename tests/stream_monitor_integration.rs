// tests/stream_monitor_integration.rs
//
// Integration tests for stream monitoring, gap detection, and silent disconnect detection

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use broker_gateway_service::core::application::stream_monitor::{
    StreamMonitor, StreamMonitorConfig, StreamControlEvent,
};

#[test]
fn test_stream_monitor_detects_sequence_gaps() {
    let monitor = StreamMonitor::default();
    
    // Process initial events
    let events1 = monitor.process_event("alpaca", "AAPL", 1, Instant::now());
    assert!(events1.is_empty());
    
    let events2 = monitor.process_event("alpaca", "AAPL", 2, Instant::now());
    assert!(events2.is_empty());
    
    // Gap: missing sequence 3
    let events3 = monitor.process_event("alpaca", "AAPL", 5, Instant::now());
    assert_eq!(events3.len(), 1);
    
    match &events3[0] {
        StreamControlEvent::SequenceGap { expected_seq, actual_seq, gap_size, .. } => {
            assert_eq!(*expected_seq, 3);
            assert_eq!(*actual_seq, 5);
            assert_eq!(*gap_size, 2); // Missing 3 and 4
        }
        _ => panic!("Expected SequenceGap event"),
    }
}

#[test]
fn test_stream_monitor_tracks_multiple_streams_independently() {
    let monitor = StreamMonitor::default();
    
    // AAPL has a gap
    monitor.process_event("alpaca", "AAPL", 1, Instant::now());
    let aapl_events = monitor.process_event("alpaca", "AAPL", 3, Instant::now());
    
    // MSFT is continuous
    monitor.process_event("alpaca", "MSFT", 1, Instant::now());
    let msft_events = monitor.process_event("alpaca", "MSFT", 2, Instant::now());
    
    assert_eq!(aapl_events.len(), 1);
    assert!(aapl_events.iter().any(|e| matches!(e, StreamControlEvent::SequenceGap { .. })));
    
    assert!(msft_events.is_empty());
    
    // Stats should reflect this
    let stats = monitor.get_statistics();
    assert_eq!(stats.total_streams, 2);
    assert_eq!(stats.total_gaps, 1);
}

#[test]
fn test_stream_monitor_detects_silent_disconnect() {
    let config = StreamMonitorConfig {
        silence_threshold: Duration::from_millis(50),
        ..Default::default()
    };
    let monitor = StreamMonitor::new(config);
    
    // Initial event
    monitor.process_event("alpaca", "TSLA", 1, Instant::now());
    
    // No events for longer than threshold
    std::thread::sleep(Duration::from_millis(100));
    
    // Check for disconnects
    let events = monitor.check_silent_disconnects();
    
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StreamControlEvent::SilentDisconnect { .. }));
    
    let health = monitor.get_stream_state("alpaca", "TSLA").unwrap();
    assert!(!health.is_healthy);
}

#[test]
fn test_stream_monitor_detects_reconnection() {
    let config = StreamMonitorConfig {
        silence_threshold: Duration::from_millis(50),
        ..Default::default()
    };
    let monitor = StreamMonitor::new(config);
    
    // Initial event
    monitor.process_event("alpaca", "TSLA", 1, Instant::now());
    
    // Wait for disconnect
    std::thread::sleep(Duration::from_millis(100));
    monitor.check_silent_disconnects();
    
    // New event (reconnection)
    let events = monitor.process_event("alpaca", "TSLA", 2, Instant::now());
    
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StreamControlEvent::StreamReconnected { .. }));
    
    let health = monitor.get_stream_state("alpaca", "TSLA").unwrap();
    assert!(health.is_healthy);
}

#[test]
fn test_stream_monitor_detects_duplicate_sequences() {
    let monitor = StreamMonitor::default();
    
    monitor.process_event("alpaca", "SPY", 1, Instant::now());
    monitor.process_event("alpaca", "SPY", 2, Instant::now());
    
    // Duplicate
    let events = monitor.process_event("alpaca", "SPY", 2, Instant::now());
    
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StreamControlEvent::DuplicateSequence { .. }));
    
    if let StreamControlEvent::DuplicateSequence { seq_no, .. } = &events[0] {
        assert_eq!(*seq_no, 2);
    }
}

#[test]
fn test_stream_monitor_detects_sequence_reset() {
    let monitor = StreamMonitor::default();
    
    // High sequence numbers
    monitor.process_event("alpaca", "QQQ", 5000, Instant::now());
    monitor.process_event("alpaca", "QQQ", 5001, Instant::now());
    
    // Reset to low number (indicates stream restart)
    let events = monitor.process_event("alpaca", "QQQ", 1, Instant::now());
    
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StreamControlEvent::SequenceReset { .. }));
}

#[test]
fn test_stream_monitor_high_latency_detection() {
    let config = StreamMonitorConfig {
        latency_threshold: Duration::from_millis(10),
        ..Default::default()
    };
    let monitor = StreamMonitor::new(config);
    
    // First event to establish stream
    monitor.process_event("alpaca", "IWM", 1, Instant::now());
    
    // Wait a bit
    std::thread::sleep(Duration::from_millis(20));
    
    // Event with old timestamp (simulating slow processing)
    let old_ts = Instant::now() - Duration::from_millis(100);
    let events = monitor.process_event("alpaca", "IWM", 2, old_ts);
    
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StreamControlEvent::HighLatency { .. }));
}

#[test]
fn test_stream_monitor_control_event_callback() {
    let monitor = StreamMonitor::default();
    let received_events = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received_events.clone();
    
    monitor.on_control_event(move |event| {
        received_clone.lock().unwrap().push(event);
    });
    
    // Create a gap
    monitor.process_event("alpaca", "AAPL", 1, Instant::now());
    monitor.process_event("alpaca", "AAPL", 3, Instant::now());
    
    let events = received_events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StreamControlEvent::SequenceGap { .. }));
}

#[test]
fn test_stream_monitor_hft_config() {
    let config = StreamMonitorConfig::hft();
    assert_eq!(config.silence_threshold, Duration::from_secs(5));
    assert_eq!(config.latency_threshold, Duration::from_millis(10));
    
    let monitor = StreamMonitor::new(config);
    monitor.process_event("alpaca", "AAPL", 1, Instant::now());
    
    // Wait 20ms - longer than HFT latency threshold
    std::thread::sleep(Duration::from_millis(20));
    
    let old_ts = Instant::now() - Duration::from_millis(50);
    let events = monitor.process_event("alpaca", "AAPL", 2, old_ts);
    
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StreamControlEvent::HighLatency { .. }));
}

#[test]
fn test_stream_monitor_backtest_config() {
    let config = StreamMonitorConfig::backtest();
    assert!(!config.emit_control_events);
    
    let monitor = StreamMonitor::new(config);
    let received = Arc::new(Mutex::new(0));
    let received_clone = received.clone();
    
    monitor.on_control_event(move |_event| {
        *received_clone.lock().unwrap() += 1;
    });
    
    // Create a gap (which would normally emit event)
    monitor.process_event("alpaca", "AAPL", 1, Instant::now());
    monitor.process_event("alpaca", "AAPL", 3, Instant::now());
    
    // No events should be emitted
    assert_eq!(*received.lock().unwrap(), 0);
}

#[test]
fn test_stream_monitor_statistics() {
    let monitor = StreamMonitor::default();
    
    // Create multiple streams with different issues
    monitor.process_event("source1", "SYM1", 1, Instant::now());
    monitor.process_event("source1", "SYM1", 3, Instant::now()); // Gap
    
    monitor.process_event("source2", "SYM2", 1, Instant::now());
    monitor.process_event("source2", "SYM2", 2, Instant::now());
    monitor.process_event("source2", "SYM2", 2, Instant::now()); // Duplicate
    
    monitor.process_event("source3", "SYM3", 1, Instant::now());
    monitor.process_event("source3", "SYM3", 2, Instant::now());
    // No issues
    
    let stats = monitor.get_statistics();
    assert_eq!(stats.total_streams, 3);
    assert_eq!(stats.healthy_streams, 3);
    assert_eq!(stats.total_gaps, 1);
    
    // All streams healthy since no disconnects yet
    assert_eq!(stats.health_percentage(), 100.0);
}

#[test]
fn test_stream_monitor_health_percentage() {
    let monitor = StreamMonitor::default();
    
    // Empty should be 100%
    let stats = monitor.get_statistics();
    assert_eq!(stats.health_percentage(), 100.0);
    
    // Add streams
    monitor.process_event("s1", "A", 1, Instant::now());
    monitor.process_event("s2", "B", 1, Instant::now());
    
    let stats = monitor.get_statistics();
    assert_eq!(stats.health_percentage(), 100.0);
}

#[test]
fn test_stream_monitor_reset() {
    let monitor = StreamMonitor::default();
    
    monitor.process_event("alpaca", "AAPL", 1, Instant::now());
    monitor.process_event("alpaca", "MSFT", 1, Instant::now());
    
    assert_eq!(monitor.get_monitored_streams().len(), 2);
    
    monitor.reset();
    
    assert!(monitor.get_monitored_streams().is_empty());
}

#[test]
fn test_control_event_getters() {
    let gap_event = StreamControlEvent::SequenceGap {
        source: "alpaca".to_string(),
        symbol: "AAPL".to_string(),
        expected_seq: 5,
        actual_seq: 8,
        gap_size: 3,
        timestamp: "2024-01-01T00:00:00Z".to_string(),
    };
    
    assert_eq!(gap_event.event_type(), "sequence_gap");
    assert_eq!(gap_event.source(), "alpaca");
    assert_eq!(gap_event.symbol(), "AAPL");
    
    let disconnect_event = StreamControlEvent::SilentDisconnect {
        source: "polygon".to_string(),
        symbol: "TSLA".to_string(),
        last_event_time: "2024-01-01T00:00:00Z".to_string(),
        silence_duration_ms: 35000,
        threshold_ms: 30000,
    };
    
    assert_eq!(disconnect_event.event_type(), "silent_disconnect");
    assert_eq!(disconnect_event.source(), "polygon");
    assert_eq!(disconnect_event.symbol(), "TSLA");
}

#[test]
fn test_stream_monitor_without_control_events() {
    let config = StreamMonitorConfig::default()
        .without_control_events();
    
    let monitor = StreamMonitor::new(config);
    let received = Arc::new(Mutex::new(0));
    let received_clone = received.clone();
    
    monitor.on_control_event(move |_event| {
        *received_clone.lock().unwrap() += 1;
    });
    
    // Create gap
    monitor.process_event("alpaca", "AAPL", 1, Instant::now());
    let events = monitor.process_event("alpaca", "AAPL", 5, Instant::now());
    
    // Events still returned but not emitted to callback
    assert_eq!(events.len(), 1);
    assert_eq!(*received.lock().unwrap(), 0);
}

#[test]
fn test_stream_monitor_ignores_disabled_checks() {
    let config = StreamMonitorConfig {
        check_sequence_gaps: false,
        check_duplicates: false,
        ..Default::default()
    };
    
    let monitor = StreamMonitor::new(config);
    
    monitor.process_event("alpaca", "AAPL", 1, Instant::now());
    
    // Gap - should not be detected
    let events = monitor.process_event("alpaca", "AAPL", 10, Instant::now());
    assert!(events.is_empty());
    
    // Duplicate - should not be detected
    let events = monitor.process_event("alpaca", "AAPL", 10, Instant::now());
    assert!(events.is_empty());
}

#[tokio::test]
async fn test_stream_monitor_concurrent_access() {
    let monitor = Arc::new(StreamMonitor::default());
    let mut handles = vec![];
    
    // Spawn multiple threads processing events
    for i in 0..5 {
        let monitor_clone = Arc::clone(&monitor);
        let handle = tokio::spawn(async move {
            for j in 0..10 {
                let symbol = format!("SYM{}", i);
                monitor_clone.process_event("alpaca", &symbol, j, Instant::now());
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let stats = monitor.get_statistics();
    assert_eq!(stats.total_streams, 5);
}
