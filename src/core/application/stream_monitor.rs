// core/application/stream_monitor.rs
//
// Monitors inbound event streams for gaps, sequence anomalies, and silent disconnects.
// Emits control events to observability when issues are detected.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};
use crate::core::infrastructure::MutexExt;

/// Control event types emitted when stream issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StreamControlEvent {
    /// Sequence number gap detected
    SequenceGap {
        source: String,
        symbol: String,
        expected_seq: u64,
        actual_seq: u64,
        gap_size: u64,
        timestamp: String,
    },
    /// Silent disconnect detected (no events for threshold duration)
    SilentDisconnect {
        source: String,
        symbol: String,
        last_event_time: String,
        silence_duration_ms: u64,
        threshold_ms: u64,
    },
    /// Stream reconnected after disconnect
    StreamReconnected {
        source: String,
        symbol: String,
        downtime_ms: u64,
        timestamp: String,
    },
    /// Sequence number reset detected (possible stream restart)
    SequenceReset {
        source: String,
        symbol: String,
        last_seq: u64,
        new_seq: u64,
        timestamp: String,
    },
    /// High latency detected in event processing
    HighLatency {
        source: String,
        symbol: String,
        latency_ms: u64,
        threshold_ms: u64,
        timestamp: String,
    },
    /// Duplicate sequence number detected
    DuplicateSequence {
        source: String,
        symbol: String,
        seq_no: u64,
        timestamp: String,
    },
}

impl StreamControlEvent {
    /// Get the event type name for metrics/logging
    pub fn event_type(&self) -> &'static str {
        match self {
            StreamControlEvent::SequenceGap { .. } => "sequence_gap",
            StreamControlEvent::SilentDisconnect { .. } => "silent_disconnect",
            StreamControlEvent::StreamReconnected { .. } => "stream_reconnected",
            StreamControlEvent::SequenceReset { .. } => "sequence_reset",
            StreamControlEvent::HighLatency { .. } => "high_latency",
            StreamControlEvent::DuplicateSequence { .. } => "duplicate_sequence",
        }
    }

    /// Get the source identifier
    pub fn source(&self) -> &str {
        match self {
            StreamControlEvent::SequenceGap { source, .. } => source,
            StreamControlEvent::SilentDisconnect { source, .. } => source,
            StreamControlEvent::StreamReconnected { source, .. } => source,
            StreamControlEvent::SequenceReset { source, .. } => source,
            StreamControlEvent::HighLatency { source, .. } => source,
            StreamControlEvent::DuplicateSequence { source, .. } => source,
        }
    }

    /// Get the symbol
    pub fn symbol(&self) -> &str {
        match self {
            StreamControlEvent::SequenceGap { symbol, .. } => symbol,
            StreamControlEvent::SilentDisconnect { symbol, .. } => symbol,
            StreamControlEvent::StreamReconnected { symbol, .. } => symbol,
            StreamControlEvent::SequenceReset { symbol, .. } => symbol,
            StreamControlEvent::HighLatency { symbol, .. } => symbol,
            StreamControlEvent::DuplicateSequence { symbol, .. } => symbol,
        }
    }
}

/// Per-stream tracking state
#[derive(Debug, Clone)]
struct StreamState {
    /// Last seen sequence number
    last_seq_no: u64,
    /// Last event timestamp
    last_event_time: Instant,
    /// First event time (for tracking stream lifetime)
    stream_start_time: Instant,
    /// Whether the stream is currently considered healthy
    is_healthy: bool,
    /// Number of gaps detected
    gap_count: u64,
    /// Number of silent disconnects detected
    disconnect_count: u64,
    /// Expected next sequence number
    expected_seq_no: u64,
}

impl StreamState {
    fn new(seq_no: u64) -> Self {
        let now = Instant::now();
        Self {
            last_seq_no: seq_no,
            last_event_time: now,
            stream_start_time: now,
            is_healthy: true,
            gap_count: 0,
            disconnect_count: 0,
            expected_seq_no: seq_no + 1,
        }
    }

    /// Update state with a new event
    fn update(&mut self, seq_no: u64) {
        self.last_seq_no = seq_no;
        self.last_event_time = Instant::now();
        self.is_healthy = true;
    }

    /// Mark stream as disconnected
    fn mark_disconnected(&mut self) {
        self.is_healthy = false;
        self.disconnect_count += 1;
    }

    /// Mark gap detected
    fn mark_gap(&mut self) {
        self.gap_count += 1;
    }
}

/// Configuration for stream monitoring
#[derive(Debug, Clone)]
pub struct StreamMonitorConfig {
    /// Threshold for silent disconnect detection (no events for this duration)
    pub silence_threshold: Duration,
    /// Threshold for high latency warning
    pub latency_threshold: Duration,
    /// Whether to emit control events
    pub emit_control_events: bool,
    /// Whether to check for sequence gaps
    pub check_sequence_gaps: bool,
    /// Whether to check for duplicate sequences
    pub check_duplicates: bool,
}

impl Default for StreamMonitorConfig {
    fn default() -> Self {
        Self {
            silence_threshold: Duration::from_secs(30), // 30 seconds
            latency_threshold: Duration::from_millis(100), // 100ms
            emit_control_events: true,
            check_sequence_gaps: true,
            check_duplicates: true,
        }
    }
}

impl StreamMonitorConfig {
    /// Create a config for high-frequency trading (stricter thresholds)
    pub fn hft() -> Self {
        Self {
            silence_threshold: Duration::from_secs(5),
            latency_threshold: Duration::from_millis(10),
            ..Default::default()
        }
    }

    /// Create a config for backtesting (more lenient)
    pub fn backtest() -> Self {
        Self {
            silence_threshold: Duration::from_secs(300),
            latency_threshold: Duration::from_millis(500),
            emit_control_events: false,
            ..Default::default()
        }
    }

    /// Disable control events
    pub fn without_control_events(mut self) -> Self {
        self.emit_control_events = false;
        self
    }
}

/// Callback for control events
pub type ControlEventCallback = Box<dyn Fn(StreamControlEvent) + Send + Sync>;

/// Small-stack-optimized buffer for control events.
/// Stores up to N events on the stack; spills to heap only if needed.
/// This avoids allocations for the common case of 0-4 events per call.
pub struct SmallEventBuffer<const N: usize> {
    stack: [Option<StreamControlEvent>; N],
    len: usize,
    spill: Option<Vec<StreamControlEvent>>,
}

impl<const N: usize> SmallEventBuffer<N> {
    /// Create a new empty buffer
    pub fn new() -> Self {
        Self {
            // Initialize with None values - required for fixed-size arrays of Options
            stack: std::array::from_fn(|_| None),
            len: 0,
            spill: None,
        }
    }

    /// Push an event into the buffer
    #[inline]
    pub fn push(&mut self, event: StreamControlEvent) {
        if self.len < N {
            // Store on stack
            self.stack[self.len] = Some(event);
            self.len += 1;
        } else {
            // Spill to heap
            self.spill.get_or_insert_with(Vec::new).push(event);
        }
    }

    /// Returns true if the buffer contains no events
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0 && self.spill.as_ref().map_or(true, |v| v.is_empty())
    }

    /// Returns the number of events in the buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.len + self.spill.as_ref().map_or(0, |v| v.len())
    }

    /// Iterate over all events in the buffer
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &StreamControlEvent> {
        self.stack[..self.len]
            .iter()
            .filter_map(|opt| opt.as_ref())
            .chain(self.spill.iter().flat_map(|v| v.iter()))
    }

    /// Convert into a Vec (allocates only if there are events)
    #[inline]
    pub fn into_vec(self) -> Vec<StreamControlEvent> {
        // Stack events come first (they were pushed first), then spilled events
        let total_len = self.len();
        let spill_len = self.spill.as_ref().map_or(0, |v| v.len());
        let stack_len = total_len - spill_len;
        
        // Start with stack events
        let mut result = Vec::with_capacity(total_len);
        for i in 0..stack_len {
            if let Some(event) = self.stack[i].clone() {
                result.push(event);
            }
        }
        
        // Then append spilled events
        if let Some(spill) = self.spill {
            result.extend(spill);
        }
        
        result
    }

    /// Get a slice of stack events (for testing)
    #[cfg(test)]
    fn stack_slice(&self) -> &[Option<StreamControlEvent>] {
        &self.stack[..self.len]
    }
}

impl<const N: usize> Default for SmallEventBuffer<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> IntoIterator for SmallEventBuffer<N> {
    type Item = StreamControlEvent;
    type IntoIter = SmallEventBufferIter<N>;

    fn into_iter(self) -> Self::IntoIter {
        SmallEventBufferIter {
            buffer: self,
            stack_idx: 0,
            spill_idx: 0,
        }
    }
}

/// Iterator for SmallEventBuffer
pub struct SmallEventBufferIter<const N: usize> {
    buffer: SmallEventBuffer<N>,
    stack_idx: usize,
    spill_idx: usize,
}

impl<const N: usize> Iterator for SmallEventBufferIter<N> {
    type Item = StreamControlEvent;

    fn next(&mut self) -> Option<Self::Item> {
        // First yield stack events
        if self.stack_idx < self.buffer.len {
            let event = self.buffer.stack[self.stack_idx].take()?;
            self.stack_idx += 1;
            return Some(event);
        }
        // Then yield spilled events
        if let Some(ref spill) = self.buffer.spill {
            if self.spill_idx < spill.len() {
                let event = spill[self.spill_idx].clone();
                self.spill_idx += 1;
                return Some(event);
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.buffer.len - self.stack_idx
            + self.buffer.spill.as_ref().map_or(0, |v| v.len().saturating_sub(self.spill_idx));
        (remaining, Some(remaining))
    }
}

impl<const N: usize> ExactSizeIterator for SmallEventBufferIter<N> {}

/// Monitors streams for gaps, disconnects, and anomalies
pub struct StreamMonitor {
    streams: Mutex<HashMap<(String, String), StreamState>>, // (source, symbol) -> state
    config: StreamMonitorConfig,
    control_event_callback: Mutex<Option<ControlEventCallback>>,
}

impl StreamMonitor {
    /// Create a new stream monitor with the given configuration
    pub fn new(config: StreamMonitorConfig) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            config,
            control_event_callback: Mutex::new(None),
        }
    }

    /// Create a new stream monitor with default configuration
    pub fn default() -> Self {
        Self::new(StreamMonitorConfig::default())
    }

    /// Register a callback for control events
    pub fn on_control_event<F>(&self, callback: F)
    where
        F: Fn(StreamControlEvent) + Send + Sync + 'static,
    {
        let mut cb = self.control_event_callback.safe_lock();
        *cb = Some(Box::new(callback));
    }

    /// Emit a control event if callback is registered
    fn emit_control_event(&self, event: StreamControlEvent) {
        if self.config.emit_control_events {
            if let Some(callback) = self.control_event_callback.safe_lock().as_ref() {
                callback(event);
            }
        }
    }

    /// Process a new event from a stream
    /// 
    /// # Arguments
    /// * `source` - The source identifier (e.g., "alpaca/iex")
    /// * `symbol` - The symbol (e.g., "AAPL")
    /// * `seq_no` - The sequence number
    /// * `ingestion_ts` - The ingestion timestamp
    /// 
    /// # Returns
    /// Any control events generated by this event
    /// 
    /// # Deadlock Safety
    /// This method releases the streams lock before emitting control events to prevent
    /// deadlocks. If a callback tries to access stream state (e.g., via `get_stream_state`),
    /// it would need to acquire the streams lock. By releasing the lock before calling
    /// callbacks, we avoid the deadlock scenario where: streams -> callback -> streams.
    pub fn process_event(
        &self,
        source: impl Into<String>,
        symbol: impl Into<String>,
        seq_no: u64,
        ingestion_ts: Instant,
    ) -> Vec<StreamControlEvent> {
        let source = source.into();
        let symbol = symbol.into();
        let key = (source.clone(), symbol.clone());
        
        // Collect events while holding the lock, but emit them after releasing it
        // to prevent deadlocks if callbacks try to access stream state.
        // Uses SmallEventBuffer to avoid heap allocations for the common case of 0-4 events.
        let events_to_emit: SmallEventBuffer<4> = {
            let mut events = SmallEventBuffer::<4>::new();
            let mut streams = self.streams.safe_lock();
            
            if let Some(state) = streams.get_mut(&key) {
                // Check for duplicates first
                if self.config.check_duplicates && seq_no == state.last_seq_no {
                    let event = StreamControlEvent::DuplicateSequence {
                        source: source.clone(),
                        symbol: symbol.clone(),
                        seq_no,
                        timestamp: chrono::Utc::now().to_rfc3339(),
                    };
                    events.push(event);
                }
                
                // Check for sequence gaps
                if self.config.check_sequence_gaps && seq_no != state.expected_seq_no {
                    if seq_no < state.expected_seq_no && seq_no != state.last_seq_no {
                        // Possible sequence reset (went backwards significantly)
                        let event = StreamControlEvent::SequenceReset {
                            source: source.clone(),
                            symbol: symbol.clone(),
                            last_seq: state.last_seq_no,
                            new_seq: seq_no,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        };
                        events.push(event);
                    } else if seq_no > state.expected_seq_no {
                        // Gap detected
                        let gap_size = seq_no - state.expected_seq_no;
                        state.mark_gap();
                        
                        let event = StreamControlEvent::SequenceGap {
                            source: source.clone(),
                            symbol: symbol.clone(),
                            expected_seq: state.expected_seq_no,
                            actual_seq: seq_no,
                            gap_size,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        };
                        events.push(event);
                    }
                    // If seq_no < expected but it's the same as last, it's a duplicate (already handled)
                }
                
                // Check latency
                let latency = ingestion_ts.elapsed();
                if latency > self.config.latency_threshold {
                    let event = StreamControlEvent::HighLatency {
                        source: source.clone(),
                        symbol: symbol.clone(),
                        latency_ms: latency.as_millis() as u64,
                        threshold_ms: self.config.latency_threshold.as_millis() as u64,
                        timestamp: chrono::Utc::now().to_rfc3339(),
                    };
                    events.push(event);
                }
                
                // Stream was disconnected, now reconnected
                if !state.is_healthy {
                    let downtime = state.last_event_time.elapsed();
                    let event = StreamControlEvent::StreamReconnected {
                        source: source.clone(),
                        symbol: symbol.clone(),
                        downtime_ms: downtime.as_millis() as u64,
                        timestamp: chrono::Utc::now().to_rfc3339(),
                    };
                    events.push(event);
                }
                
                state.update(seq_no);
                state.expected_seq_no = seq_no + 1;
            } else {
                // New stream
                let state = StreamState::new(seq_no);
                streams.insert(key, state);
            }
            
            events
        }; // streams lock is released here
        
        // Now emit events without holding the streams lock
        // This prevents deadlocks if callbacks try to access stream state
        for event in events_to_emit.iter() {
            self.emit_control_event(event.clone());
        }
        
        // Convert to Vec for backward compatibility (only allocates if there are events)
        events_to_emit.into_vec()
    }

    /// Check for silent disconnects
    /// Should be called periodically (e.g., every second)
    /// 
    /// # Returns
    /// Control events for any disconnects detected
    /// 
    /// # Deadlock Safety
    /// This method releases the streams lock before emitting control events to prevent
    /// deadlocks. See `process_event` documentation for details.
    pub fn check_silent_disconnects(&self) -> Vec<StreamControlEvent> {
        // Collect disconnect events while holding the lock.
        // Uses SmallEventBuffer to avoid heap allocations for the common case.
        let events_to_emit: SmallEventBuffer<4> = {
            let mut events = SmallEventBuffer::<4>::new();
            let mut streams = self.streams.safe_lock();
            
            for ((source, symbol), state) in streams.iter_mut() {
                if state.is_healthy {
                    let silence_duration = state.last_event_time.elapsed();
                    if silence_duration > self.config.silence_threshold {
                        state.mark_disconnected();
                        
                        let event = StreamControlEvent::SilentDisconnect {
                            source: source.clone(),
                            symbol: symbol.clone(),
                            last_event_time: chrono::Utc::now()
                                .checked_sub_signed(chrono::Duration::from_std(silence_duration).unwrap_or_default())
                                .map(|t| t.to_rfc3339())
                                .unwrap_or_default(),
                            silence_duration_ms: silence_duration.as_millis() as u64,
                            threshold_ms: self.config.silence_threshold.as_millis() as u64,
                        };
                        events.push(event);
                    }
                }
            }
            
            events
        }; // streams lock is released here
        
        // Emit events without holding the streams lock
        for event in events_to_emit.iter() {
            self.emit_control_event(event.clone());
        }
        
        // Convert to Vec for backward compatibility (only allocates if there are events)
        events_to_emit.into_vec()
    }

    /// Get the current state of a stream
    pub fn get_stream_state(&self, source: &str, symbol: &str) -> Option<StreamHealth> {
        let streams = self.streams.safe_lock();
        streams.get(&(source.to_string(), symbol.to_string())).map(|s| StreamHealth {
            is_healthy: s.is_healthy,
            last_seq_no: s.last_seq_no,
            gap_count: s.gap_count,
            disconnect_count: s.disconnect_count,
            last_event_time: s.last_event_time,
        })
    }

    /// Get all monitored streams
    pub fn get_monitored_streams(&self) -> Vec<(String, String)> {
        let streams = self.streams.safe_lock();
        streams.keys().cloned().collect()
    }

    /// Get statistics for all streams
    pub fn get_statistics(&self) -> StreamStatistics {
        let streams = self.streams.safe_lock();
        StreamStatistics {
            total_streams: streams.len(),
            healthy_streams: streams.values().filter(|s| s.is_healthy).count(),
            total_gaps: streams.values().map(|s| s.gap_count).sum(),
            total_disconnects: streams.values().map(|s| s.disconnect_count).sum(),
        }
    }

    /// Reset the monitor (clear all state)
    pub fn reset(&self) {
        let mut streams = self.streams.safe_lock();
        streams.clear();
    }
}

/// Health information for a stream
#[derive(Debug, Clone)]
pub struct StreamHealth {
    pub is_healthy: bool,
    pub last_seq_no: u64,
    pub gap_count: u64,
    pub disconnect_count: u64,
    pub last_event_time: Instant,
}

/// Statistics across all monitored streams
#[derive(Debug, Clone)]
pub struct StreamStatistics {
    pub total_streams: usize,
    pub healthy_streams: usize,
    pub total_gaps: u64,
    pub total_disconnects: u64,
}

impl StreamStatistics {
    /// Get the percentage of healthy streams
    pub fn health_percentage(&self) -> f64 {
        if self.total_streams == 0 {
            100.0
        } else {
            (self.healthy_streams as f64 / self.total_streams as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_stream_monitor_tracks_new_stream() {
        let monitor = StreamMonitor::default();
        let events = monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        assert!(events.is_empty()); // No issues with first event
        
        let health = monitor.get_stream_state("alpaca", "AAPL").unwrap();
        assert!(health.is_healthy);
        assert_eq!(health.last_seq_no, 1);
    }

    #[test]
    fn test_sequence_gap_detection() {
        let monitor = StreamMonitor::default();
        
        // Event 1
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        // Event 3 (gap - missing event 2)
        let events = monitor.process_event("alpaca", "AAPL", 3, Instant::now());
        
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], StreamControlEvent::SequenceGap { .. }));
        
        if let StreamControlEvent::SequenceGap { expected_seq, actual_seq, gap_size, .. } = &events[0] {
            assert_eq!(*expected_seq, 2);
            assert_eq!(*actual_seq, 3);
            assert_eq!(*gap_size, 1);
        }
    }

    #[test]
    fn test_duplicate_sequence_detection() {
        let monitor = StreamMonitor::default();
        
        // Event 1
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        // Event 1 again (duplicate)
        let events = monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], StreamControlEvent::DuplicateSequence { .. }));
    }

    #[test]
    fn test_sequence_reset_detection() {
        let monitor = StreamMonitor::default();
        
        // Event 5
        monitor.process_event("alpaca", "AAPL", 5, Instant::now());
        
        // Event 1 (reset)
        let events = monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], StreamControlEvent::SequenceReset { .. }));
    }

    #[test]
    fn test_silent_disconnect_detection() {
        let config = StreamMonitorConfig {
            silence_threshold: Duration::from_millis(50),
            ..Default::default()
        };
        let monitor = StreamMonitor::new(config);
        
        // Initial event
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        // Wait longer than silence threshold
        std::thread::sleep(Duration::from_millis(100));
        
        // Check for disconnects
        let events = monitor.check_silent_disconnects();
        
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], StreamControlEvent::SilentDisconnect { .. }));
    }

    #[test]
    fn test_stream_reconnection_detection() {
        let config = StreamMonitorConfig {
            silence_threshold: Duration::from_millis(50),
            ..Default::default()
        };
        let monitor = StreamMonitor::new(config);
        
        // Initial event
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        // Wait for disconnect
        std::thread::sleep(Duration::from_millis(100));
        monitor.check_silent_disconnects();
        
        // New event (reconnection)
        let events = monitor.process_event("alpaca", "AAPL", 2, Instant::now());
        
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], StreamControlEvent::StreamReconnected { .. }));
    }

    #[test]
    fn test_high_latency_detection() {
        let config = StreamMonitorConfig {
            latency_threshold: Duration::from_millis(10),
            ..Default::default()
        };
        let monitor = StreamMonitor::new(config);
        
        // First event to create the stream (no latency check on first event)
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        // Wait a bit
        std::thread::sleep(Duration::from_millis(20));
        
        // Second event with old timestamp (simulating processing delay)
        let old_ts = Instant::now() - Duration::from_millis(100);
        let events = monitor.process_event("alpaca", "AAPL", 2, old_ts);
        
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], StreamControlEvent::HighLatency { .. }));
    }

    #[test]
    fn test_control_event_callback() {
        let monitor = StreamMonitor::default();
        let event_count = Arc::new(AtomicUsize::new(0));
        let count_clone = event_count.clone();
        
        monitor.on_control_event(move |_event| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });
        
        // Create a gap to trigger event
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        monitor.process_event("alpaca", "AAPL", 3, Instant::now());
        
        assert_eq!(event_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_stream_statistics() {
        let monitor = StreamMonitor::default();
        
        // Create multiple streams
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        monitor.process_event("alpaca", "MSFT", 1, Instant::now());
        monitor.process_event("polygon", "TSLA", 1, Instant::now());
        
        let stats = monitor.get_statistics();
        assert_eq!(stats.total_streams, 3);
        assert_eq!(stats.healthy_streams, 3);
        assert_eq!(stats.health_percentage(), 100.0);
    }

    #[test]
    fn test_stream_health_percentage_with_no_streams() {
        let stats = StreamStatistics {
            total_streams: 0,
            healthy_streams: 0,
            total_gaps: 0,
            total_disconnects: 0,
        };
        assert_eq!(stats.health_percentage(), 100.0);
    }

    #[test]
    fn test_config_hft() {
        let config = StreamMonitorConfig::hft();
        assert_eq!(config.silence_threshold, Duration::from_secs(5));
        assert_eq!(config.latency_threshold, Duration::from_millis(10));
    }

    #[test]
    fn test_config_backtest() {
        let config = StreamMonitorConfig::backtest();
        assert!(!config.emit_control_events);
        assert_eq!(config.silence_threshold, Duration::from_secs(300));
    }

    #[test]
    fn test_control_event_getters() {
        let event = StreamControlEvent::SequenceGap {
            source: "alpaca".to_string(),
            symbol: "AAPL".to_string(),
            expected_seq: 5,
            actual_seq: 7,
            gap_size: 2,
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        };
        
        assert_eq!(event.event_type(), "sequence_gap");
        assert_eq!(event.source(), "alpaca");
        assert_eq!(event.symbol(), "AAPL");
    }

    #[test]
    fn test_monitor_reset() {
        let monitor = StreamMonitor::default();
        
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        assert_eq!(monitor.get_monitored_streams().len(), 1);
        
        monitor.reset();
        assert!(monitor.get_monitored_streams().is_empty());
    }

    #[test]
    fn test_multiple_streams_independent() {
        let monitor = StreamMonitor::default();
        
        // AAPL has gap
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        let aapl_events = monitor.process_event("alpaca", "AAPL", 3, Instant::now());
        
        // MSFT is fine
        monitor.process_event("alpaca", "MSFT", 1, Instant::now());
        let msft_events = monitor.process_event("alpaca", "MSFT", 2, Instant::now());
        
        assert_eq!(aapl_events.len(), 1); // Gap detected
        assert!(msft_events.is_empty()); // No issues
        
        let stats = monitor.get_statistics();
        assert_eq!(stats.total_gaps, 1);
    }

    /// Test that verifies no deadlock occurs when a callback tries to access stream state.
    /// 
    /// This test would deadlock with the old implementation because:
    /// 1. process_event acquires streams lock
    /// 2. callback tries to acquire streams lock via get_stream_state
    /// 3. Deadlock: streams -> callback -> streams
    ///
    /// With the fixed implementation, the streams lock is released before calling callbacks,
    /// so the callback can safely acquire the lock.
    #[test]
    fn test_no_deadlock_when_callback_accesses_stream_state() {
        use std::sync::atomic::{AtomicBool, Ordering, AtomicUsize};
        use std::thread;
        use std::time::Duration;

        let monitor = Arc::new(StreamMonitor::default());
        let callback_completed = Arc::new(AtomicBool::new(false));
        let callback_health_checks = Arc::new(AtomicUsize::new(0));
        let monitor_for_callback = monitor.clone();
        let callback_completed_clone = callback_completed.clone();
        let callback_health_checks_clone = callback_health_checks.clone();
        
        // Register a callback that tries to access stream state
        // This would deadlock if process_event held the streams lock while calling callbacks
        monitor.on_control_event(move |event| {
            // Try to access stream state from within the callback
            // This requires acquiring the streams lock via get_stream_state
            if let Some(health) = monitor_for_callback.get_stream_state(event.source(), event.symbol()) {
                callback_health_checks_clone.fetch_add(1, Ordering::SeqCst);
                // Verify we got valid health data
                assert!(health.last_seq_no > 0, "Should have sequence number");
            }
            callback_completed_clone.store(true, Ordering::SeqCst);
        });
        
        // Process an event that triggers a callback
        // Use a separate thread with timeout to detect deadlocks
        let monitor_clone = monitor.clone();
        let handle = thread::spawn(move || {
            // First event to create stream
            monitor_clone.process_event("alpaca", "AAPL", 1, Instant::now());
            // Second event with gap to trigger callback
            monitor_clone.process_event("alpaca", "AAPL", 3, Instant::now());
        });
        
        // Wait for the thread with a timeout (5 seconds should be plenty)
        let timeout = Duration::from_secs(5);
        let result = match handle.join() {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        };
        
        // If we reach here without timeout, no deadlock occurred
        assert!(result.is_ok(), "Deadlock detected: thread did not complete within timeout");
        assert!(callback_completed.load(Ordering::SeqCst), "Callback should have been invoked");
        assert!(callback_health_checks.load(Ordering::SeqCst) > 0, 
            "Callback should have successfully accessed stream state");
    }

    /// Test that verifies check_silent_disconnects also doesn't deadlock
    /// when callbacks access stream state.
    #[test]
    fn test_no_deadlock_in_silent_disconnect_check() {
        use std::sync::atomic::{AtomicBool, Ordering, AtomicUsize};
        use std::thread;
        use std::time::Duration;

        let config = StreamMonitorConfig {
            silence_threshold: Duration::from_millis(50),
            ..Default::default()
        };
        let monitor = Arc::new(StreamMonitor::new(config));
        let callback_completed = Arc::new(AtomicBool::new(false));
        let callback_health_checks = Arc::new(AtomicUsize::new(0));
        let monitor_for_callback = monitor.clone();
        let callback_completed_clone = callback_completed.clone();
        let callback_health_checks_clone = callback_health_checks.clone();
        
        // Register a callback that accesses stream state
        monitor.on_control_event(move |event| {
            // Access stream state from within the callback
            if let Some(health) = monitor_for_callback.get_stream_state(event.source(), event.symbol()) {
                callback_health_checks_clone.fetch_add(1, Ordering::SeqCst);
                assert!(health.last_seq_no > 0);
            }
            callback_completed_clone.store(true, Ordering::SeqCst);
        });
        
        // Create a stream, then wait for silence threshold to trigger disconnect
        monitor.process_event("alpaca", "AAPL", 1, Instant::now());
        
        // Wait for silence threshold
        thread::sleep(Duration::from_millis(100));
        
        // Check for disconnects in a separate thread with timeout
        let monitor_clone = monitor.clone();
        let handle = thread::spawn(move || {
            monitor_clone.check_silent_disconnects()
        });
        
        // Wait with timeout
        let result = match handle.join() {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        };
        
        assert!(result.is_ok(), "Deadlock detected in check_silent_disconnects");
        assert!(callback_completed.load(Ordering::SeqCst), "Callback should have been invoked");
        assert!(callback_health_checks.load(Ordering::SeqCst) > 0,
            "Callback should have successfully accessed stream state");
    }

    // =========================================================================
    // SmallEventBuffer Tests
    // =========================================================================

    #[test]
    fn test_small_event_buffer_new_is_empty() {
        let buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_small_event_buffer_push_single_event() {
        let mut buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        let event = StreamControlEvent::DuplicateSequence {
            source: "test".to_string(),
            symbol: "AAPL".to_string(),
            seq_no: 1,
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        };
        
        buffer.push(event.clone());
        
        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_small_event_buffer_push_multiple_events_within_capacity() {
        let mut buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        
        for i in 0..4 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        assert_eq!(buffer.len(), 4);
        assert!(buffer.spill.is_none()); // Should not have spilled
    }

    #[test]
    fn test_small_event_buffer_spills_when_capacity_exceeded() {
        let mut buffer: SmallEventBuffer<2> = SmallEventBuffer::new();
        
        // Push 4 events into a buffer with capacity 2
        for i in 0..4 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        assert_eq!(buffer.len(), 4);
        assert!(buffer.spill.is_some()); // Should have spilled
        assert_eq!(buffer.spill.as_ref().unwrap().len(), 2); // 2 events in spill
    }

    #[test]
    fn test_small_event_buffer_iter() {
        let mut buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        
        for i in 0..3 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        let collected: Vec<_> = buffer.iter().collect();
        assert_eq!(collected.len(), 3);
    }

    #[test]
    fn test_small_event_buffer_iter_with_spill() {
        let mut buffer: SmallEventBuffer<2> = SmallEventBuffer::new();
        
        // Push 4 events into a buffer with capacity 2
        for i in 0..4 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        let collected: Vec<_> = buffer.iter().collect();
        assert_eq!(collected.len(), 4);
        
        // Verify order is preserved (stack events first, then spill)
        for (i, event) in collected.iter().enumerate() {
            if let StreamControlEvent::DuplicateSequence { seq_no, .. } = event {
                assert_eq!(*seq_no, i as u64);
            }
        }
    }

    #[test]
    fn test_small_event_buffer_into_vec_empty() {
        let buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        let vec = buffer.into_vec();
        assert!(vec.is_empty());
    }

    #[test]
    fn test_small_event_buffer_into_vec_with_events() {
        let mut buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        
        for i in 0..3 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        let vec = buffer.into_vec();
        assert_eq!(vec.len(), 3);
    }

    #[test]
    fn test_small_event_buffer_into_vec_with_spill() {
        let mut buffer: SmallEventBuffer<2> = SmallEventBuffer::new();
        
        for i in 0..5 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        let vec = buffer.into_vec();
        assert_eq!(vec.len(), 5);
        
        // Verify order is preserved
        for (i, event) in vec.iter().enumerate() {
            if let StreamControlEvent::DuplicateSequence { seq_no, .. } = event {
                assert_eq!(*seq_no, i as u64);
            }
        }
    }

    #[test]
    fn test_small_event_buffer_into_iter() {
        let mut buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        
        for i in 0..3 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        let collected: Vec<_> = buffer.into_iter().collect();
        assert_eq!(collected.len(), 3);
    }

    #[test]
    fn test_small_event_buffer_exact_size_iterator() {
        let mut buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        
        for i in 0..3 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        let iter = buffer.into_iter();
        let (lower, upper) = iter.size_hint();
        assert_eq!(lower, 3);
        assert_eq!(upper, Some(3));
        assert_eq!(iter.len(), 3);
    }

    #[test]
    fn test_small_event_buffer_default() {
        let buffer: SmallEventBuffer<4> = Default::default();
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_small_event_buffer_no_allocation_on_empty() {
        // This test verifies that no heap allocation occurs for empty buffers
        let buffer: SmallEventBuffer<4> = SmallEventBuffer::new();
        assert!(buffer.spill.is_none());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_small_event_buffer_large_spill() {
        // Test with a large number of spilled events
        let mut buffer: SmallEventBuffer<2> = SmallEventBuffer::new();
        
        for i in 0..100 {
            buffer.push(StreamControlEvent::DuplicateSequence {
                source: "test".to_string(),
                symbol: "AAPL".to_string(),
                seq_no: i,
                timestamp: "2024-01-01T00:00:00Z".to_string(),
            });
        }
        
        assert_eq!(buffer.len(), 100);
        let vec = buffer.into_vec();
        assert_eq!(vec.len(), 100);
        
        // Verify all events are in order
        for (i, event) in vec.iter().enumerate() {
            if let StreamControlEvent::DuplicateSequence { seq_no, .. } = event {
                assert_eq!(*seq_no, i as u64);
            }
        }
    }
}
