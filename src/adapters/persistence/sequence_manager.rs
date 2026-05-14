// adapters/persistence/sequence_manager.rs
//
// Manages sequence numbers for gap detection and ordering guarantees.
// Tracks per-stream sequence numbers and detects anomalies.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Errors that can occur in sequence management
#[derive(Debug, Clone, PartialEq)]
pub enum SequenceError {
    /// Sequence number is older than expected (possible replay)
    OutOfOrder { expected: u64, received: u64 },
    /// Sequence number is a duplicate
    Duplicate { seq_no: u64 },
    /// Sequence gap detected
    Gap { expected: u64, received: u64, gap_size: u64 },
    /// Sequence reset detected (stream restart)
    Reset { last_seq: u64, new_seq: u64 },
}

impl std::fmt::Display for SequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequenceError::OutOfOrder { expected, received } => {
                write!(f, "Out of order sequence: expected {}, received {}", expected, received)
            }
            SequenceError::Duplicate { seq_no } => {
                write!(f, "Duplicate sequence number: {}", seq_no)
            }
            SequenceError::Gap { expected, received, gap_size } => {
                write!(f, "Sequence gap: expected {}, received {}, gap_size {}", expected, received, gap_size)
            }
            SequenceError::Reset { last_seq, new_seq } => {
                write!(f, "Sequence reset: last {}, new {}", last_seq, new_seq)
            }
        }
    }
}

impl std::error::Error for SequenceError {}

/// Result of validating a sequence number
#[derive(Debug, Clone, PartialEq)]
pub enum SequenceValidation {
    /// Sequence is valid and expected
    Valid,
    /// Sequence gap detected
    Gap { missing: Vec<u64> },
    /// Sequence is a duplicate
    Duplicate,
    /// Sequence indicates stream reset
    Reset,
}

/// Per-stream sequence tracking
#[derive(Debug, Clone)]
struct StreamSequences {
    /// The last seen sequence number
    last_seq: u64,
    /// The expected next sequence number
    expected_seq: u64,
    /// Total gaps detected for this stream
    gap_count: u64,
    /// Total duplicates detected
    duplicate_count: u64,
    /// All seen sequence numbers (for duplicate detection, limited window)
    seen_sequences: VecDeque<u64>,
    /// Maximum window size for duplicate detection
    window_size: usize,
}

impl StreamSequences {
    fn new(initial_seq: u64, window_size: usize) -> Self {
        let mut seen_sequences = VecDeque::with_capacity(window_size);
        seen_sequences.push_back(initial_seq);
        Self {
            last_seq: initial_seq,
            expected_seq: initial_seq + 1,
            gap_count: 0,
            duplicate_count: 0,
            seen_sequences,
            window_size,
        }
    }

    /// Validate and process a new sequence number
    fn process(&mut self, seq_no: u64) -> SequenceValidation {
        // Check for duplicate within window
        if self.seen_sequences.contains(&seq_no) {
            self.duplicate_count += 1;
            return SequenceValidation::Duplicate;
        }

        // Check for reset (sequence went backwards significantly)
        if seq_no < self.last_seq && self.last_seq - seq_no > 1000 {
            self.reset(seq_no);
            return SequenceValidation::Reset;
        }

        // Check for gap
        if seq_no > self.expected_seq {
            let gap_size = seq_no - self.expected_seq;
            self.gap_count += 1;
            let missing: Vec<u64> = (self.expected_seq..seq_no).collect();
            
            self.last_seq = seq_no;
            self.expected_seq = seq_no + 1;
            self.add_to_window(seq_no);
            
            return SequenceValidation::Gap { missing };
        }

        // Check for out of order (shouldn't happen in normal flow)
        if seq_no < self.expected_seq && seq_no != self.last_seq {
            // Still add to window as we've seen it
            self.add_to_window(seq_no);
            return SequenceValidation::Valid;
        }

        // Normal progression
        if seq_no == self.expected_seq {
            self.last_seq = seq_no;
            self.expected_seq = seq_no + 1;
            self.add_to_window(seq_no);
            return SequenceValidation::Valid;
        }

        // Fallback
        self.add_to_window(seq_no);
        SequenceValidation::Valid
    }

    /// Reset sequence tracking
    fn reset(&mut self, new_seq: u64) {
        self.last_seq = new_seq;
        self.expected_seq = new_seq + 1;
        self.seen_sequences.clear();
        self.seen_sequences.push_back(new_seq);
    }

    /// Add sequence to window, maintaining size limit.
    /// Uses VecDeque for O(1) push_back and pop_front operations.
    fn add_to_window(&mut self, seq_no: u64) {
        self.seen_sequences.push_back(seq_no);
        if self.seen_sequences.len() > self.window_size {
            self.seen_sequences.pop_front();
        }
    }

    /// Get the next expected sequence number
    fn next_expected(&self) -> u64 {
        self.expected_seq
    }

    /// Get statistics
    fn stats(&self) -> StreamStats {
        StreamStats {
            last_seq: self.last_seq,
            expected_seq: self.expected_seq,
            gap_count: self.gap_count,
            duplicate_count: self.duplicate_count,
        }
    }
}

/// Statistics for a single stream
#[derive(Debug, Clone)]
pub struct StreamStats {
    pub last_seq: u64,
    pub expected_seq: u64,
    pub gap_count: u64,
    pub duplicate_count: u64,
}

/// Manages sequence numbers across multiple streams
pub struct SequenceManager {
    streams: Mutex<HashMap<String, StreamSequences>>,
    window_size: usize,
}

impl SequenceManager {
    /// Create a new sequence manager with default window size
    pub fn new() -> Self {
        Self::with_window_size(1000)
    }

    /// Create a new sequence manager with specified window size
    pub fn with_window_size(window_size: usize) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            window_size,
        }
    }

    /// Process a sequence number for a stream
    /// 
    /// # Arguments
    /// * `stream_id` - Unique identifier for the stream (e.g., "alpaca/AAPL")
    /// * `seq_no` - The sequence number to process
    /// 
    /// # Returns
    /// Validation result indicating if the sequence is valid, a gap, duplicate, etc.
    pub fn process(&self, stream_id: impl Into<String>, seq_no: u64) -> SequenceValidation {
        let stream_id = stream_id.into();
        let mut streams = self.streams.lock().unwrap();
        
        if let Some(stream) = streams.get_mut(&stream_id) {
            stream.process(seq_no)
        } else {
            // New stream
            let stream = StreamSequences::new(seq_no, self.window_size);
            streams.insert(stream_id, stream);
            SequenceValidation::Valid
        }
    }

    /// Get the next expected sequence number for a stream
    pub fn next_expected(&self, stream_id: &str) -> Option<u64> {
        let streams = self.streams.lock().unwrap();
        streams.get(stream_id).map(|s| s.next_expected())
    }

    /// Get statistics for a stream
    pub fn get_stats(&self, stream_id: &str) -> Option<StreamStats> {
        let streams = self.streams.lock().unwrap();
        streams.get(stream_id).map(|s| s.stats())
    }

    /// Reset tracking for a stream
    pub fn reset_stream(&self, stream_id: &str) {
        let mut streams = self.streams.lock().unwrap();
        streams.remove(stream_id);
    }

    /// Reset all streams
    pub fn reset_all(&self) {
        let mut streams = self.streams.lock().unwrap();
        streams.clear();
    }

    /// Get all tracked stream IDs
    pub fn get_streams(&self) -> Vec<String> {
        let streams = self.streams.lock().unwrap();
        streams.keys().cloned().collect()
    }

    /// Get total number of tracked streams
    pub fn stream_count(&self) -> usize {
        let streams = self.streams.lock().unwrap();
        streams.len()
    }

    /// Get total gaps across all streams
    pub fn total_gaps(&self) -> u64 {
        let streams = self.streams.lock().unwrap();
        streams.values().map(|s| s.stats().gap_count).sum()
    }

    /// Get total duplicates across all streams
    pub fn total_duplicates(&self) -> u64 {
        let streams = self.streams.lock().unwrap();
        streams.values().map(|s| s.stats().duplicate_count).sum()
    }

    /// Generate the next sequence number for a new event
    /// This is used when the system is generating sequence numbers
    pub fn next(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

impl Default for SequenceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_manager_tracks_new_stream() {
        let manager = SequenceManager::new();
        let result = manager.process("alpaca/AAPL", 1);
        
        assert_eq!(result, SequenceValidation::Valid);
        assert_eq!(manager.next_expected("alpaca/AAPL"), Some(2));
    }

    #[test]
    fn test_sequence_progression() {
        let manager = SequenceManager::new();
        
        assert_eq!(manager.process("stream1", 1), SequenceValidation::Valid);
        assert_eq!(manager.process("stream1", 2), SequenceValidation::Valid);
        assert_eq!(manager.process("stream1", 3), SequenceValidation::Valid);
        
        assert_eq!(manager.next_expected("stream1"), Some(4));
    }

    #[test]
    fn test_gap_detection() {
        let manager = SequenceManager::new();
        
        manager.process("stream1", 1);
        manager.process("stream1", 2);
        
        // Gap: missing 3 and 4
        let result = manager.process("stream1", 5);
        
        assert!(matches!(result, SequenceValidation::Gap { .. }));
        if let SequenceValidation::Gap { missing } = result {
            assert_eq!(missing, vec![3, 4]);
        }
        
        // Stats should show gap
        let stats = manager.get_stats("stream1").unwrap();
        assert_eq!(stats.gap_count, 1);
    }

    #[test]
    fn test_duplicate_detection() {
        let manager = SequenceManager::new();
        
        manager.process("stream1", 1);
        manager.process("stream1", 2);
        
        // Duplicate
        let result = manager.process("stream1", 1);
        assert_eq!(result, SequenceValidation::Duplicate);
        
        let stats = manager.get_stats("stream1").unwrap();
        assert_eq!(stats.duplicate_count, 1);
    }

    #[test]
    fn test_sequence_reset() {
        let manager = SequenceManager::new();
        
        // High sequence number
        manager.process("stream1", 1000);
        manager.process("stream1", 1002); // Use 1002 so gap > 1000
        
        // Reset to low number (should detect as reset: 1002 - 1 = 1001 > 1000)
        let result = manager.process("stream1", 1);
        assert_eq!(result, SequenceValidation::Reset);
    }

    #[test]
    fn test_multiple_streams() {
        let manager = SequenceManager::new();
        
        // Two independent streams
        manager.process("stream1", 1);
        manager.process("stream1", 2);
        manager.process("stream2", 100);
        manager.process("stream2", 101);
        
        assert_eq!(manager.stream_count(), 2);
        assert_eq!(manager.next_expected("stream1"), Some(3));
        assert_eq!(manager.next_expected("stream2"), Some(102));
    }

    #[test]
    fn test_stream_reset() {
        let manager = SequenceManager::new();
        
        manager.process("stream1", 1);
        manager.process("stream1", 2);
        manager.process("stream1", 3);
        
        // Reset the stream
        manager.reset_stream("stream1");
        
        // Should be treated as new stream
        let result = manager.process("stream1", 1);
        assert_eq!(result, SequenceValidation::Valid);
        assert_eq!(manager.next_expected("stream1"), Some(2));
    }

    #[test]
    fn test_reset_all() {
        let manager = SequenceManager::new();
        
        manager.process("stream1", 1);
        manager.process("stream2", 1);
        
        assert_eq!(manager.stream_count(), 2);
        
        manager.reset_all();
        
        assert_eq!(manager.stream_count(), 0);
    }

    #[test]
    fn test_window_size_limit() {
        let manager = SequenceManager::with_window_size(5);
        
        // Add more sequences than window size
        for i in 1..=10 {
            manager.process("stream1", i);
        }
        
        // Old sequences should have fallen out of window
        // But we should still detect recent duplicates
        let result = manager.process("stream1", 9); // Recent duplicate
        assert_eq!(result, SequenceValidation::Duplicate);
    }

    #[test]
    fn test_stats() {
        let manager = SequenceManager::new();
        
        manager.process("stream1", 1);
        manager.process("stream1", 2);
        manager.process("stream1", 4); // Gap
        manager.process("stream1", 2); // Duplicate
        
        let stats = manager.get_stats("stream1").unwrap();
        assert_eq!(stats.last_seq, 4);
        assert_eq!(stats.expected_seq, 5);
        assert_eq!(stats.gap_count, 1);
        assert_eq!(stats.duplicate_count, 1);
    }

    #[test]
    fn test_total_gaps_and_duplicates() {
        let manager = SequenceManager::new();
        
        // Stream 1 with gaps
        manager.process("s1", 1);
        manager.process("s1", 3); // Gap
        
        // Stream 2 with duplicate
        manager.process("s2", 1);
        manager.process("s2", 1); // Duplicate
        
        assert_eq!(manager.total_gaps(), 1);
        assert_eq!(manager.total_duplicates(), 1);
    }

    #[test]
    fn test_error_display() {
        let err = SequenceError::Gap { expected: 5, received: 7, gap_size: 2 };
        assert!(err.to_string().contains("gap"));
        
        let err = SequenceError::Duplicate { seq_no: 5 };
        assert!(err.to_string().contains("Duplicate"));
    }

    #[test]
    fn test_sequence_manager_default() {
        let manager: SequenceManager = Default::default();
        assert_eq!(manager.stream_count(), 0);
    }

    #[test]
    fn test_next_generates_unique_values() {
        let manager = SequenceManager::new();
        let seq1 = manager.next();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let seq2 = manager.next();
        assert_ne!(seq1, seq2);
    }

    // =========================================================================
    // VecDeque Window Tests (O(1) operations)
    // =========================================================================

    #[test]
    fn test_window_maintains_size_limit_with_vecdeque() {
        // Use a very small window to test boundary conditions
        let manager = SequenceManager::with_window_size(3);
        
        // Fill window to capacity
        manager.process("stream1", 1);
        manager.process("stream1", 2);
        manager.process("stream1", 3);
        
        // Window is now full: [1, 2, 3]
        // Add one more - should evict oldest (1)
        manager.process("stream1", 4);
        
        // Sequence 1 should have fallen out of window
        // So it should be treated as valid (not duplicate)
        let result = manager.process("stream1", 1);
        assert_eq!(result, SequenceValidation::Valid, 
            "Old sequence should have fallen out of window");
        
        // But recent sequences should still be detected as duplicates
        let result = manager.process("stream1", 4);
        assert_eq!(result, SequenceValidation::Duplicate,
            "Recent sequence should still be in window");
    }

    #[test]
    fn test_window_slides_correctly_with_high_volume() {
        let window_size = 100;
        let manager = SequenceManager::with_window_size(window_size);
        
        // Process many more sequences than window size
        for i in 1..=window_size * 3 {
            let result = manager.process("stream1", i as u64);
            assert_eq!(result, SequenceValidation::Valid,
                "Sequence {} should be valid", i);
        }
        
        // First sequence should have fallen out
        let result = manager.process("stream1", 1);
        assert_eq!(result, SequenceValidation::Valid,
            "First sequence should have fallen out of window after high volume");
        
        // Recent sequences near the end should still be duplicates
        let recent_seq = (window_size * 3 - 10) as u64;
        let result = manager.process("stream1", recent_seq);
        assert_eq!(result, SequenceValidation::Duplicate,
            "Recent sequence should still be in window");
    }

    #[test]
    fn test_vecdeque_operations_are_o1() {
        // This test verifies the behavior is correct with VecDeque
        // The actual O(1) performance is guaranteed by VecDeque's implementation
        let manager = SequenceManager::with_window_size(1000);
        
        // Process many sequences - with VecDeque this is O(n) total
        // With Vec it would be O(n^2) due to O(n) remove(0) operations
        for i in 1..=5000 {
            manager.process("stream1", i);
        }
        
        let stats = manager.get_stats("stream1").unwrap();
        assert_eq!(stats.last_seq, 5000);
        assert_eq!(stats.expected_seq, 5001);
        
        // Verify duplicates still work for recent sequences
        let result = manager.process("stream1", 4999);
        assert_eq!(result, SequenceValidation::Duplicate);
    }

    #[test]
    fn test_window_reset_clears_vecdeque() {
        let manager = SequenceManager::with_window_size(10);
        
        manager.process("stream1", 1);
        manager.process("stream1", 2);
        manager.process("stream1", 3);
        
        // Reset should clear the VecDeque
        manager.reset_stream("stream1");
        
        // Start fresh
        let result = manager.process("stream1", 1);
        assert_eq!(result, SequenceValidation::Valid);
        
        // Should be duplicate now
        let result = manager.process("stream1", 1);
        assert_eq!(result, SequenceValidation::Duplicate);
    }

    #[test]
    fn test_duplicate_detection_boundary() {
        // Test exactly at window boundary
        let window_size = 5;
        let manager = SequenceManager::with_window_size(window_size);
        
        // Process 1, 2, 3, 4, 5, 6 sequentially
        assert_eq!(manager.process("stream1", 1), SequenceValidation::Valid);
        assert_eq!(manager.process("stream1", 2), SequenceValidation::Valid);
        assert_eq!(manager.process("stream1", 3), SequenceValidation::Valid);
        assert_eq!(manager.process("stream1", 4), SequenceValidation::Valid);
        assert_eq!(manager.process("stream1", 5), SequenceValidation::Valid);
        
        // Now window has [1,2,3,4,5], expected is 6
        // Process 6 - should be valid and trigger eviction of 1
        assert_eq!(manager.process("stream1", 6), SequenceValidation::Valid);
        
        // Window should now be [2,3,4,5,6]
        // Check that sequences 2-6 are in window (duplicates)
        // IMPORTANT: Check 2 first, because checking 1 would add it back and evict 2!
        assert_eq!(manager.process("stream1", 2), SequenceValidation::Duplicate, "2 should be duplicate");
        assert_eq!(manager.process("stream1", 3), SequenceValidation::Duplicate, "3 should be duplicate");
        assert_eq!(manager.process("stream1", 4), SequenceValidation::Duplicate, "4 should be duplicate");
        assert_eq!(manager.process("stream1", 5), SequenceValidation::Duplicate, "5 should be duplicate");
        assert_eq!(manager.process("stream1", 6), SequenceValidation::Duplicate, "6 should be duplicate");
        
        // Now 1 should be evicted (not duplicate) - check this LAST
        assert_eq!(manager.process("stream1", 1), SequenceValidation::Valid, "1 should have fallen out of window");
    }
}
