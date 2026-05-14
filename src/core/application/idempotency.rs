// idempotency.rs
// 
// Bounded idempotency store with LRU eviction to prevent unbounded memory growth
// in long-running trading systems.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Default maximum number of entries in the idempotency store.
/// When exceeded, least recently used entries are evicted.
pub const DEFAULT_CAPACITY: usize = 100_000;

/// Entry in the idempotency store with timestamp for potential TTL-based eviction
#[derive(Clone, Debug)]
struct Entry {
    result: String,
    last_accessed: std::time::Instant,
}

/// Bounded idempotency store with LRU eviction.
/// 
/// This store prevents duplicate processing of requests by tracking processed keys.
/// It maintains a fixed maximum capacity - when the limit is reached,
/// the least recently used entries are evicted to prevent unbounded memory growth.
pub struct IdempotencyStore {
    processed: Mutex<HashMap<String, Entry>>,
    access_order: Mutex<VecDeque<String>>,
    capacity: usize,
}

impl IdempotencyStore {
    /// Create a new idempotency store with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a new idempotency store with a specific capacity.
    /// 
    /// # Arguments
    /// * `capacity` - Maximum number of entries to store before evicting LRU entries
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            processed: Mutex::new(HashMap::with_capacity(capacity)),
            access_order: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Check if a key has been processed.
    /// Updates the access order to mark this key as recently used.
    pub fn is_processed(&self, key: &str) -> bool {
        let mut processed = self.processed.lock().unwrap();
        
        if let Some(entry) = processed.get_mut(key) {
            // Update last accessed time
            entry.last_accessed = std::time::Instant::now();
            
            // Move to front of access order (mark as recently used)
            drop(processed); // Release lock before acquiring access_order lock
            self.update_access_order(key);
            
            true
        } else {
            false
        }
    }

    /// Mark a key as processed with its result.
    /// If the store is at capacity, evicts the least recently used entry.
    pub fn mark_processed(&self, key: String, result: String) {
        let mut processed = self.processed.lock().unwrap();
        let mut access_order = self.access_order.lock().unwrap();

        // Check if we need to evict (only if this is a new key)
        if !processed.contains_key(&key) && processed.len() >= self.capacity {
            // Evict least recently used entry
            if let Some(lru_key) = access_order.pop_back() {
                processed.remove(&lru_key);
            }
        }

        // Insert or update the entry
        let entry = Entry {
            result,
            last_accessed: std::time::Instant::now(),
        };
        
        // If key already exists, remove from current position in access_order
        if processed.contains_key(&key) {
            access_order.retain(|k| k != &key);
        }
        
        // Insert at front (most recently used)
        processed.insert(key.clone(), entry);
        access_order.push_front(key);
    }

    /// Get the result for a processed key if it exists.
    /// Updates the access order to mark this key as recently used.
    pub fn get_result(&self, key: &str) -> Option<String> {
        let mut processed = self.processed.lock().unwrap();
        
        if let Some(entry) = processed.get_mut(key) {
            entry.last_accessed = std::time::Instant::now();
            let result = entry.result.clone();
            
            // Move to front of access order
            drop(processed);
            self.update_access_order(key);
            
            Some(result)
        } else {
            None
        }
    }

    /// Get the current number of entries in the store.
    pub fn len(&self) -> usize {
        self.processed.lock().unwrap().len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.processed.lock().unwrap().is_empty()
    }

    /// Get the configured capacity of the store.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clear all entries from the store.
    pub fn clear(&self) {
        let mut processed = self.processed.lock().unwrap();
        let mut access_order = self.access_order.lock().unwrap();
        processed.clear();
        access_order.clear();
    }

    /// Update the access order for a key (move to front)
    fn update_access_order(&self, key: &str) {
        let mut access_order = self.access_order.lock().unwrap();
        
        // Remove from current position if exists
        if let Some(pos) = access_order.iter().position(|k| k == key) {
            access_order.remove(pos);
        }
        
        // Add to front
        access_order.push_front(key.to_string());
    }
}

impl Default for IdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_store_is_empty() {
        let store = IdempotencyStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.capacity(), DEFAULT_CAPACITY);
    }

    #[test]
    fn test_mark_and_check_processed() {
        let store = IdempotencyStore::new();
        
        assert!(!store.is_processed("key1"));
        
        store.mark_processed("key1".to_string(), "result1".to_string());
        
        assert!(store.is_processed("key1"));
        assert!(!store.is_processed("key2"));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_get_result() {
        let store = IdempotencyStore::new();
        
        store.mark_processed("key1".to_string(), "result1".to_string());
        
        assert_eq!(store.get_result("key1"), Some("result1".to_string()));
        assert_eq!(store.get_result("key2"), None);
    }

    #[test]
    fn test_with_custom_capacity() {
        let store = IdempotencyStore::with_capacity(10);
        assert_eq!(store.capacity(), 10);
    }

    #[test]
    fn test_eviction_when_at_capacity() {
        let store = IdempotencyStore::with_capacity(3);
        
        // Fill to capacity
        store.mark_processed("key1".to_string(), "result1".to_string());
        store.mark_processed("key2".to_string(), "result2".to_string());
        store.mark_processed("key3".to_string(), "result3".to_string());
        
        assert_eq!(store.len(), 3);
        assert!(store.is_processed("key1"));
        assert!(store.is_processed("key2"));
        assert!(store.is_processed("key3"));
        
        // Add one more - should evict key1 (LRU)
        store.mark_processed("key4".to_string(), "result4".to_string());
        
        assert_eq!(store.len(), 3);
        assert!(!store.is_processed("key1"), "key1 should have been evicted");
        assert!(store.is_processed("key2"));
        assert!(store.is_processed("key3"));
        assert!(store.is_processed("key4"));
    }

    #[test]
    fn test_lru_order_updated_on_access() {
        let store = IdempotencyStore::with_capacity(3);
        
        // Fill to capacity
        store.mark_processed("key1".to_string(), "result1".to_string());
        store.mark_processed("key2".to_string(), "result2".to_string());
        store.mark_processed("key3".to_string(), "result3".to_string());
        
        // Access key1 to make it recently used
        assert!(store.is_processed("key1"));
        
        // Add key4 - should evict key2 (now the LRU)
        store.mark_processed("key4".to_string(), "result4".to_string());
        
        assert!(store.is_processed("key1"), "key1 should still exist (was accessed)");
        assert!(!store.is_processed("key2"), "key2 should have been evicted");
        assert!(store.is_processed("key3"));
        assert!(store.is_processed("key4"));
    }

    #[test]
    fn test_lru_order_updated_on_get_result() {
        let store = IdempotencyStore::with_capacity(3);
        
        // Fill to capacity
        store.mark_processed("key1".to_string(), "result1".to_string());
        store.mark_processed("key2".to_string(), "result2".to_string());
        store.mark_processed("key3".to_string(), "result3".to_string());
        
        // Get result for key1 to make it recently used
        let _ = store.get_result("key1");
        
        // Add key4 - should evict key2 (now the LRU)
        store.mark_processed("key4".to_string(), "result4".to_string());
        
        assert!(store.is_processed("key1"), "key1 should still exist (was accessed via get_result)");
        assert!(!store.is_processed("key2"), "key2 should have been evicted");
        assert!(store.is_processed("key3"));
        assert!(store.is_processed("key4"));
    }

    #[test]
    fn test_update_existing_key() {
        let store = IdempotencyStore::new();
        
        store.mark_processed("key1".to_string(), "result1".to_string());
        assert_eq!(store.get_result("key1"), Some("result1".to_string()));
        
        // Update with new result
        store.mark_processed("key1".to_string(), "updated_result".to_string());
        assert_eq!(store.get_result("key1"), Some("updated_result".to_string()));
        assert_eq!(store.len(), 1, "Length should still be 1 after update");
    }

    #[test]
    fn test_clear() {
        let store = IdempotencyStore::new();
        
        store.mark_processed("key1".to_string(), "result1".to_string());
        store.mark_processed("key2".to_string(), "result2".to_string());
        
        assert_eq!(store.len(), 2);
        
        store.clear();
        
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(!store.is_processed("key1"));
        assert!(!store.is_processed("key2"));
    }

    #[test]
    fn test_default_trait() {
        let store: IdempotencyStore = Default::default();
        assert_eq!(store.capacity(), DEFAULT_CAPACITY);
        assert!(store.is_empty());
    }

    #[test]
    fn test_large_capacity() {
        // Test with the default large capacity
        let store = IdempotencyStore::new();
        
        // Add many entries (but less than capacity)
        for i in 0..1000 {
            store.mark_processed(format!("key{}", i), format!("result{}", i));
        }
        
        assert_eq!(store.len(), 1000);
        
        // Verify all are accessible
        for i in 0..1000 {
            assert!(store.is_processed(&format!("key{}", i)), "key{} should exist", i);
        }
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(IdempotencyStore::with_capacity(100));
        let mut handles = vec![];

        // Spawn multiple threads to access the store concurrently
        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = thread::spawn(move || {
                for j in 0..20 {
                    let key = format!("thread{}_key{}", i, j);
                    store_clone.mark_processed(key.clone(), format!("result{}", j));
                    store_clone.is_processed(&key);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Store should have at most capacity entries
        assert!(store.len() <= store.capacity());
    }
}
