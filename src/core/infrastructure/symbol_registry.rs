//! Fixed-capacity symbol registry for O(1) lookups.
//!
//! This module provides a cache-friendly alternative to HashMap<String, T>
//! for high-frequency trading scenarios where symbol lookups are in the hot path.
//!
//! # Design
//!
//! - Pre-allocates a fixed-size array indexed by symbol ID
//! - Uses bi-directional mapping: Symbol String <-> u16 index
//! - O(1) lookup with perfect cache locality
//! - No hashing in the hot path
//!
//! # Trade-offs
//!
//! - Fixed maximum number of symbols (default: 10,000)
//! - Symbols must be registered before use
//! - Slightly higher memory usage for sparse symbol sets

use std::collections::HashMap;
use std::fmt;

/// Maximum number of symbols that can be registered.
/// This is a compile-time constant for performance and memory layout.
pub const MAX_SYMBOLS: usize = 10_000;

/// A unique identifier for a symbol (u16 allows 65,535 unique symbols).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SymbolId(u16);

impl SymbolId {
    /// Create a SymbolId from a raw u16 value.
    /// # Safety
    /// The caller must ensure the value is less than MAX_SYMBOLS.
    pub const fn from_raw(id: u16) -> Self {
        Self(id)
    }

    /// Get the raw u16 value.
    pub const fn as_u16(&self) -> u16 {
        self.0
    }

    /// Get the index into arrays sized by MAX_SYMBOLS.
    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl fmt::Display for SymbolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SymbolId({})", self.0)
    }
}

/// Registry for mapping symbols to compact integer IDs.
///
/// This structure maintains a bidirectional mapping between symbol strings
/// and compact u16 IDs, enabling O(1) array-based lookups.
///
/// # Example
///
/// ```
/// use broker_gateway_service::core::infrastructure::symbol_registry::SymbolRegistry;
///
/// let mut registry = SymbolRegistry::new();
///
/// // Register symbols
/// let aapl_id = registry.register("AAPL");
/// let msft_id = registry.register("MSFT");
///
/// // Lookup by string returns the same ID
/// assert_eq!(registry.lookup("AAPL"), Some(aapl_id));
/// assert_eq!(registry.lookup("MSFT"), Some(msft_id));
/// assert_eq!(registry.lookup("GOOGL"), None);
///
/// // Get symbol string from ID
/// assert_eq!(registry.get_symbol(aapl_id), Some("AAPL"));
/// ```
pub struct SymbolRegistry {
    /// Maps symbol strings to their IDs (used for registration/lookup).
    symbol_to_id: HashMap<String, SymbolId>,
    /// Maps IDs back to symbol strings (used for reverse lookup).
    id_to_symbol: Vec<Option<String>>,
    /// Next available ID counter.
    next_id: u16,
}

impl SymbolRegistry {
    /// Create a new empty symbol registry.
    pub fn new() -> Self {
        Self {
            symbol_to_id: HashMap::with_capacity(MAX_SYMBOLS),
            id_to_symbol: vec![None; MAX_SYMBOLS],
            next_id: 0,
        }
    }

    /// Register a symbol and return its ID.
    ///
    /// If the symbol is already registered, returns the existing ID.
    /// If the registry is full, panics (this is a programming error in trading systems).
    ///
    /// # Panics
    ///
    /// Panics if the registry is full (MAX_SYMBOLS symbols already registered).
    pub fn register(&mut self, symbol: impl Into<String>) -> SymbolId {
        let symbol = symbol.into();
        
        // Check if already registered
        if let Some(&id) = self.symbol_to_id.get(&symbol) {
            return id;
        }

        // Allocate new ID
        let id = SymbolId(self.next_id);
        assert!(
            (self.next_id as usize) < MAX_SYMBOLS,
            "SymbolRegistry capacity exceeded: maximum {} symbols allowed",
            MAX_SYMBOLS
        );

        // Store mappings
        self.id_to_symbol[self.next_id as usize] = Some(symbol.clone());
        self.symbol_to_id.insert(symbol, id);
        self.next_id += 1;

        id
    }

    /// Lookup a symbol ID by its string representation.
    ///
    /// Returns `None` if the symbol is not registered.
    pub fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        self.symbol_to_id.get(symbol).copied()
    }

    /// Get the symbol string for a given ID.
    ///
    /// Returns `None` if the ID is invalid or not assigned.
    pub fn get_symbol(&self, id: SymbolId) -> Option<&str> {
        self.id_to_symbol
            .get(id.as_usize())
            .and_then(|opt| opt.as_deref())
    }

    /// Check if a symbol is registered.
    pub fn contains(&self, symbol: &str) -> bool {
        self.symbol_to_id.contains_key(symbol)
    }

    /// Get the number of registered symbols.
    pub fn len(&self) -> usize {
        self.symbol_to_id.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.symbol_to_id.is_empty()
    }

    /// Returns true if the registry has reached capacity.
    pub fn is_full(&self) -> bool {
        self.symbol_to_id.len() >= MAX_SYMBOLS
    }

    /// Clear all registrations.
    pub fn clear(&mut self) {
        self.symbol_to_id.clear();
        self.id_to_symbol.iter_mut().for_each(|slot| *slot = None);
        self.next_id = 0;
    }
}

impl Default for SymbolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SymbolRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SymbolRegistry")
            .field("registered_count", &self.len())
            .field("capacity", &MAX_SYMBOLS)
            .field("is_full", &self.is_full())
            .finish()
    }
}

/// A fixed-capacity array indexed by SymbolId.
///
/// This provides O(1) lookups with cache-friendly memory layout.
/// Similar to Vec<Option<T>> but with SymbolId indexing.
///
/// # Example
///
/// ```
/// use broker_gateway_service::core::infrastructure::symbol_registry::{SymbolIdArray, SymbolRegistry};
///
/// let mut registry = SymbolRegistry::new();
/// let aapl_id = registry.register("AAPL");
///
/// let mut array = SymbolIdArray::<u32>::new();
/// array.insert(aapl_id, 42);
///
/// assert_eq!(array.get(aapl_id), Some(&42));
/// ```
pub struct SymbolIdArray<T: Default> {
    data: Vec<Option<T>>,
}

impl<T: Default> SymbolIdArray<T> {
    /// Create a new empty symbol ID array.
    pub fn new() -> Self {
        // Initialize with empty Vec of correct size without requiring Clone
        let mut data = Vec::with_capacity(MAX_SYMBOLS);
        for _ in 0..MAX_SYMBOLS {
            data.push(None);
        }
        Self { data }
    }

    /// Insert a value at the given symbol ID.
    pub fn insert(&mut self, id: SymbolId, value: T) {
        let idx = id.as_usize();
        if idx < self.data.len() {
            self.data[idx] = Some(value);
        }
    }

    /// Get a reference to the value at the given symbol ID.
    pub fn get(&self, id: SymbolId) -> Option<&T> {
        self.data.get(id.as_usize()).and_then(|opt| opt.as_ref())
    }

    /// Get a mutable reference to the value at the given symbol ID.
    /// If the slot is empty, initializes it with Default::default().
    pub fn get_mut(&mut self, id: SymbolId) -> Option<&mut T> {
        let idx = id.as_usize();
        if idx < self.data.len() {
            if self.data[idx].is_none() {
                self.data[idx] = Some(T::default());
            }
            self.data[idx].as_mut()
        } else {
            None
        }
    }

    /// Remove the value at the given symbol ID.
    pub fn remove(&mut self, id: SymbolId) -> Option<T> {
        let idx = id.as_usize();
        if idx < self.data.len() {
            self.data[idx].take()
        } else {
            None
        }
    }

    /// Check if a value exists at the given symbol ID.
    pub fn contains(&self, id: SymbolId) -> bool {
        self.get(id).is_some()
    }

    /// Clear all values.
    pub fn clear(&mut self) {
        self.data.iter_mut().for_each(|slot| *slot = None);
    }

    /// Iterate over all filled slots.
    pub fn iter(&self) -> impl Iterator<Item = (SymbolId, &T)> {
        self.data
            .iter()
            .enumerate()
            .filter_map(|(idx, opt)| opt.as_ref().map(|v| (SymbolId::from_raw(idx as u16), v)))
    }

    /// Iterate over all filled slots mutably.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (SymbolId, &mut T)> {
        self.data
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, opt)| opt.as_mut().map(|v| (SymbolId::from_raw(idx as u16), v)))
    }
}

impl<T: Default> Default for SymbolIdArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_id_basic_operations() {
        let id = SymbolId::from_raw(42);
        assert_eq!(id.as_u16(), 42);
        assert_eq!(id.as_usize(), 42);
    }

    #[test]
    fn symbol_registry_registration() {
        let mut registry = SymbolRegistry::new();

        let aapl = registry.register("AAPL");
        let msft = registry.register("MSFT");

        // IDs should be sequential
        assert_eq!(aapl.as_u16(), 0);
        assert_eq!(msft.as_u16(), 1);

        // Lookup should return same IDs
        assert_eq!(registry.lookup("AAPL"), Some(aapl));
        assert_eq!(registry.lookup("MSFT"), Some(msft));
        assert_eq!(registry.lookup("GOOGL"), None);

        // Reverse lookup
        assert_eq!(registry.get_symbol(aapl), Some("AAPL"));
        assert_eq!(registry.get_symbol(msft), Some("MSFT"));
    }

    #[test]
    fn symbol_registry_duplicate_registration() {
        let mut registry = SymbolRegistry::new();

        let id1 = registry.register("AAPL");
        let id2 = registry.register("AAPL");

        assert_eq!(id1, id2);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn symbol_registry_contains() {
        let mut registry = SymbolRegistry::new();
        registry.register("AAPL");

        assert!(registry.contains("AAPL"));
        assert!(!registry.contains("MSFT"));
    }

    #[test]
    fn symbol_registry_clear() {
        let mut registry = SymbolRegistry::new();
        registry.register("AAPL");
        registry.register("MSFT");

        assert_eq!(registry.len(), 2);
        
        registry.clear();
        
        assert_eq!(registry.len(), 0);
        assert!(!registry.contains("AAPL"));
    }

    #[test]
    fn symbol_id_array_operations() {
        let mut array = SymbolIdArray::<String>::new();
        let id = SymbolId::from_raw(0);

        // Initially empty
        assert!(array.get(id).is_none());

        // Insert
        array.insert(id, "test".to_string());
        assert_eq!(array.get(id), Some(&"test".to_string()));

        // Get mutable
        if let Some(val) = array.get_mut(id) {
            *val = "modified".to_string();
        }
        assert_eq!(array.get(id), Some(&"modified".to_string()));

        // Remove
        let removed = array.remove(id);
        assert_eq!(removed, Some("modified".to_string()));
        assert!(array.get(id).is_none());
    }

    #[test]
    fn symbol_id_array_get_mut_initializes_default() {
        // Test the auto-initialization behavior of get_mut
        let mut array = SymbolIdArray::<Vec<u32>>::new();
        let id = SymbolId::from_raw(5);

        // Initially empty
        assert!(array.get(id).is_none());
        assert!(!array.contains(id));

        // get_mut should auto-initialize with Default::default()
        {
            let vec_ref = array.get_mut(id).expect("Should auto-initialize");
            vec_ref.push(42);
        }

        // Now it should exist
        assert!(array.contains(id));
        assert_eq!(array.get(id), Some(&vec![42]));

        // Remove it
        let removed = array.remove(id);
        assert_eq!(removed, Some(vec![42]));
        assert!(array.get(id).is_none());
    }

    #[test]
    fn symbol_id_array_iteration() {
        let mut array = SymbolIdArray::<u32>::new();
        
        array.insert(SymbolId::from_raw(0), 100);
        array.insert(SymbolId::from_raw(5), 500);
        array.insert(SymbolId::from_raw(10), 1000);

        let collected: Vec<_> = array.iter().collect();
        assert_eq!(collected.len(), 3);
        assert!(collected.contains(&(SymbolId::from_raw(0), &100)));
        assert!(collected.contains(&(SymbolId::from_raw(5), &500)));
        assert!(collected.contains(&(SymbolId::from_raw(10), &1000)));
    }

    #[test]
    fn symbol_registry_is_full() {
        let mut registry = SymbolRegistry::new();
        assert!(!registry.is_full());

        // Register many symbols
        for i in 0..100 {
            registry.register(format!("SYM{}", i));
        }

        assert!(!registry.is_full());
        assert_eq!(registry.len(), 100);
    }

    #[test]
    fn symbol_id_display() {
        let id = SymbolId::from_raw(42);
        assert_eq!(format!("{}", id), "SymbolId(42)");
    }

    #[test]
    fn symbol_registry_debug() {
        let mut registry = SymbolRegistry::new();
        registry.register("AAPL");
        
        let debug_str = format!("{:?}", registry);
        assert!(debug_str.contains("registered_count: 1"));
        assert!(debug_str.contains("capacity: 10000"));
    }
}
