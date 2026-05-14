//! Infrastructure layer containing low-level utilities and performance optimizations.
//!
//! This module provides cache-friendly data structures and utilities designed
//! for high-frequency trading scenarios where microsecond-level latency matters.

pub mod symbol_registry;

// Re-export commonly used types for convenience
pub use symbol_registry::{SymbolId, SymbolIdArray, SymbolRegistry, MAX_SYMBOLS};
