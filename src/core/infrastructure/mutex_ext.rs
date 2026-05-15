// core/infrastructure/mutex_ext.rs
//
// Extension trait for std::sync::Mutex that provides safe locking
// with automatic recovery from poisoned mutexes.
//
// This is critical for trading systems where a panic in one thread
// should not cause cascading failures in other threads.

use std::sync::{Mutex, MutexGuard};

/// Extension trait for Mutex that provides safe locking with poison recovery
pub trait MutexExt<T> {
    /// Safely lock the mutex, recovering from poison if necessary
    ///
    /// If a thread panics while holding the lock, this method will
    /// recover the data from the poisoned mutex instead of panicking.
    /// This prevents cascading failures in trading systems.
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::Mutex;
    /// use broker_gateway_service::core::infrastructure::MutexExt;
    ///
    /// let mutex = Mutex::new(0);
    /// let guard = mutex.safe_lock();
    /// ```
    fn safe_lock(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    #[inline]
    fn safe_lock(&self) -> MutexGuard<'_, T> {
        match self.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Recover from poisoned mutex - take ownership of the data
                // This prevents cascading failures when one thread panics
                poisoned.into_inner()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_safe_lock_normal_case() {
        let mutex = Mutex::new(42);
        let guard = mutex.safe_lock();
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_safe_lock_recovers_from_poison() {
        use std::sync::Arc;
        let mutex = Arc::new(Mutex::new(0));

        // Spawn a thread that panics while holding the lock
        let handle = thread::spawn({
            let mutex = mutex.clone();
            move || {
                let _guard = mutex.lock().unwrap();
                panic!("Intentional panic to poison mutex");
            }
        });

        // Wait for the thread to panic
        let _ = handle.join();

        // safe_lock should recover from poison
        let guard = mutex.safe_lock();
        assert_eq!(*guard, 0);
    }

    #[test]
    fn test_safe_lock_allows_access_after_poison() {
        use std::sync::Arc;
        let mutex = Arc::new(Mutex::new(String::from("initial")));

        // Poison the mutex
        let handle = thread::spawn({
            let mutex = mutex.clone();
            move || {
                let mut guard = mutex.lock().unwrap();
                *guard = String::from("modified");
                panic!("Poison!");
            }
        });
        let _ = handle.join();

        // safe_lock should recover and see the modified value
        let guard = mutex.safe_lock();
        assert_eq!(*guard, "modified");
    }

    #[test]
    fn test_safe_lock_concurrent_access() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mutex = Arc::new(Mutex::new(0));
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for _ in 0..10 {
            let mutex = mutex.clone();
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                let mut guard = mutex.safe_lock();
                *guard += 1;
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(*mutex.safe_lock(), 10);
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }
}
