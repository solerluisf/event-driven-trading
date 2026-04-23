// circuit_breaker.rs

#[derive(Default)]
pub struct CircuitBreaker {
    failures: Mutex<u32>,
    open: Mutex<bool>,
}

impl CircuitBreaker {
    pub fn call<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        if self.is_open() {
            return f();
        }
        match f() {
            Ok(v) => {
                self.record_success();
                Ok(v)
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    pub fn record_success(&self) {
        *self.failures.lock().unwrap() = 0;
        *self.open.lock().unwrap() = false;
    }

    pub fn record_failure(&self) {
        let mut failures = self.failures.lock().unwrap();
        *failures += 1;
        if *failures > 3 {
            *self.open.lock().unwrap() = true;
        }
    }

    pub fn is_open(&self) -> bool {
        *self.open.lock().unwrap()
    }
}