// retry_policy.rs
pub struct RetryPolicy {
    pub retries: usize,
}

impl RetryPolicy {
    pub fn execute<F, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Result<T, E>,
    {
        let mut last = None;
        for _ in 0..=self.retries {
            match f() {
                Ok(v) => return Ok(v),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap())
    }
}

