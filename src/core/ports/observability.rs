// observability.rs

pub trait IObservability {
    fn emit(&self, event: String);
}

