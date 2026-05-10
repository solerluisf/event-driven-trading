// telemetry_decorator.rs
//
// Measures and emits telemetry for all external round-trips.
// Emits: latency, status code, payload size, error type, and trace IDs.

use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};

/// Unique identifier for tracing requests across the system
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceId(pub String);

impl TraceId {
    /// Generate a new trace ID
    pub fn new() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(format!("trace-{}-{}", timestamp, rand::random::<u64>()))
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

/// Telemetry event for external round-trips
#[derive(Debug, Clone, PartialEq)]
pub struct TelemetryEvent {
    /// Unique trace ID for request tracking
    pub trace_id: TraceId,
    /// Operation name (e.g., "alpaca_submit_order", "fix_cancel")
    pub operation: String,
    /// Start time of the operation
    pub start_time: Instant,
    /// End time of the operation
    pub end_time: Instant,
    /// Duration of the operation
    pub latency: Duration,
    /// HTTP status code or FIX error code
    pub status_code: Option<u16>,
    /// Size of request payload in bytes
    pub request_payload_size: usize,
    /// Size of response payload in bytes
    pub response_payload_size: usize,
    /// Error type if operation failed
    pub error_type: Option<String>,
    /// Error message if operation failed
    pub error_message: Option<String>,
    /// Broker/exchange identifier
    pub target: String,
    /// Whether the operation succeeded
    pub success: bool,
}

impl TelemetryEvent {
    /// Create a new telemetry event builder
    pub fn builder(operation: impl Into<String>, target: impl Into<String>) -> TelemetryEventBuilder {
        TelemetryEventBuilder::new(operation, target)
    }
}

/// Builder for telemetry events
pub struct TelemetryEventBuilder {
    trace_id: TraceId,
    operation: String,
    target: String,
    start_time: Instant,
    status_code: Option<u16>,
    request_payload_size: usize,
    response_payload_size: usize,
    error_type: Option<String>,
    error_message: Option<String>,
    success: bool,
}

impl TelemetryEventBuilder {
    fn new(operation: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            trace_id: TraceId::new(),
            operation: operation.into(),
            target: target.into(),
            start_time: Instant::now(),
            status_code: None,
            request_payload_size: 0,
            response_payload_size: 0,
            error_type: None,
            error_message: None,
            success: true,
        }
    }

    /// Set the trace ID
    pub fn trace_id(mut self, trace_id: TraceId) -> Self {
        self.trace_id = trace_id;
        self
    }

    /// Set the status code
    pub fn status_code(mut self, code: u16) -> Self {
        self.status_code = Some(code);
        self
    }

    /// Set the request payload size
    pub fn request_payload_size(mut self, size: usize) -> Self {
        self.request_payload_size = size;
        self
    }

    /// Set the response payload size
    pub fn response_payload_size(mut self, size: usize) -> Self {
        self.response_payload_size = size;
        self
    }

    /// Mark as failed with error type
    pub fn failed(mut self, error_type: impl Into<String>, error_message: impl Into<String>) -> Self {
        self.success = false;
        self.error_type = Some(error_type.into());
        self.error_message = Some(error_message.into());
        self
    }

    /// Build the telemetry event
    pub fn build(self) -> TelemetryEvent {
        let end_time = Instant::now();
        TelemetryEvent {
            trace_id: self.trace_id,
            operation: self.operation,
            start_time: self.start_time,
            end_time,
            latency: end_time.duration_since(self.start_time),
            status_code: self.status_code,
            request_payload_size: self.request_payload_size,
            response_payload_size: self.response_payload_size,
            error_type: self.error_type,
            error_message: self.error_message,
            target: self.target,
            success: self.success,
        }
    }
}

/// Callback for telemetry event consumption
pub type TelemetryCallback = Arc<dyn Fn(TelemetryEvent) + Send + Sync>;

/// Telemetry decorator that measures and emits telemetry for external operations
pub struct TelemetryDecorator {
    callback: Mutex<Option<TelemetryCallback>>,
    /// Storage for active operation contexts (trace_id -> start_time)
    active_operations: Mutex<HashMap<TraceId, (String, Instant)>>,
}

impl TelemetryDecorator {
    /// Create a new telemetry decorator
    pub fn new() -> Self {
        Self {
            callback: Mutex::new(None),
            active_operations: Mutex::new(HashMap::new()),
        }
    }

    /// Register a callback to receive telemetry events
    pub fn register_callback<F>(&self, callback: F)
    where
        F: Fn(TelemetryEvent) + Send + Sync + 'static,
    {
        let mut cb = self.callback.lock().unwrap();
        *cb = Some(Arc::new(callback));
    }

    /// Check if a callback is registered
    pub fn has_callback(&self) -> bool {
        self.callback.lock().unwrap().is_some()
    }

    /// Start tracking an operation and return a trace ID
    pub fn start_operation(&self, operation: impl Into<String>) -> TraceId {
        let trace_id = TraceId::new();
        let mut ops = self.active_operations.lock().unwrap();
        ops.insert(trace_id.clone(), (operation.into(), Instant::now()));
        trace_id
    }

    /// Complete an operation and emit telemetry
    pub fn complete_operation(
        &self,
        trace_id: TraceId,
        status_code: Option<u16>,
        request_size: usize,
        response_size: usize,
        error: Option<(String, String)>,
    ) {
        let mut ops = self.active_operations.lock().unwrap();
        if let Some((operation, start_time)) = ops.remove(&trace_id) {
            let end_time = Instant::now();
            let latency = end_time.duration_since(start_time);
            
            let event = TelemetryEvent {
                trace_id,
                operation,
                start_time,
                end_time,
                latency,
                status_code,
                request_payload_size: request_size,
                response_payload_size: response_size,
                error_type: error.as_ref().map(|e| e.0.clone()),
                error_message: error.as_ref().map(|e| e.1.clone()),
                target: "unknown".to_string(), // Can be enhanced to track target
                success: error.is_none(),
            };
            
            self.emit_event(event);
        }
    }

    /// Wrap an outbound operation with telemetry measurement
    /// 
    /// # Arguments
    /// * `operation_name` - Name of the operation for identification
    /// * `target` - Target broker/exchange
    /// * `f` - The operation to wrap
    /// 
    /// # Returns
    /// A tuple of (result, telemetry_event)
    /// 
    /// # Note
    /// This version cannot detect errors. Use `wrap_outbound_result` for Result-returning operations.
    pub fn wrap_outbound<F, T>(&self, operation_name: impl Into<String>, target: impl Into<String>, f: F) -> (T, TelemetryEvent)
    where
        F: FnOnce() -> T,
    {
        let operation_name = operation_name.into();
        let target = target.into();
        let start = Instant::now();
        let trace_id = TraceId::new();
        
        // Execute the operation
        let result = f();
        
        let end = Instant::now();
        let latency = end.duration_since(start);
        
        let event = TelemetryEvent {
            trace_id,
            operation: operation_name,
            start_time: start,
            end_time: end,
            latency,
            status_code: None, // Can be set by caller if known
            request_payload_size: 0, // Can be measured by caller
            response_payload_size: 0, // Can be measured by caller
            error_type: None,
            error_message: None,
            target,
            success: true,
        };
        
        self.emit_event(event.clone());
        (result, event)
    }

    /// Wrap an outbound operation that returns a Result, with full error tracking
    /// 
    /// # Arguments
    /// * `operation_name` - Name of the operation for identification
    /// * `target` - Target broker/exchange
    /// * `f` - The operation to wrap (must return Result)
    /// * `error_classifier` - Optional function to classify errors
    /// 
    /// # Returns
    /// A tuple of (result, telemetry_event) where telemetry includes error info
    /// 
    /// # Example
    /// ```
    /// use broker_gateway_service::core::patterns::telemetry_decorator::TelemetryDecorator;
    /// 
    /// let decorator = TelemetryDecorator::new();
    /// 
    /// // Example error classifier function
    /// fn classify_error(e: &std::io::Error) -> &'static str {
    ///     use std::io::ErrorKind;
    ///     match e.kind() {
    ///         ErrorKind::NotFound => "not_found",
    ///         ErrorKind::ConnectionRefused => "connection_refused",
    ///         _ => "unknown_io_error",
    ///     }
    /// }
    /// 
    /// let (result, event) = decorator.wrap_outbound_result(
    ///     "read_file",
    ///     "filesystem",
    ///     || Ok::<String, std::io::Error>("data".to_string()),
    ///     Some(classify_error)
    /// );
    /// 
    /// assert!(result.is_ok());
    /// assert!(event.success);
    /// ```
    pub fn wrap_outbound_result<F, T, E>(
        &self,
        operation_name: impl Into<String>,
        target: impl Into<String>,
        f: F,
        error_classifier: Option<fn(&E) -> &'static str>,
    ) -> (Result<T, E>, TelemetryEvent)
    where
        F: FnOnce() -> Result<T, E>,
        E: std::fmt::Display,
    {
        let operation_name = operation_name.into();
        let target = target.into();
        let start = Instant::now();
        let trace_id = TraceId::new();
        
        // Execute the operation
        let result = f();
        
        let end = Instant::now();
        let latency = end.duration_since(start);
        
        // Extract error information if failed
        let (success, error_type, error_message) = match &result {
            Ok(_) => (true, None, None),
            Err(e) => {
                let error_type = error_classifier.map(|classifier| classifier(e).to_string());
                let error_message = Some(e.to_string());
                (false, error_type, error_message)
            }
        };
        
        let event = TelemetryEvent {
            trace_id,
            operation: operation_name,
            start_time: start,
            end_time: end,
            latency,
            status_code: None, // Can be set by caller if known
            request_payload_size: 0,
            response_payload_size: 0,
            error_type,
            error_message,
            target,
            success,
        };
        
        self.emit_event(event.clone());
        (result, event)
    }

    /// Wrap an outbound async operation with telemetry measurement
    #[cfg(feature = "tokio")]
    pub async fn wrap_outbound_async<F, Fut, T>(
        &self,
        operation_name: impl Into<String>,
        target: impl Into<String>,
        f: F,
    ) -> (T, TelemetryEvent)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let operation_name = operation_name.into();
        let target = target.into();
        let start = Instant::now();
        let trace_id = TraceId::new();
        
        // Execute the operation
        let result = f().await;
        
        let end = Instant::now();
        let latency = end.duration_since(start);
        
        let event = TelemetryEvent {
            trace_id,
            operation: operation_name,
            start_time: start,
            end_time: end,
            latency,
            status_code: None,
            request_payload_size: 0,
            response_payload_size: 0,
            error_type: None,
            error_message: None,
            target,
            success: true,
        };
        
        self.emit_event(event.clone());
        (result, event)
    }

    /// Wrap an outbound async operation that returns a Result, with full error tracking
    /// 
    /// # Arguments
    /// * `operation_name` - Name of the operation for identification
    /// * `target` - Target broker/exchange
    /// * `f` - The async operation to wrap (must return Result)
    /// * `error_classifier` - Optional function to classify errors
    /// 
    /// # Returns
    /// A tuple of (result, telemetry_event) where telemetry includes error info
    pub async fn wrap_outbound_result_async<F, Fut, T, E>(
        &self,
        operation_name: impl Into<String>,
        target: impl Into<String>,
        f: F,
        error_classifier: Option<fn(&E) -> &'static str>,
    ) -> (Result<T, E>, TelemetryEvent)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let operation_name = operation_name.into();
        let target = target.into();
        let start = Instant::now();
        let trace_id = TraceId::new();
        
        // Execute the async operation
        let result = f().await;
        
        let end = Instant::now();
        let latency = end.duration_since(start);
        
        // Extract error information if failed
        let (success, error_type, error_message) = match &result {
            Ok(_) => (true, None, None),
            Err(e) => {
                let error_type = error_classifier.map(|classifier| classifier(e).to_string());
                let error_message = Some(e.to_string());
                (false, error_type, error_message)
            }
        };
        
        let event = TelemetryEvent {
            trace_id,
            operation: operation_name,
            start_time: start,
            end_time: end,
            latency,
            status_code: None,
            request_payload_size: 0,
            response_payload_size: 0,
            error_type,
            error_message,
            target,
            success,
        };
        
        self.emit_event(event.clone());
        (result, event)
    }

    /// Emit a telemetry event
    pub fn emit_telemetry(&self, event: impl Into<String>) {
        // Legacy API - convert string to a basic event
        let event_str = event.into();
        let telemetry_event = TelemetryEvent::builder("legacy", "unknown")
            .failed("legacy_event", event_str)
            .build();
        self.emit_event(telemetry_event);
    }

    /// Emit a structured telemetry event
    pub fn emit_event(&self, event: TelemetryEvent) {
        if let Some(callback) = self.callback.lock().unwrap().as_ref() {
            callback(event);
        }
    }

    /// Get the number of active operations
    pub fn active_operation_count(&self) -> usize {
        self.active_operations.lock().unwrap().len()
    }

    /// Get all active trace IDs
    pub fn active_trace_ids(&self) -> Vec<TraceId> {
        self.active_operations
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }
}

impl Default for TelemetryDecorator {
    fn default() -> Self {
        Self::new()
    }
}

/// Result wrapper that includes telemetry
pub struct TelemetryResult<T> {
    pub value: T,
    pub event: TelemetryEvent,
}

impl<T> TelemetryResult<T> {
    pub fn new(value: T, event: TelemetryEvent) -> Self {
        Self { value, event }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn test_telemetry_decorator_creates_trace_ids() {
        let decorator = TelemetryDecorator::new();
        let id1 = decorator.start_operation("test_op");
        let id2 = decorator.start_operation("test_op");
        
        assert_ne!(id1.0, id2.0, "Trace IDs should be unique");
    }

    #[test]
    fn test_wrap_outbound_measures_latency() {
        let decorator = TelemetryDecorator::new();
        
        let (result, event) = decorator.wrap_outbound("test_op", "test_target", || {
            std::thread::sleep(Duration::from_millis(10));
            42
        });
        
        assert_eq!(result, 42);
        assert!(event.latency >= Duration::from_millis(10), "Should measure at least 10ms");
        assert_eq!(event.operation, "test_op");
        assert_eq!(event.target, "test_target");
        assert!(event.success);
    }

    #[test]
    fn test_telemetry_callback_receives_events() {
        let decorator = TelemetryDecorator::new();
        let event_count = Arc::new(AtomicUsize::new(0));
        let event_count_clone = event_count.clone();
        
        decorator.register_callback(move |_event| {
            event_count_clone.fetch_add(1, Ordering::SeqCst);
        });
        
        decorator.wrap_outbound("op1", "target1", || ());
        decorator.wrap_outbound("op2", "target2", || ());
        
        assert_eq!(event_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_complete_operation_emits_telemetry() {
        let decorator = TelemetryDecorator::new();
        let received_event = Arc::new(Mutex::new(None));
        let received_clone = received_event.clone();
        
        decorator.register_callback(move |event| {
            *received_clone.lock().unwrap() = Some(event);
        });
        
        let trace_id = decorator.start_operation("my_operation");
        std::thread::sleep(Duration::from_millis(5));
        
        decorator.complete_operation(
            trace_id.clone(),
            Some(200),
            100,
            200,
            None,
        );
        
        let event = received_event.lock().unwrap().clone().expect("Event should be received");
        assert_eq!(event.operation, "my_operation");
        assert_eq!(event.status_code, Some(200));
        assert_eq!(event.request_payload_size, 100);
        assert_eq!(event.response_payload_size, 200);
        assert!(event.success);
    }

    #[test]
    fn test_complete_operation_with_error() {
        let decorator = TelemetryDecorator::new();
        let received_event = Arc::new(Mutex::new(None));
        let received_clone = received_event.clone();
        
        decorator.register_callback(move |event| {
            *received_clone.lock().unwrap() = Some(event);
        });
        
        let trace_id = decorator.start_operation("failing_op");
        decorator.complete_operation(
            trace_id,
            Some(500),
            50,
            0,
            Some(("ConnectionError".to_string(), "Failed to connect".to_string())),
        );
        
        let event = received_event.lock().unwrap().clone().expect("Event should be received");
        assert!(!event.success);
        assert_eq!(event.error_type, Some("ConnectionError".to_string()));
        assert_eq!(event.error_message, Some("Failed to connect".to_string()));
    }

    #[test]
    fn test_active_operations_tracking() {
        let decorator = TelemetryDecorator::new();
        
        assert_eq!(decorator.active_operation_count(), 0);
        
        let id1 = decorator.start_operation("op1");
        let id2 = decorator.start_operation("op2");
        
        assert_eq!(decorator.active_operation_count(), 2);
        
        let trace_ids = decorator.active_trace_ids();
        assert!(trace_ids.contains(&id1));
        assert!(trace_ids.contains(&id2));
        
        decorator.complete_operation(id1, None, 0, 0, None);
        assert_eq!(decorator.active_operation_count(), 1);
        
        decorator.complete_operation(id2, None, 0, 0, None);
        assert_eq!(decorator.active_operation_count(), 0);
    }

    #[test]
    fn test_event_builder() {
        let event = TelemetryEvent::builder("submit_order", "alpaca")
            .status_code(200)
            .request_payload_size(150)
            .response_payload_size(300)
            .build();
        
        assert_eq!(event.operation, "submit_order");
        assert_eq!(event.target, "alpaca");
        assert_eq!(event.status_code, Some(200));
        assert_eq!(event.request_payload_size, 150);
        assert_eq!(event.response_payload_size, 300);
        assert!(event.success);
    }

    #[test]
    fn test_event_builder_with_error() {
        let event = TelemetryEvent::builder("cancel_order", "alpaca")
            .status_code(404)
            .failed("OrderNotFound", "Order ID not found")
            .build();
        
        assert!(!event.success);
        assert_eq!(event.error_type, Some("OrderNotFound".to_string()));
        assert_eq!(event.error_message, Some("Order ID not found".to_string()));
    }

    #[test]
    fn test_telemetry_decorator_thread_safe() {
        let decorator = Arc::new(TelemetryDecorator::new());
        let event_count = Arc::new(AtomicUsize::new(0));
        let event_count_clone = event_count.clone();
        
        decorator.register_callback(move |_event| {
            event_count_clone.fetch_add(1, Ordering::SeqCst);
        });
        
        let mut handles = vec![];
        
        for i in 0..5 {
            let dec_clone = Arc::clone(&decorator);
            let handle = thread::spawn(move || {
                dec_clone.wrap_outbound(format!("op{}", i), "target", || {
                    std::thread::sleep(Duration::from_millis(1));
                });
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(event_count.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn test_has_callback_returns_false_initially() {
        let decorator = TelemetryDecorator::new();
        assert!(!decorator.has_callback());
    }

    #[test]
    fn test_has_callback_returns_true_after_register() {
        let decorator = TelemetryDecorator::new();
        decorator.register_callback(|_event| {});
        assert!(decorator.has_callback());
    }

    #[test]
    fn test_emit_telemetry_legacy_api() {
        let decorator = TelemetryDecorator::new();
        let received = Arc::new(Mutex::new(None));
        let received_clone = received.clone();
        
        decorator.register_callback(move |event| {
            *received_clone.lock().unwrap() = Some(event);
        });
        
        decorator.emit_telemetry("some event");
        
        let event = received.lock().unwrap().clone().expect("Event should be received");
        assert!(!event.success); // Legacy events marked as failed
        assert_eq!(event.error_message, Some("some event".to_string()));
    }

    #[test]
    fn test_trace_id_uniqueness() {
        let mut ids = std::collections::HashSet::new();
        for _ in 0..1000 {
            let id = TraceId::new();
            assert!(ids.insert(id), "Duplicate trace ID generated");
        }
    }

    // =========================================================================
    // NEW: Result-based error tracking tests
    // =========================================================================

    #[derive(Debug)]
    enum TestError {
        NotFound,
        RateLimited,
        ConnectionFailed(String),
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TestError::NotFound => write!(f, "Resource not found"),
                TestError::RateLimited => write!(f, "Rate limit exceeded"),
                TestError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            }
        }
    }

    fn classify_test_error(e: &TestError) -> &'static str {
        match e {
            TestError::NotFound => "not_found",
            TestError::RateLimited => "rate_limited",
            TestError::ConnectionFailed(_) => "connection_error",
        }
    }

    #[test]
    fn test_wrap_outbound_result_success() {
        let decorator = TelemetryDecorator::new();
        let received = Arc::new(Mutex::new(None));
        let received_clone = received.clone();
        
        decorator.register_callback(move |event| {
            *received_clone.lock().unwrap() = Some(event);
        });
        
        let (result, event) = decorator.wrap_outbound_result(
            "submit_order",
            "alpaca",
            || Ok::<i32, TestError>(42),
            Some(classify_test_error),
        );
        
        assert_eq!(result.unwrap(), 42);
        assert!(event.success);
        assert!(event.error_type.is_none());
        assert!(event.error_message.is_none());
        assert!(event.latency > Duration::from_nanos(0));
        
        // Verify callback received the event
        let received_event = received.lock().unwrap().clone().expect("Event should be received");
        assert!(received_event.success);
    }

    #[test]
    fn test_wrap_outbound_result_error_with_classification() {
        let decorator = TelemetryDecorator::new();
        let received = Arc::new(Mutex::new(None));
        let received_clone = received.clone();
        
        decorator.register_callback(move |event| {
            *received_clone.lock().unwrap() = Some(event);
        });
        
        let (result, event) = decorator.wrap_outbound_result(
            "submit_order",
            "alpaca",
            || Err::<i32, TestError>(TestError::RateLimited),
            Some(classify_test_error),
        );
        
        assert!(result.is_err());
        assert!(!event.success);
        assert_eq!(event.error_type, Some("rate_limited".to_string()));
        assert_eq!(event.error_message, Some("Rate limit exceeded".to_string()));
        
        // Verify callback received the error event
        let received_event = received.lock().unwrap().clone().expect("Event should be received");
        assert!(!received_event.success);
        assert_eq!(received_event.error_type, Some("rate_limited".to_string()));
    }

    #[test]
    fn test_wrap_outbound_result_error_without_classifier() {
        let decorator = TelemetryDecorator::new();
        
        let (result, event) = decorator.wrap_outbound_result(
            "submit_order",
            "alpaca",
            || Err::<i32, TestError>(TestError::NotFound),
            None, // No classifier
        );
        
        assert!(result.is_err());
        assert!(!event.success);
        assert!(event.error_type.is_none()); // No classifier = no error type
        assert_eq!(event.error_message, Some("Resource not found".to_string()));
    }

    #[test]
    fn test_wrap_outbound_result_different_error_types() {
        let decorator = TelemetryDecorator::new();
        
        // Test NotFound
        let (_, event) = decorator.wrap_outbound_result(
            "query",
            "alpaca",
            || Err::<i32, TestError>(TestError::NotFound),
            Some(classify_test_error),
        );
        assert_eq!(event.error_type, Some("not_found".to_string()));
        
        // Test ConnectionFailed
        let (_, event) = decorator.wrap_outbound_result(
            "submit",
            "alpaca",
            || Err::<i32, TestError>(TestError::ConnectionFailed("timeout".to_string())),
            Some(classify_test_error),
        );
        assert_eq!(event.error_type, Some("connection_error".to_string()));
        assert!(event.error_message.as_ref().unwrap().contains("timeout"));
    }

    #[test]
    fn test_wrap_outbound_result_measures_latency() {
        let decorator = TelemetryDecorator::new();
        
        let (_, event) = decorator.wrap_outbound_result(
            "slow_op",
            "alpaca",
            || {
                std::thread::sleep(Duration::from_millis(50));
                Ok::<i32, TestError>(42)
            },
            None,
        );
        
        assert!(event.latency >= Duration::from_millis(45));
    }

    #[test]
    fn test_wrap_outbound_result_preserves_result() {
        let decorator = TelemetryDecorator::new();
        
        // Success case
        let (result, _) = decorator.wrap_outbound_result(
            "test",
            "alpaca",
            || Ok::<i32, TestError>(123),
            None,
        );
        assert_eq!(result.unwrap(), 123);
        
        // Error case
        let (result, _) = decorator.wrap_outbound_result(
            "test",
            "alpaca",
            || Err::<i32, TestError>(TestError::NotFound),
            None,
        );
        assert!(matches!(result.unwrap_err(), TestError::NotFound));
    }

    #[tokio::test]
    async fn test_wrap_outbound_result_async_success() {
        let decorator = TelemetryDecorator::new();
        
        let (result, event) = decorator.wrap_outbound_result_async(
            "async_op",
            "alpaca",
            || async { Ok::<i32, TestError>(42) },
            Some(classify_test_error),
        ).await;
        
        assert_eq!(result.unwrap(), 42);
        assert!(event.success);
        assert!(event.error_type.is_none());
    }

    #[tokio::test]
    async fn test_wrap_outbound_result_async_error() {
        let decorator = TelemetryDecorator::new();
        
        let (result, event) = decorator.wrap_outbound_result_async(
            "async_op",
            "alpaca",
            || async { Err::<i32, TestError>(TestError::RateLimited) },
            Some(classify_test_error),
        ).await;
        
        assert!(result.is_err());
        assert!(!event.success);
        assert_eq!(event.error_type, Some("rate_limited".to_string()));
    }

    #[tokio::test]
    async fn test_wrap_outbound_result_async_measures_latency() {
        let decorator = TelemetryDecorator::new();
        
        let (_, event) = decorator.wrap_outbound_result_async(
            "slow_async",
            "alpaca",
            || async {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                Ok::<i32, TestError>(42)
            },
            None,
        ).await;
        
        assert!(event.latency >= Duration::from_millis(45));
    }
}
