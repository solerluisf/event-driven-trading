// adapters/messaging/circuit_breaker_publisher.rs
//
// PUB socket on service.broker_gateway.circuit_breaker.
// Emits events when circuit breaker state changes.

use serde::Serialize;
use tokio::sync::mpsc;
use zmq::Context;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CircuitBreakerEvent {
    Opened {
        service: String,
        failure_count: u32,
    },
    Closed {
        service: String,
    },
    HalfOpen {
        service: String,
    },
}

pub struct CircuitBreakerPublisher;

impl CircuitBreakerPublisher {
    pub fn spawn(
        endpoint: &str,
    ) -> (mpsc::Sender<CircuitBreakerEvent>, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<CircuitBreakerEvent>(32);
        let endpoint = endpoint.to_string();

        let handle = tokio::spawn(async move {
            let ctx = Context::new();
            let socket = match ctx.socket(zmq::PUB) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("CircuitBreakerPublisher socket creation failed: {}", e);
                    return;
                }
            };

            if let Err(e) = socket.bind(&endpoint) {
                tracing::error!("CircuitBreakerPublisher bind failed: {}", e);
                return;
            }

            tracing::info!("CircuitBreakerPublisher bound to {}", endpoint);

            while let Some(event) = rx.recv().await {
                Self::publish(&socket, &event);
            }

            tracing::info!("CircuitBreakerPublisher shutting down");
        });

        (tx, handle)
    }

    fn publish(socket: &zmq::Socket, event: &CircuitBreakerEvent) {
        let json = match serde_json::to_string(event) {
            Ok(j) => j,
            Err(e) => {
                tracing::error!("CircuitBreakerPublisher serialize error: {}", e);
                return;
            }
        };

        if let Err(e) = socket.send(json.as_bytes(), 0) {
            tracing::warn!("CircuitBreakerPublisher send error: {}", e);
        }
    }
}
