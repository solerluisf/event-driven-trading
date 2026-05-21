// adapters/messaging/heartbeat_publisher.rs
//
// PUB socket publishing GatewayHealthSnapshot to service.broker_gateway.health
// every 5 seconds. Actor pattern.

use tokio::sync::mpsc;
use zmq::Context;

use crate::core::domain::gateway_health::GatewayHealthSnapshot;

pub struct HeartbeatPublisher;

impl HeartbeatPublisher {
    pub fn spawn(
        endpoint: &str,
    ) -> (mpsc::Sender<GatewayHealthSnapshot>, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<GatewayHealthSnapshot>(32);
        let endpoint = endpoint.to_string();

        let handle = tokio::spawn(async move {
            let ctx = Context::new();
            let socket = match ctx.socket(zmq::PUB) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("HeartbeatPublisher socket creation failed: {}", e);
                    return;
                }
            };

            if let Err(e) = socket.bind(&endpoint) {
                tracing::error!("HeartbeatPublisher bind failed: {}", e);
                return;
            }

            tracing::info!("HeartbeatPublisher bound to {}", endpoint);

            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Publish on timer tick if no recent snapshot
                    }
                    Some(snapshot) = rx.recv() => {
                        Self::publish(&socket, &snapshot);
                    }
                    else => {
                        tracing::info!("HeartbeatPublisher channel closed, shutting down");
                        break;
                    }
                }
            }
        });

        (tx, handle)
    }

    fn publish(socket: &zmq::Socket, snapshot: &GatewayHealthSnapshot) {
        let json = match serde_json::to_string(snapshot) {
            Ok(j) => j,
            Err(e) => {
                tracing::error!("HeartbeatPublisher serialize error: {}", e);
                return;
            }
        };

        if let Err(e) = socket.send(json.as_bytes(), 0) {
            tracing::warn!("HeartbeatPublisher send error: {}", e);
        }
    }
}
