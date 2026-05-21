// adapters/messaging/mode_subscriber.rs
//
// SUB socket on system.mode. Listens for mode change broadcasts.

use zmq::Context;

pub struct ModeSubscriber;

impl ModeSubscriber {
    pub fn spawn(endpoint: &str) -> tokio::task::JoinHandle<()> {
        let endpoint = endpoint.to_string();

        tokio::spawn(async move {
            let ctx = Context::new();
            let socket = ctx.socket(zmq::SUB).expect("create SUB socket");

            if let Err(e) = socket.connect(&endpoint) {
                tracing::error!("ModeSubscriber connect failed: {}", e);
                return;
            }

            if let Err(e) = socket.set_subscribe(b"system.mode") {
                tracing::error!("ModeSubscriber set_subscribe failed: {}", e);
                return;
            }

            tracing::info!("ModeSubscriber listening on {}", endpoint);

            loop {
                let payload = match socket.recv_bytes(0) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!("ModeSubscriber recv error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };

                Self::handle_message(&payload);
            }
        })
    }

    fn handle_message(data: &[u8]) {
        let text = String::from_utf8_lossy(data);

        #[derive(serde::Deserialize)]
        struct ModeEvent {
            #[serde(rename = "event_type")]
            event_type: String,
            to: Option<String>,
        }

        let event: ModeEvent = match serde_json::from_str(&text) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("ModeSubscriber parse error: {}", e);
                return;
            }
        };

        match event.event_type.as_str() {
            "ModeTransitionCompleted" => {
                if let Some(ref mode) = event.to {
                    tracing::info!("Mode transition completed via bus: to={}", mode);
                }
            }
            other => {
                tracing::warn!("ModeSubscriber unknown event type: {}", other);
            }
        }
    }
}
