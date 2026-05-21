// adapters/messaging/kill_switch_subscriber.rs
//
// SUB socket on system.kill_switch. Listens for kill-switch broadcasts
// from the Orchestrator.

use std::sync::Arc;
use zmq::Context;

use crate::core::application::kill_switch::KillSwitch;

pub struct KillSwitchSubscriber;

impl KillSwitchSubscriber {
    pub fn spawn(endpoint: &str, kill_switch: Arc<KillSwitch>) -> tokio::task::JoinHandle<()> {
        let endpoint = endpoint.to_string();

        tokio::spawn(async move {
            let ctx = Context::new();
            let socket = ctx.socket(zmq::SUB).expect("create SUB socket");

            if let Err(e) = socket.connect(&endpoint) {
                tracing::error!("KillSwitchSubscriber connect failed: {}", e);
                return;
            }

            if let Err(e) = socket.set_subscribe(b"system.kill_switch") {
                tracing::error!("KillSwitchSubscriber set_subscribe failed: {}", e);
                return;
            }

            tracing::info!("KillSwitchSubscriber listening on {}", endpoint);

            loop {
                let payload = match socket.recv_bytes(0) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!("KillSwitchSubscriber recv error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };

                Self::handle_message(&payload, &kill_switch);
            }
        })
    }

    fn handle_message(data: &[u8], kill_switch: &KillSwitch) {
        let text = String::from_utf8_lossy(data);

        #[derive(serde::Deserialize)]
        struct KillSwitchEvent {
            #[serde(rename = "event_type")]
            event_type: String,
            reason: Option<String>,
            actor: Option<String>,
        }

        let event: KillSwitchEvent = match serde_json::from_str(&text) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("KillSwitchSubscriber parse error: {}", e);
                return;
            }
        };

        match event.event_type.as_str() {
            "KillSwitchActivated" => {
                let reason = event.reason.as_deref().unwrap_or("unknown");
                let actor = event.actor.as_deref().unwrap_or("unknown");
                tracing::error!(
                    "Kill switch activated via bus: reason={} actor={}",
                    reason,
                    actor
                );
                kill_switch.enable();
            }
            "KillSwitchCleared" => {
                let reason = event.reason.as_deref().unwrap_or("unknown");
                let actor = event.actor.as_deref().unwrap_or("unknown");
                tracing::info!(
                    "Kill switch cleared via bus: reason={} actor={}",
                    reason,
                    actor
                );
                kill_switch.disable();
            }
            other => {
                tracing::warn!("KillSwitchSubscriber unknown event type: {}", other);
            }
        }
    }
}
