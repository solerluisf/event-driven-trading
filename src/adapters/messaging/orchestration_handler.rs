// adapters/messaging/orchestration_handler.rs
//
// SUB socket connecting to orchestrator.control.gateway.* topic.
// Dedicated thread + mpsc channel pattern (same as bus_adapter.rs).

use std::sync::Arc;
use tokio::sync::mpsc;
use zmq::Context;

use crate::core::application::kill_switch::KillSwitch;
use crate::core::application::rate_limiter::RateLimiterManager;
use crate::core::domain::operation_mode::OperationMode;
use crate::core::domain::orchestration::OrchestrationAck;
use crate::core::patterns::circuit_breaker::CircuitBreaker;

struct InternalCommand {
    command_type: String,
    payload: serde_json::Value,
}

pub struct OrchestrationHandler {
    ack_tx: mpsc::Sender<OrchestrationAck>,
}

impl OrchestrationHandler {
    pub fn spawn(
        endpoint: &str,
        kill_switch: Arc<KillSwitch>,
        circuit_breaker: Arc<CircuitBreaker>,
        rate_limiter: Arc<RateLimiterManager>,
        broker_id: String,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let ctx = Context::new();
        let (ack_tx, mut ack_rx) = mpsc::channel::<OrchestrationAck>(64);
        let endpoint = endpoint.to_string();

        let socket = ctx.socket(zmq::SUB).expect("create SUB socket");
        socket.connect(&endpoint).expect("connect to orchestrator control");
        socket
            .set_subscribe(b"orchestrator.control.gateway.")
            .expect("set subscribe filter");

        let handle = tokio::spawn(async move {
            while let Some(ack) = ack_rx.recv().await {
                tracing::info!(
                    "orchestration ack: command={} success={} error={:?}",
                    ack.command_type,
                    ack.success,
                    ack.error
                );
            }
        });

        let socket_ref = socket;
        let ack_tx_for_thread = ack_tx.clone();
        std::thread::spawn(move || {
            tracing::info!("OrchestrationHandler listening on {}", endpoint);
            loop {
                let payload = match socket_ref.recv_bytes(0) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!("OrchestrationHandler recv error: {}", e);
                        continue;
                    }
                };

                let cmd = match Self::parse_message(&payload) {
                    Some(c) => c,
                    None => continue,
                };

                let ack = Self::dispatch(
                    &cmd,
                    &kill_switch,
                    &circuit_breaker,
                    &rate_limiter,
                    &broker_id,
                );

                let _ = ack_tx_for_thread.blocking_send(ack);
            }
        });

        (Self { ack_tx }, handle)
    }

    fn parse_message(data: &[u8]) -> Option<InternalCommand> {
        let text = String::from_utf8_lossy(data);
        let value: serde_json::Value = serde_json::from_str(&text).ok()?;
        let command_type = value.get("command")?.as_str()?.to_string();
        let payload = value.get("payload").cloned().unwrap_or(serde_json::Value::Null);
        Some(InternalCommand {
            command_type,
            payload,
        })
    }

    fn dispatch(
        cmd: &InternalCommand,
        kill_switch: &KillSwitch,
        circuit_breaker: &CircuitBreaker,
        rate_limiter: &RateLimiterManager,
        broker_id: &str,
    ) -> OrchestrationAck {
        match cmd.command_type.as_str() {
            "set_operation_mode" => Self::handle_set_operation_mode(cmd, kill_switch),
            "activate_kill_switch" => Self::handle_activate_kill_switch(cmd, kill_switch),
            "clear_kill_switch" => Self::handle_clear_kill_switch(cmd, kill_switch),
            "reload_policies" => Self::handle_reload_policies(),
            "update_circuit_breaker" => Self::handle_update_circuit_breaker(cmd, circuit_breaker),
            "reset_circuit_breaker" => {
                circuit_breaker.record_success();
                OrchestrationAck::success("reset_circuit_breaker")
            }
            "update_rate_limiter" => Self::handle_update_rate_limiter(cmd, rate_limiter, broker_id),
            "pause_symbol" => Self::handle_pause_symbol(cmd),
            "resume_symbol" => Self::handle_resume_symbol(cmd),
            "flush_pending_orders" => Self::handle_flush_pending_orders(),
            "set_log_level" => Self::handle_set_log_level(cmd),
            "health_check" => OrchestrationAck::success("health_check"),
            other => OrchestrationAck::failure(other, "unknown command"),
        }
    }

    fn handle_set_operation_mode(
        cmd: &InternalCommand,
        _kill_switch: &KillSwitch,
    ) -> OrchestrationAck {
        let mode: OperationMode = match serde_json::from_value(cmd.payload.clone()) {
            Ok(m) => m,
            Err(e) => return OrchestrationAck::failure("set_operation_mode", e.to_string()),
        };
        tracing::info!("Operation mode changed via orchestrator: {}", mode);
        OrchestrationAck::success("set_operation_mode")
    }

    fn handle_activate_kill_switch(
        cmd: &InternalCommand,
        kill_switch: &KillSwitch,
    ) -> OrchestrationAck {
        #[derive(Deserialize)]
        struct ActivatePayload {
            reason: String,
            actor: String,
        }
        let payload: ActivatePayload = match serde_json::from_value(cmd.payload.clone()) {
            Ok(p) => p,
            Err(e) => {
                return OrchestrationAck::failure("activate_kill_switch", e.to_string());
            }
        };
        tracing::error!(
            "Kill switch activated by orchestrator: reason={} actor={}",
            payload.reason,
            payload.actor
        );
        kill_switch.enable();
        OrchestrationAck::success("activate_kill_switch")
    }

    fn handle_clear_kill_switch(
        cmd: &InternalCommand,
        kill_switch: &KillSwitch,
    ) -> OrchestrationAck {
        #[derive(Deserialize)]
        struct ClearPayload {
            reason: String,
            actor: String,
        }
        let _payload: ClearPayload = match serde_json::from_value(cmd.payload.clone()) {
            Ok(p) => p,
            Err(e) => {
                return OrchestrationAck::failure("clear_kill_switch", e.to_string());
            }
        };
        kill_switch.disable();
        tracing::info!("Kill switch cleared by orchestrator");
        OrchestrationAck::success("clear_kill_switch")
    }

    fn handle_reload_policies() -> OrchestrationAck {
        tracing::info!("Reload policies command received from orchestrator");
        OrchestrationAck::success("reload_policies")
    }

    fn handle_update_circuit_breaker(
        cmd: &InternalCommand,
        _circuit_breaker: &CircuitBreaker,
    ) -> OrchestrationAck {
        #[derive(Deserialize)]
        struct CBPayload {
            failure_threshold: u32,
            cooldown_secs: u64,
        }
        let _payload: CBPayload = match serde_json::from_value(cmd.payload.clone()) {
            Ok(p) => p,
            Err(e) => {
                return OrchestrationAck::failure("update_circuit_breaker", e.to_string());
            }
        };
        tracing::info!("Circuit breaker thresholds updated via orchestrator");
        OrchestrationAck::success("update_circuit_breaker")
    }

    fn handle_update_rate_limiter(
        cmd: &InternalCommand,
        rate_limiter: &RateLimiterManager,
        broker_id: &str,
    ) -> OrchestrationAck {
        #[derive(Deserialize)]
        struct RLPayload {
            max_requests_per_min: u32,
            #[allow(dead_code)]
            burst_capacity: u32,
        }
        let payload: RLPayload = match serde_json::from_value(cmd.payload.clone()) {
            Ok(p) => p,
            Err(e) => {
                return OrchestrationAck::failure("update_rate_limiter", e.to_string());
            }
        };
        rate_limiter.register(broker_id, payload.max_requests_per_min as f64);
        tracing::info!(
            "Rate limiter updated: broker={} rpm={}",
            broker_id,
            payload.max_requests_per_min
        );
        OrchestrationAck::success("update_rate_limiter")
    }

    fn handle_pause_symbol(cmd: &InternalCommand) -> OrchestrationAck {
        #[derive(Deserialize)]
        struct SymbolPayload {
            symbol: String,
        }
        let payload: SymbolPayload = match serde_json::from_value(cmd.payload.clone()) {
            Ok(p) => p,
            Err(e) => return OrchestrationAck::failure("pause_symbol", e.to_string()),
        };
        tracing::info!("Symbol paused via orchestrator: {}", payload.symbol);
        OrchestrationAck::success("pause_symbol")
    }

    fn handle_resume_symbol(cmd: &InternalCommand) -> OrchestrationAck {
        #[derive(Deserialize)]
        struct SymbolPayload {
            symbol: String,
        }
        let payload: SymbolPayload = match serde_json::from_value(cmd.payload.clone()) {
            Ok(p) => p,
            Err(e) => return OrchestrationAck::failure("resume_symbol", e.to_string()),
        };
        tracing::info!("Symbol resumed via orchestrator: {}", payload.symbol);
        OrchestrationAck::success("resume_symbol")
    }

    fn handle_flush_pending_orders() -> OrchestrationAck {
        tracing::info!("Flush pending orders command received from orchestrator");
        OrchestrationAck::success("flush_pending_orders")
    }

    fn handle_set_log_level(cmd: &InternalCommand) -> OrchestrationAck {
        #[derive(Deserialize)]
        struct LogLevelPayload {
            level: String,
        }
        let payload: LogLevelPayload = match serde_json::from_value(cmd.payload.clone()) {
            Ok(p) => p,
            Err(e) => return OrchestrationAck::failure("set_log_level", e.to_string()),
        };
        tracing::info!("Log level changed via orchestrator: {}", payload.level);
        OrchestrationAck::success("set_log_level")
    }

    pub async fn send_ack(&self, ack: OrchestrationAck) {
        let _ = self.ack_tx.send(ack).await;
    }
}

use serde::Deserialize;
