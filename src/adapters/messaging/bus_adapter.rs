// adapters/messaging/bus_adapter.rs
//
// Binds a ZeroMQ REP socket and drives the main command-listening loop.
// Every inbound frame is deserialized as a GatewayRequest, routed to
// GatewayService, and the result is serialized back as a GatewayResponse.
//
// Socket topology:
//
//   Execution Service (REQ)  ──►  Gateway (REP, this file)
//
// One message in, one reply out — exactly REQ/REP semantics.

use std::sync::Arc;
use std::sync::Mutex;
use zmq::Context;

use crate::core::application::gateway_service::GatewayService;
use crate::core::domain::wire_message::{
    GatewayRequest, GatewayResponse, ResponsePayload, ErrorPayload,
};

pub struct BusAdapter {
    /// ZeroMQ endpoint string, e.g. "tcp://127.0.0.1:5555"
    endpoint: String,
    gateway: Arc<GatewayService>,
}

impl BusAdapter {
    pub fn new(endpoint: impl Into<String>, gateway: Arc<GatewayService>) -> Self {
        Self {
            endpoint: endpoint.into(),
            gateway,
        }
    }

    /// Bind the REP socket and loop forever processing commands.
    /// Call this as the main async task in main().
    pub async fn listen(&self) -> Result<(), Box<dyn std::error::Error>> {
        let ctx = Context::new();
        let socket = Arc::new(Mutex::new(ctx.socket(zmq::REP)?));
        socket.lock().unwrap().bind(&self.endpoint)?;

        tracing::info!("BusAdapter listening on {}", self.endpoint);

        loop {
            let socket_clone = socket.clone();
            let gateway_clone = self.gateway.clone();

            // Receive message in blocking context
            let dispatch_result = tokio::task::spawn_blocking(move || {
                let socket = socket_clone.lock().unwrap();
                socket.recv_bytes(0) // 0 = blocking recv
            })
            .await;

            let msg = match dispatch_result {
                Ok(Ok(m)) => m,
                Ok(Err(_)) => continue,
                Err(e) => {
                    tracing::warn!("Task join error: {}", e);
                    continue;
                }
            };

            // Deserialize
            let response: GatewayResponse = match serde_json::from_slice(&msg) {
                Err(e) => {
                    tracing::warn!("Failed to deserialize request: {}", e);
                    GatewayResponse::Err(ErrorPayload {
                        correlation_id: None,
                        code: "DESERIALIZE_ERROR".into(),
                        message: e.to_string(),
                    })
                }
                Ok(req) => self.dispatch(req).await,
            };

            // Serialize
            let reply_bytes = serde_json::to_vec(&response).unwrap_or_else(|e| {
                format!(r#"{{"status":"err","payload":{{"code":"SERIALIZE_ERROR","message":"{}"}}}}"#, e)
                    .into_bytes()
            });

            // Send response in blocking context
            let socket_clone = socket.clone();
            let send_result = tokio::task::spawn_blocking(move || {
                let socket = socket_clone.lock().unwrap();
                socket.send(&reply_bytes, 0)
            })
            .await;

            if let Ok(Err(e)) = send_result {
                tracing::warn!("Failed to send response: {}", e);
            }
        }
    }

    /// Route a deserialized request to the correct GatewayService method.
    async fn dispatch(&self, req: GatewayRequest) -> GatewayResponse {
        match req {
            GatewayRequest::SubmitOrder(cmd) => {
                let correlation_id = cmd.client_order_id.clone();
                match self.gateway.submit_order(cmd).await {
                    Ok(exec_id) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: exec_id.0,
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "SUBMIT_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::CancelOrder(cmd) => {
                let correlation_id = Some(cmd.execution_id.0.clone());
                match self.gateway.cancel_order(cmd).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "cancelled".into(),
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "CANCEL_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::ReplaceOrder(cmd) => {
                let correlation_id = Some(cmd.execution_id.0.clone());
                match self.gateway.replace_order(cmd).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "replaced".into(),
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "REPLACE_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::QueryStatus(query) => {
                let correlation_id = Some(query.execution_id.0.clone());
                match self.gateway.query_status(query).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "ok".into(),
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "QUERY_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::Subscribe(sub) => {
                let correlation_id = Some(sub.symbol.clone());
                match self.gateway.subscribe(sub).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "subscribed".into(),
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "SUBSCRIBE_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }

            GatewayRequest::Unsubscribe(sub) => {
                let correlation_id = Some(sub.symbol.clone());
                match self.gateway.unsubscribe(sub).await {
                    Ok(()) => GatewayResponse::Ok(ResponsePayload {
                        correlation_id,
                        result: "unsubscribed".into(),
                    }),
                    Err(e) => GatewayResponse::Err(ErrorPayload {
                        correlation_id,
                        code: "UNSUBSCRIBE_FAILED".into(),
                        message: e.to_string(),
                    }),
                }
            }
        }
    }
}
