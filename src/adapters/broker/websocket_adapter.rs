// websocket_adapter.rs

#[derive(Default)]
pub struct WebSocketBrokerAdapter;

impl IBrokerAdapter for WebSocketBrokerAdapter {
    fn connect(&self) -> Connection {
        Connection {
            conn_id: "ws-conn".into(),
        }
    }

    fn disconnect(&self) {}

    fn send(&self, req: RequestEnvelope) -> ResponseEnvelope {
        let _ = req;
        ResponseEnvelope {
            correlation_id: "ws".into(),
            ok: true,
        }
    }

    fn on_event(&self, _callback: Box<dyn Fn(String) + Send + Sync>) {}
}
