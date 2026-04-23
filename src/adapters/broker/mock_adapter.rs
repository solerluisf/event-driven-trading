// mock_adapter.rs

#[derive(Default)]
pub struct MockAdapter;

impl IBrokerAdapter for MockAdapter {
    fn connect(&self) -> Connection {
        Connection {
            conn_id: "mock-conn".into(),
        }
    }

    fn disconnect(&self) {}

    fn send(&self, req: RequestEnvelope) -> ResponseEnvelope {
        ResponseEnvelope {
            correlation_id: req.correlation_id,
            ok: true,
        }
    }

    fn on_event(&self, _callback: Box<dyn Fn(String) + Send + Sync>) {}
}