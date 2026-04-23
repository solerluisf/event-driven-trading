// fix_adapter.rs

#[derive(Default)]
pub struct FixBrokerAdapter;

impl IBrokerAdapter for FixBrokerAdapter {
    fn connect(&self) -> Connection {
        Connection {
            conn_id: "fix-conn".into(),
        }
    }

    fn disconnect(&self) {}

    fn send(&self, req: RequestEnvelope) -> ResponseEnvelope {
        let _ = req;
        ResponseEnvelope {
            correlation_id: "fix".into(),
            ok: true,
        }
    }

    fn on_event(&self, _callback: Box<dyn Fn(String) + Send + Sync>) {}
}