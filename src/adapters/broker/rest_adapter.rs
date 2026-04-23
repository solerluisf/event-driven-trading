// rest_adapter.rs

#[derive(Default)]
pub struct RestBrokerAdapter {
    pub cb: CircuitBreaker,
}

impl IBrokerAdapter for RestBrokerAdapter {
    fn connect(&self) -> Connection {
        Connection {
            conn_id: "rest-conn".into(),
        }
    }

    fn disconnect(&self) {}

    fn send(&self, req: RequestEnvelope) -> ResponseEnvelope {
        let _ = req;
        ResponseEnvelope {
            correlation_id: "rest".into(),
            ok: true,
        }
    }

    fn on_event(&self, _callback: Box<dyn Fn(String) + Send + Sync>) {}
}