// outbound_port.rs

pub trait IBrokerOutboundPort {
    fn send(&self, req: RequestEnvelope) -> ResponseEnvelope;
    fn open_connection(&self, config: ConnectionConfig);
    fn close_connection(&self, conn_id: String);
    fn heartbeat(&self, conn_id: String);
}