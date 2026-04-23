#[derive(Clone, Debug)]
pub struct RequestEnvelope {
    pub correlation_id: String,
}

#[derive(Clone, Debug)]
pub struct ResponseEnvelope {
    pub correlation_id: String,
    pub ok: bool,
}

#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub broker_id: String,
}

#[derive(Clone, Debug)]
pub struct Connection {
    pub conn_id: String,
}

#[derive(Clone, Debug)]
pub struct Health {
    pub ok: bool,
}

#[derive(Clone, Debug)]
pub struct BrokerId(pub String);

#[derive(Clone, Debug)]
pub struct BrokerAdapterError(pub String);
