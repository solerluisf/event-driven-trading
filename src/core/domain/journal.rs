#[derive(Clone, Debug)]
pub struct RequestRecord {
    pub id: String,
}


#[derive(Clone, Debug)]
pub struct ResponseRecord {
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct ValidatedRequest {
    pub envelope: RequestEnvelope,
}

