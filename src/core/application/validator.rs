// validator.rs
use crate::core::domain::request::RequestEnvelope;
use crate::core::domain::journal::ValidatedRequest;


pub struct RequestValidator;

impl RequestValidator {
    pub fn validate(&self, req: RequestEnvelope) -> ValidatedRequest {
        ValidatedRequest { envelope: req }
    }
}


