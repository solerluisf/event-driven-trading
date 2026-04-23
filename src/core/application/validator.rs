// validator.rs

pub struct RequestValidator;

impl RequestValidator {
    pub fn validate(&self, req: RequestEnvelope) -> ValidatedRequest {
        ValidatedRequest { envelope: req }
    }
}


