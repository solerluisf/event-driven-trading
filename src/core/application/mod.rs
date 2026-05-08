// core/application/mod.rs
pub mod connection_manager;
pub mod event_reactor;
pub mod gateway_service;
pub mod idempotency;
pub mod kill_switch;
pub mod observability_service;      // ✅ new
pub mod order_submission_service;   // ✅ new
pub mod rate_limiter;
pub mod risk_management_service;    // ✅ new
pub mod validator;
pub mod service_impls;             // ✅ new: implements the port traits on the services
