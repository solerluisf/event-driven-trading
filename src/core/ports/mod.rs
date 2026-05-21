// core/ports/mod.rs
pub mod bus_adapter;        // ✅ must be declared
pub mod execution_port;
pub mod event_handler;
pub mod health_pub_port;
pub mod journal_repo;
pub mod market_data_port;
pub mod observability;
pub mod orchestration_port;
pub mod service_traits;
