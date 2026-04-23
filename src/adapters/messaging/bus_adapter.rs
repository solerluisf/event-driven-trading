// bus_adapter.rs

pub struct BusAdapter;

impl BusAdapterTrait for BusAdapter {
    fn pub_msg(&self, _topic: String, _msg: String) {}
    fn sub(&self, _topic: String, _handler: Box<dyn Fn(String) + Send + Sync>) {}
}