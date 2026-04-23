pub trait BusAdapterTrait {
    fn pub_msg(&self, topic: String, msg: String);
    fn sub(&self, topic: String, handler: Box<dyn Fn(String) + Send + Sync>);
}
