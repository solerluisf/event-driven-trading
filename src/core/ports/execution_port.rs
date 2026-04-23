// execution_port.rs

pub trait IExecutionPort {
    fn submit(&self, cmd: OrderCmd);
    fn cancel(&self, cmd: CancelCmd);
    fn replace(&self, cmd: ReplaceCmd);
    fn status_query(&self, query: StatusQuery);
}

impl IExecutionPort for GatewayService {
    fn submit(&self, _cmd: OrderCmd) {}
    fn cancel(&self, _cmd: CancelCmd) {}
    fn replace(&self, _cmd: ReplaceCmd) {}
    fn status_query(&self, _query: StatusQuery) {}
}

