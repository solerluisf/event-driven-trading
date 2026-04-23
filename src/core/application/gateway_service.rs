// gateway_service.rs

pub struct GatewayService {
    adapter_factory: AdapterFactory,
    connection_manager: ConnectionManager,
    validator: RequestValidator,
    idempotency: IdempotencyStore,
    journal: Arc<dyn IJournalRepo + Send + Sync>,
    rate_limiter: RateLimiterManager,
    telemetry: TelemetryDecorator,
    kill_switch: KillSwitch,
    observability: Arc<dyn IObservability + Send + Sync>,
}

impl GatewayService {
    pub fn handle_execution_message(&self, _msg: ExecutionMessage) {}
    pub fn handle_market_data(&self, _msg: Message) {}
    pub fn shutdown(&self) {}
    pub fn enter_simulation_mode(&self) {}
}

impl IExecutionPort for GatewayService {
    fn submit(&self, _cmd: OrderCmd) {}
    fn cancel(&self, _cmd: CancelCmd) {}
    fn replace(&self, _cmd: ReplaceCmd) {}
    fn status_query(&self, _query: StatusQuery) {}
}

impl IMarketDataPort for GatewayService {
    fn subscribe(&self, _sub: MarketSubscription) {}
    fn unsubscribe(&self, _sub: MarketSubscription) {}
}