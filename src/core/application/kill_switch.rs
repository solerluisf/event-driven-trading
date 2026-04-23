#[derive(Default, Clone)]
pub struct KillSwitch {
    enabled: Arc<Mutex<bool>>,
}

impl KillSwitch {
    pub fn is_enabled(&self) -> bool {
        *self.enabled.lock().unwrap()
    }

    pub fn enable(&self) {
        *self.enabled.lock().unwrap() = true;
    }

    pub fn disable(&self) {
        *self.enabled.lock().unwrap() = false;
    }
}