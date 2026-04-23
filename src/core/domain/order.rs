// order.rs

#[derive(Clone, Debug)]
pub struct OrderCmd {
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct CancelCmd {
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct ReplaceCmd {
    pub id: String,
}

#[derive(Clone, Debug)]
pub struct StatusQuery {
    pub id: String,
}