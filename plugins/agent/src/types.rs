use forest_serde::{DeriveSchema, DeriveValue};
pub const OBJECT: &str = "/module/agent";
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Settings {
    pub allowed_groups: Vec<i64>,
    pub bot_name: Option<String>,
    pub high_water: i64,
    pub low_water: i64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            allowed_groups: vec![],
            bot_name: None,
            high_water: 256,
            low_water: 128,
        }
    }
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Status {
    pub group: i64,
    pub working: bool,
    pub window_units: i64,
    pub queued: i64,
    pub diagnostics: Vec<String>,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    Memory(crate::memory::Error),
    Provider(crate::provider::Error),
    Context(crate::context::Error),
    Dispatch(String),
    InvalidConfiguration(String),
    Protocol(String),
    Delivery(String),
}
impl From<forest_protocol::DispatchError> for Error {
    fn from(e: forest_protocol::DispatchError) -> Self {
        Self::Dispatch(e.to_string())
    }
}
