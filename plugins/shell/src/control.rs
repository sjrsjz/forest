//! Local Schema/Value mapping; this guest does not link the host implementation.
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/core/control";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Phase {
    Creating,
    Serving,
    Stopping,
    Destroying,
    Releasing,
    Failed(String),
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Endpoint {
    pub object: String,
    pub interface: String,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct ModuleView {
    pub object: String,
    pub version: String,
    pub name: Option<String>,
    pub phase: Phase,
    pub queued: i64,
    pub dependencies: Vec<Endpoint>,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Snapshot {
    pub parallelism: i64,
    pub modules: Vec<ModuleView>,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum LocalPackage {
    Ready {
        directory: String,
        object: String,
        version: String,
    },
    Invalid {
        directory: String,
        message: String,
    },
}
#[derive(Clone, Copy, Debug, DeriveSchema, DeriveValue)]
pub enum Mode {
    Drain,
    AtYield,
    Force,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Stop {
    pub object: String,
    pub mode: Mode,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct ConfigAddress {
    pub object: String,
    pub key: String,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct ConfigValue {
    pub target: ConfigAddress,
    /// Forest value text: lossless integers, variants, bytes and compound values.
    pub value: String,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct ConfigEntry {
    pub schema: String,
    pub default_value: String,
    pub value: String,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    Inspection(String),
    Management(String),
    Configuration(String),
    /// A synchronous request cannot wait for its own caller to drain.
    CallingModule,
}
