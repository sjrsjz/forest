//! Each structure belongs to a dedicated host import, not a string opcode.
use crate::{Token, Value};
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallRequest {
    pub object: Token,
    pub interface: Token,
    pub input: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigKey {
    pub object: Token,
    pub key: Token,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigSet {
    pub target: ConfigKey,
    pub value: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub timeout_ms: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebSocketRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub timeout_ms: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WebSocketEvent {
    Message(WebSocketMessage),
    Closed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebSocketSend {
    pub handle: u64,
    pub message: WebSocketMessage,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub enum IoEvent {
    WebSocketReady(i64),
    Timer(i64),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebSocketWatch {
    pub handle: u64,
    pub interface: Token,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerRequest {
    pub delay_ms: u64,
    pub interface: Token,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmitRequest {
    pub event: Token,
    pub input: Value,
    pub subject: Option<Token>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventDelivery {
    pub object: Token,
    pub interface: Token,
    pub result: crate::DispatchResult<()>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileWrite {
    pub path: String,
    pub bytes: Vec<u8>,
}

/// SQLite's five storage classes. Floats belong to this I/O protocol, not Forest Value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlStatement {
    pub sql: String,
    pub parameters: Vec<SqlValue>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlRows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub changed: u64,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogRecord {
    pub level: LogLevel,
    pub message: String,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, DeriveSchema, DeriveValue,
)]
pub enum ServiceError {
    #[error("operation timed out")]
    Timeout,
    #[error("operating system randomness unavailable")]
    RandomUnavailable,
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("resource not found")]
    NotFound,
    #[error("resource is in use")]
    InUse,
    #[error("transport failed: {0}")]
    Transport(String),
    #[error("storage failed: {0}")]
    Storage(String),
}
pub type HostResult<T> = crate::DispatchResult<Result<T, ServiceError>>;
