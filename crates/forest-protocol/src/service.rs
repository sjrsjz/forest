//! Each structure belongs to a dedicated host import, not a string opcode.
use crate::{Token, Value};
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

#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum ServiceError {
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
