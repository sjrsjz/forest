use std::{borrow::Cow, error::Error, future::Future};

use serde::Deserialize;
use serde_json::Value;

use crate::action::Action;

/// Performs one exchange. Errors must return to this caller, not a background log.
pub trait Transport: Sync {
    type Error: Error + Send + Sync + 'static;

    fn request(
        &self,
        action: &str,
        params: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, Self::Error>> + Send;
}

/// OneBot execution mode, independent of Rust's asynchronous I/O.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CallMode {
    #[default]
    Normal,
    Async,
    RateLimited,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Ok,
    Async,
    Failed,
}

/// Submitted means the final success/failure is unknown, not an empty success.
#[must_use]
#[derive(Debug, PartialEq)]
pub enum Outcome<T> {
    Completed(T),
    Submitted,
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("OneBot API failed (retcode {retcode})")]
pub struct ApiFailure {
    pub retcode: i32,
    pub message: Option<String>,
    pub wording: Option<String>,
    pub data: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    #[error("inconsistent OneBot status {status:?} and retcode {retcode}")]
    InconsistentStatus { status: Status, retcode: i32 },
}

#[derive(Debug, thiserror::Error)]
pub enum CallError<E: Error + 'static> {
    #[error("transport failed: {0}")]
    Transport(#[source] E),
    #[error("request JSON encoding failed: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("response JSON decoding failed: {0}")]
    Decode(#[source] serde_json::Error),
    #[error(transparent)]
    Protocol(ProtocolError),
    #[error(transparent)]
    Api(ApiFailure),
}

#[derive(Deserialize)]
struct Envelope {
    status: Status,
    retcode: i32,
    #[serde(default)]
    data: Value,
    message: Option<String>,
    wording: Option<String>,
}

pub struct Client<T> {
    transport: T,
}

impl<T> Client<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }
}

impl<T: Transport> Client<T> {
    pub async fn call<A: Action>(
        &self,
        action: &A,
    ) -> Result<Outcome<A::Response>, CallError<T::Error>> {
        self.call_with_mode(action, CallMode::Normal).await
    }

    pub async fn call_with_mode<A: Action>(
        &self,
        action: &A,
        mode: CallMode,
    ) -> Result<Outcome<A::Response>, CallError<T::Error>> {
        let name = match mode {
            CallMode::Normal => Cow::Borrowed(A::NAME),
            CallMode::Async => Cow::Owned(format!("{}_async", A::NAME)),
            CallMode::RateLimited => Cow::Owned(format!("{}_rate_limited", A::NAME)),
        };
        let params = serde_json::to_vec(action).map_err(CallError::Encode)?;
        let bytes = self
            .transport
            .request(&name, params)
            .await
            .map_err(CallError::Transport)?;
        let response: Envelope = serde_json::from_slice(&bytes).map_err(CallError::Decode)?;
        match (response.status, response.retcode) {
            (Status::Ok, 0) => serde_json::from_value(response.data)
                .map(Outcome::Completed)
                .map_err(CallError::Decode),
            (Status::Async, 1) => Ok(Outcome::Submitted),
            (Status::Failed, code) if code != 0 && code != 1 => Err(CallError::Api(ApiFailure {
                retcode: code,
                message: response.message,
                wording: response.wording,
                data: response.data,
            })),
            (status, retcode) => Err(CallError::Protocol(ProtocolError::InconsistentStatus {
                status,
                retcode,
            })),
        }
    }
}
