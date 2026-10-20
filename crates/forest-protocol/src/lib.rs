//! Shared data contracts. No runtime, transport, routing, or guest execution.
pub mod abi;
pub mod manifest;
pub mod service;

pub use forest_serde::adt::{AsSchema, AsValue, Schema, Value};
pub use forest_util::{
    acl::{AccessPolicy, SubjectPattern},
    token::{Token, TokenPattern},
};
pub use manifest::*;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum DispatchError {
    #[error("object not found: {0}")]
    ObjectNotFound(Token),
    #[error("interface not found: {0}")]
    InterfaceNotFound(Token),
    #[error("object is unavailable: {0}")]
    Unavailable(Token),
    #[error("permission denied")]
    Denied,
    #[error("operation is forbidden during this lifecycle phase")]
    LifecycleRestricted,
    #[error("request was not accepted: queue full")]
    QueueFull,
    #[error("input does not match the interface contract")]
    InputContract,
    #[error("output does not match the interface contract")]
    OutputContract,
    #[error("execution failed: {0}")]
    ExecutionFailed(String),
    #[error("execution interrupted by external management")]
    Interrupted,
    #[error("host has stopped")]
    HostStopped,
}

pub type DispatchResult<T> = Result<T, DispatchError>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallContext {
    pub origin: Token,
    pub caller: Token,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Invocation {
    pub interface: Token,
    pub input: Value,
    pub context: CallContext,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateContext {
    pub object: Token,
}
