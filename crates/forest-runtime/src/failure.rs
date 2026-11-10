//! Instance failures retain module data independently of its Rust implementation.
use forest_protocol::{DispatchError, Schema, Token, Value};
use serde::Serialize;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecyclePhase {
    Create,
    Destroy,
}

impl fmt::Display for LifecyclePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Create => "create",
            Self::Destroy => "destroy",
        })
    }
}

#[derive(Clone, Debug, Serialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InstanceFailure {
    #[error("{error}")]
    Dispatch { error: DispatchError },
    #[error("{phase} rejected: {error}")]
    Lifecycle {
        phase: LifecyclePhase,
        schema: Schema,
        error: Value,
    },
}

impl From<DispatchError> for InstanceFailure {
    fn from(error: DispatchError) -> Self {
        Self::Dispatch { error }
    }
}

impl InstanceFailure {
    /// A failed lifecycle does not become an ordinary interface error payload.
    pub(crate) fn dispatch_error(&self, object: &Token) -> DispatchError {
        match self {
            Self::Dispatch { error } => error.clone(),
            Self::Lifecycle { .. } => DispatchError::Unavailable(object.clone()),
        }
    }
}
