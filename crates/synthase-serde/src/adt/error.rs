use std::fmt;

use serde::{Deserialize, Serialize};

use super::{Value, VariantPayloadValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueKind {
    I32,
    I64,
    Bool,
    String,
    Bytes,
    Some,
    None,
    Tuple,
    Array,
    Map,
    Variant,
    Record,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayloadKind {
    Unnamed,
    Named,
}

/// Conversion failures are data; formatting does not determine their meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecodeError {
    TypeMismatch {
        expected: ValueKind,
        actual: ValueKind,
    },
    PayloadMismatch {
        expected: PayloadKind,
        actual: PayloadKind,
    },
    MissingField {
        field: String,
    },
    TooFewElements {
        minimum: usize,
        actual: usize,
    },
    UnknownVariant {
        tag: String,
    },
    DuplicateMapKey,
    AtField {
        field: String,
        source: Box<DecodeError>,
    },
    AtIndex {
        index: usize,
        source: Box<DecodeError>,
    },
}

impl DecodeError {
    pub fn expected(expected: ValueKind, actual: &Value) -> Self {
        Self::TypeMismatch {
            expected,
            actual: actual.kind(),
        }
    }

    pub fn at_field(self, field: impl Into<String>) -> Self {
        Self::AtField {
            field: field.into(),
            source: Box::new(self),
        }
    }

    pub fn at_index(self, index: usize) -> Self {
        Self::AtIndex {
            index,
            source: Box::new(self),
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch { expected, actual } => {
                write!(f, "expected {expected:?}, got {actual:?}")
            }
            Self::PayloadMismatch { expected, actual } => {
                write!(f, "expected {expected:?} payload, got {actual:?}")
            }
            Self::MissingField { field } => write!(f, "missing field {field:?}"),
            Self::TooFewElements { minimum, actual } => {
                write!(f, "expected at least {minimum} elements, got {actual}")
            }
            Self::UnknownVariant { tag } => write!(f, "unknown variant {tag:?}"),
            Self::DuplicateMapKey => {
                f.write_str("distinct encoded keys decode to the same map key")
            }
            Self::AtField { field, source } => write!(f, "at field {field:?}: {source}"),
            Self::AtIndex { index, source } => write!(f, "at index {index}: {source}"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AtField { source, .. } | Self::AtIndex { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl Value {
    pub fn kind(&self) -> ValueKind {
        match self {
            Self::I32(_) => ValueKind::I32,
            Self::I64(_) => ValueKind::I64,
            Self::Bool(_) => ValueKind::Bool,
            Self::String(_) => ValueKind::String,
            Self::Bytes(_) => ValueKind::Bytes,
            Self::Some(_) => ValueKind::Some,
            Self::None => ValueKind::None,
            Self::Tuple(_) => ValueKind::Tuple,
            Self::Array(_) => ValueKind::Array,
            Self::Map(_) => ValueKind::Map,
            Self::Variant(_, _) => ValueKind::Variant,
            Self::Record(_) => ValueKind::Record,
        }
    }
}

impl VariantPayloadValue {
    pub fn kind(&self) -> PayloadKind {
        match self {
            Self::Unnamed(_) => PayloadKind::Unnamed,
            Self::Named(_) => PayloadKind::Named,
        }
    }
}
