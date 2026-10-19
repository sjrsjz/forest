use std::marker::PhantomData;

use forest_serde::adt::{AsSchema, AsValue, Schema, Value};
use forest_util::token::Token;

use crate::ConfigError;

/// A declaration always includes a checked default. Fields cannot be replaced
/// through ordinary value writes.
#[derive(Clone, Debug)]
pub struct Declaration {
    pub(crate) key: Token,
    pub(crate) schema: Schema,
    pub(crate) default: Value,
}

impl Declaration {
    pub fn new(key: Token, schema: Schema, default: Value) -> Result<Self, ConfigError> {
        if !default.check(&schema, false) {
            return Err(ConfigError::InvalidDefault { key });
        }
        Ok(Self {
            key,
            schema,
            default,
        })
    }
}

/// Every entry is fully initialized when its entity is registered.
#[derive(Clone, Debug)]
pub struct Entry {
    pub(crate) schema: Schema,
    pub(crate) default: Value,
    pub(crate) value: Value,
}

impl Entry {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    pub fn default_value(&self) -> &Value {
        &self.default
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
}

/// A typed path, not authority to read or modify that path.
#[derive(Debug)]
pub struct Key<T> {
    path: Token,
    schema: Schema,
    marker: PhantomData<fn() -> T>,
}

impl<T> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            schema: self.schema.clone(),
            marker: PhantomData,
        }
    }
}

impl<T: AsSchema> Key<T> {
    pub fn new(path: Token) -> Self {
        Self {
            path,
            schema: T::as_schema(),
            marker: PhantomData,
        }
    }
}

impl<T> Key<T> {
    pub fn path(&self) -> &Token {
        &self.path
    }

    pub(crate) fn check(&self, entity: &Token, entry: &Entry) -> Result<(), ConfigError> {
        if !entry.schema.subof(&self.schema) || !self.schema.subof(&entry.schema) {
            return Err(ConfigError::KeyTypeMismatch {
                entity: entity.clone(),
                key: self.path.clone(),
            });
        }
        Ok(())
    }
}

impl<T: AsValue> Key<T> {
    pub fn declare(&self, default: &T) -> Result<Declaration, ConfigError> {
        Declaration::new(self.path.clone(), self.schema.clone(), default.as_value())
    }

    pub fn set(&self, entity: Token, value: &T) -> Change {
        Change {
            entity,
            key: self.path.clone(),
            value: WriteValue::Typed {
                schema: self.schema.clone(),
                value: value.as_value(),
            },
        }
    }
}

/// A data-only change, applied only by an explicit store operation.
#[derive(Clone, Debug)]
pub struct Change {
    pub(crate) entity: Token,
    pub(crate) key: Token,
    pub(crate) value: WriteValue,
}

#[derive(Clone, Debug)]
pub(crate) enum WriteValue {
    Raw(Value),
    Typed { schema: Schema, value: Value },
    Default,
}

impl Change {
    pub fn set(entity: Token, key: Token, value: Value) -> Self {
        Self {
            entity,
            key,
            value: WriteValue::Raw(value),
        }
    }

    pub fn reset(entity: Token, key: Token) -> Self {
        Self {
            entity,
            key,
            value: WriteValue::Default,
        }
    }
}
