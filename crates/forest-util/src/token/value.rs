//! Forest represents a concrete text token as its canonical absolute path.
use super::Token;
use forest_serde::adt::{AsSchema, AsValue, DecodeError, GlobalSchemaEnum, Value};
use std::{any::TypeId, collections::HashMap};

impl AsSchema for Token {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        let schema = String::register_dependencies_and_self(cache, detector);
        GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
        schema
    }
}

impl AsValue for Token {
    fn as_value(&self) -> Value {
        self.to_string().as_value()
    }

    fn from_value(value: Value) -> Result<Self, DecodeError> {
        String::from_value(value)?
            .parse()
            .map_err(|error: super::PathParseError| DecodeError::InvalidValue {
                message: error.to_string(),
            })
    }
}
