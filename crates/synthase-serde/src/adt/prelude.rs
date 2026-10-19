//! This module provides default implementations of `AsSchema` and `AsValue` for common Rust types,
//! including primitive types, `Option`, `Vec`, `Box`, `Rc`, `Arc`, tuples, `Result`, and maps like `HashMap` and `BTreeMap`.
//! These implementations allow these types to be easily used within the Synthase serialization framework without requiring manual schema or value definitions.

use std::{
    any::TypeId,
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

use synthase_serde_macros::{__impl_as_schema_for_tuples, __impl_as_value_for_tuples};

use crate::adt::{
    AsSchema, AsValue, DecodeError, GlobalSchemaEnum, PayloadKind, Value, ValueKind,
    VariantPayloadGlobal, VariantPayloadValue,
};

// ==== Default implementations for primitive types and collection wrappers ====

macro_rules! __impl_as_schema_for_primitive {
    ($t:ty, $variant:ident) => {
        impl AsSchema for $t {
            fn register_dependencies_and_self(
                cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
                _rec_detector: &mut Vec<TypeId>,
            ) -> GlobalSchemaEnum {
                GlobalSchemaEnum::alloc::<Self>(GlobalSchemaEnum::$variant, cache);
                GlobalSchemaEnum::$variant
            }
        }
    };
}
__impl_as_schema_for_primitive!(i32, I32);
__impl_as_schema_for_primitive!(i64, I64);
__impl_as_schema_for_primitive!(bool, Bool);
__impl_as_schema_for_primitive!(String, String);
__impl_as_schema_for_tuples!(12);

impl<T: AsSchema + 'static> AsSchema for Option<T> {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let inner = T::register_dependencies_and_self(cache, rec_detector);
        rec_detector.pop();
        let schema = GlobalSchemaEnum::Option(Box::new(inner));
        GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
        schema
    }
}

impl<T: AsSchema + 'static> AsSchema for Vec<T> {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let inner = T::register_dependencies_and_self(cache, rec_detector);
        rec_detector.pop();
        let schema = GlobalSchemaEnum::Array(Box::new(inner));
        GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
        schema
    }
}

impl<T: AsSchema + 'static> AsSchema for Box<T> {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let inner_schema = T::register_dependencies_and_self(cache, rec_detector);
        rec_detector.pop();
        // Map the Box<T> TypeId to the exact same schema as T, providing transparent wrapping.
        GlobalSchemaEnum::alloc::<Self>(inner_schema.clone(), cache);
        inner_schema
    }
}

impl<T: AsSchema + 'static> AsSchema for std::rc::Rc<T> {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let inner_schema = T::register_dependencies_and_self(cache, rec_detector);
        rec_detector.pop();
        GlobalSchemaEnum::alloc::<Self>(inner_schema.clone(), cache);
        inner_schema
    }
}

impl<T: AsSchema + 'static> AsSchema for std::sync::Arc<T> {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let inner_schema = T::register_dependencies_and_self(cache, rec_detector);
        rec_detector.pop();
        GlobalSchemaEnum::alloc::<Self>(inner_schema.clone(), cache);
        inner_schema
    }
}

macro_rules! __impl_as_value_for_primitive {
    ($t:ty, $variant:ident) => {
        impl AsValue for $t {
            fn as_value(&self) -> Value {
                Value::$variant(self.clone())
            }
            fn from_value(value: Value) -> Result<Self, DecodeError> {
                match value {
                    Value::$variant(v) => Ok(v),
                    _ => Err(DecodeError::expected(ValueKind::$variant, &value)),
                }
            }
        }
    };
}
__impl_as_value_for_primitive!(i32, I32);
__impl_as_value_for_primitive!(bool, Bool);
__impl_as_value_for_primitive!(String, String);
__impl_as_value_for_tuples!(12);

impl AsValue for i64 {
    fn as_value(&self) -> Value {
        Value::I64(*self)
    }

    fn from_value(value: Value) -> Result<Self, DecodeError> {
        match value {
            Value::I64(value) => Ok(value),
            Value::I32(value) => Ok(i64::from(value)),
            _ => Err(DecodeError::expected(ValueKind::I64, &value)),
        }
    }
}

impl<T: AsValue + 'static> AsValue for Option<T> {
    fn as_value(&self) -> Value {
        match self {
            Some(inner) => Value::Some(Box::new(inner.as_value())),
            None => Value::None,
        }
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        match value {
            Value::Some(inner) => Ok(Some(T::from_value(*inner)?)),
            Value::None => Ok(None),
            _ => Err(DecodeError::expected(ValueKind::Some, &value)),
        }
    }
}

impl<T: AsValue + 'static> AsValue for Vec<T> {
    fn as_value(&self) -> Value {
        Value::Array(self.iter().map(|e| e.as_value()).collect())
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        match value {
            Value::Array(elems) => elems
                .into_iter()
                .enumerate()
                .map(|(index, value)| T::from_value(value).map_err(|error| error.at_index(index)))
                .collect(),
            _ => Err(DecodeError::expected(ValueKind::Array, &value)),
        }
    }
}

impl<T: AsValue + 'static> AsValue for Box<T> {
    fn as_value(&self) -> Value {
        self.as_ref().as_value()
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        Ok(Box::new(T::from_value(value)?))
    }
}

impl<T: AsValue + 'static> AsValue for std::sync::Arc<T> {
    fn as_value(&self) -> Value {
        self.as_ref().as_value()
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        Ok(std::sync::Arc::new(T::from_value(value)?))
    }
}

impl<T: AsValue + 'static> AsValue for std::rc::Rc<T> {
    fn as_value(&self) -> Value {
        self.as_ref().as_value()
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        Ok(std::rc::Rc::new(T::from_value(value)?))
    }
}

// ---------- () Unit Type ----------
impl AsSchema for () {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        _rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        let schema = GlobalSchemaEnum::Tuple(vec![]);
        GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
        schema
    }
}

impl AsValue for () {
    fn as_value(&self) -> Value {
        Value::Tuple(vec![])
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        match value {
            Value::Tuple(_) | Value::Record(_) => Ok(()),
            _ => Err(DecodeError::expected(ValueKind::Tuple, &value)),
        }
    }
}

// ---------- Array [T; N] ----------
impl<T: AsSchema, const N: usize> AsSchema for [T; N] {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let fields = if N == 0 {
            vec![]
        } else {
            T::register_dependencies_and_self(cache, rec_detector);
            vec![GlobalSchemaEnum::Ref(TypeId::of::<T>()); N]
        };
        rec_detector.pop();
        let schema = GlobalSchemaEnum::Tuple(fields);
        GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
        schema
    }
}

impl<T: AsValue, const N: usize> AsValue for [T; N] {
    fn as_value(&self) -> Value {
        Value::Tuple(self.iter().map(AsValue::as_value).collect())
    }

    fn from_value(value: Value) -> Result<Self, DecodeError> {
        let values = match value {
            Value::Tuple(values) => values,
            Value::Record(_) if N == 0 => vec![],
            _ => return Err(DecodeError::expected(ValueKind::Tuple, &value)),
        };
        let actual = values.len();
        if actual < N {
            return Err(DecodeError::TooFewElements { minimum: N, actual });
        }
        let values: Vec<T> = values
            .into_iter()
            .take(N)
            .enumerate()
            .map(|(index, value)| T::from_value(value).map_err(|error| error.at_index(index)))
            .collect::<Result<_, _>>()?;
        values
            .try_into()
            .map_err(|_| DecodeError::TooFewElements { minimum: N, actual })
    }
}

// ---------- Result<T, E> represented as a Dual-Variant ADT ----------
impl<T: AsSchema + 'static, E: AsSchema + 'static> AsSchema for Result<T, E> {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let ok_schema = T::register_dependencies_and_self(cache, rec_detector);
        let err_schema = E::register_dependencies_and_self(cache, rec_detector);
        rec_detector.pop();
        let mut variants = BTreeMap::new();
        variants.insert(
            "Ok".to_string(),
            VariantPayloadGlobal::Unnamed(vec![ok_schema]),
        );
        variants.insert(
            "Err".to_string(),
            VariantPayloadGlobal::Unnamed(vec![err_schema]),
        );
        let schema = GlobalSchemaEnum::Variant(variants);
        GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
        schema
    }
}

impl<T: AsValue + 'static, E: AsValue + 'static> AsValue for Result<T, E> {
    fn as_value(&self) -> Value {
        match self {
            Ok(val) => Value::Variant(
                "Ok".to_string(),
                VariantPayloadValue::Unnamed(vec![val.as_value()]),
            ),
            Err(err) => Value::Variant(
                "Err".to_string(),
                VariantPayloadValue::Unnamed(vec![err.as_value()]),
            ),
        }
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        match value {
            Value::Variant(tag, payload) => match tag.as_str() {
                "Ok" => match payload {
                    VariantPayloadValue::Unnamed(mut fields) if !fields.is_empty() => {
                        let inner = fields.remove(0);
                        Ok(Ok(T::from_value(inner)?))
                    }
                    VariantPayloadValue::Unnamed(fields) => Err(DecodeError::TooFewElements {
                        minimum: 1,
                        actual: fields.len(),
                    }),
                    _ => Err(DecodeError::PayloadMismatch {
                        expected: PayloadKind::Unnamed,
                        actual: payload.kind(),
                    }),
                },
                "Err" => match payload {
                    VariantPayloadValue::Unnamed(mut fields) if !fields.is_empty() => {
                        let inner = fields.remove(0);
                        Ok(Err(E::from_value(inner)?))
                    }
                    VariantPayloadValue::Unnamed(fields) => Err(DecodeError::TooFewElements {
                        minimum: 1,
                        actual: fields.len(),
                    }),
                    _ => Err(DecodeError::PayloadMismatch {
                        expected: PayloadKind::Unnamed,
                        actual: payload.kind(),
                    }),
                },
                _ => Err(DecodeError::UnknownVariant { tag }),
            },
            _ => Err(DecodeError::expected(ValueKind::Variant, &value)),
        }
    }
}

// ---------- HashMap<K, V> represented as a Map with key-value pairs ----------
impl<K: AsSchema + 'static, V: AsSchema + 'static> AsSchema for HashMap<K, V> {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let key_schema = K::register_dependencies_and_self(cache, rec_detector);
        let value_schema = V::register_dependencies_and_self(cache, rec_detector);
        rec_detector.pop();
        let schema = GlobalSchemaEnum::Map(Box::new(key_schema), Box::new(value_schema));
        GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
        schema
    }
}

impl<K: AsValue + Eq + Hash + 'static, V: AsValue + 'static> AsValue for HashMap<K, V> {
    fn as_value(&self) -> Value {
        let entries = self
            .iter()
            .map(|(k, v)| (k.as_value(), v.as_value()))
            .collect();
        Value::Map(entries)
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        match value {
            Value::Map(entries) => {
                let mut decoded = Self::with_capacity(entries.len());
                for (key, value) in entries {
                    if decoded
                        .insert(K::from_value(key)?, V::from_value(value)?)
                        .is_some()
                    {
                        return Err(DecodeError::DuplicateMapKey);
                    }
                }
                Ok(decoded)
            }
            _ => Err(DecodeError::expected(ValueKind::Map, &value)),
        }
    }
}

// ---------- BTreeMap<K, V> represented as a Map with key-value pairs ----------
impl<K: AsSchema + 'static, V: AsSchema + 'static> AsSchema for BTreeMap<K, V> {
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum {
        if rec_detector.contains(&TypeId::of::<Self>()) {
            return GlobalSchemaEnum::Ref(TypeId::of::<Self>());
        }
        rec_detector.push(TypeId::of::<Self>());
        let key_schema = K::register_dependencies_and_self(cache, rec_detector);
        let value_schema = V::register_dependencies_and_self(cache, rec_detector);
        rec_detector.pop();
        let schema = GlobalSchemaEnum::Map(Box::new(key_schema), Box::new(value_schema));
        GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
        schema
    }
}

impl<K: AsValue + Ord + 'static, V: AsValue + 'static> AsValue for BTreeMap<K, V> {
    fn as_value(&self) -> Value {
        let entries = self
            .iter()
            .map(|(k, v)| (k.as_value(), v.as_value()))
            .collect();
        Value::Map(entries)
    }
    fn from_value(value: Value) -> Result<Self, DecodeError> {
        match value {
            Value::Map(entries) => {
                let mut decoded = Self::new();
                for (key, value) in entries {
                    if decoded
                        .insert(K::from_value(key)?, V::from_value(value)?)
                        .is_some()
                    {
                        return Err(DecodeError::DuplicateMapKey);
                    }
                }
                Ok(decoded)
            }
            _ => Err(DecodeError::expected(ValueKind::Map, &value)),
        }
    }
}
