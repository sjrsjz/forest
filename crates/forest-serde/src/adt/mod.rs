//! Forest Schema & Value Core Library
//!
//! This library provides a lightweight yet powerful runtime type system in Rust, supporting:
//! - **Algebraic Data Types (ADTs)**: Includes primitives, `Option`, `Tuple`, `Array`, `Variant` (enums), and `Record` (structs).
//! - **Structural Subtyping**: Supports record width subtyping and tuple prefix projection.
//! - **Recursive Types**: Handled via `Ref` indices for cycles and self-referencing types.
//! - **Value Validation & Loose Matching**: Capability to check if a `Value` conforms to a `Schema`, with support for interoperability (e.g., matching a serialized `Array` against a `Tuple` schema).
//!
//! Generation of schemas and values for custom types is typically automated using `#[derive(DeriveSchema, DeriveValue)]`.
//!
//! # Example: Structural Subtyping
//! ```rust
//! use forest_serde::{DeriveSchema};
//! use forest_serde::adt::AsSchema;
//!
//! fn test_derive_subtype() {
//!     #[derive(DeriveSchema)]
//!     struct Base {
//!         id: i32,
//!     }
//!
//!     #[derive(DeriveSchema)]
//!     struct Derived {
//!         id: i32,
//!         name: String,
//!     }
//!
//!     let base_schema = Base::as_schema();
//!     let derived_schema = Derived::as_schema();
//!
//!     // Derived struct has all fields of Base (and more), so it's a subtype.
//!     assert!(derived_schema.subof(&base_schema));
//!     assert!(!base_schema.subof(&derived_schema));
//! }
//! ```

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    any::TypeId,
    collections::{BTreeMap, HashMap},
    fmt,
    hash::Hash,
};

mod error;
mod map_serde;
pub mod prelude;

pub use error::{DecodeError, PayloadKind, ValueKind};

// ============================================================================
// Construction Helpers
// ============================================================================

/// In-memory representation of schemas used during the construction phase.
///
/// This is used exclusively during `AsSchema` macro expansion and registration to collect
/// the full type dependency graph. Before the final `Schema` is published or serialized,
/// this representation is flattened into a list of `SchemaEnum` items, where recursion
/// is handled by numeric indices.
#[derive(Debug, Clone)]
pub enum GlobalSchemaEnum {
    I32,
    I64,
    Bool,
    String,
    Bytes(usize), // Fixed-size byte array (length in bytes)
    Option(Box<GlobalSchemaEnum>),
    Tuple(Vec<GlobalSchemaEnum>),
    Array(Box<GlobalSchemaEnum>),
    Map(Box<GlobalSchemaEnum>, Box<GlobalSchemaEnum>),
    Variant(BTreeMap<String, VariantPayloadGlobal>),
    Record(BTreeMap<String, GlobalSchemaEnum>),
    Ref(TypeId),
    ByteBuffer,
}

/// Global representation of variant payloads, distinguishing between unnamed (tuple-like)
/// and named (struct-like) variants.
#[derive(Debug, Clone)]
pub enum VariantPayloadGlobal {
    Unnamed(Vec<GlobalSchemaEnum>),
    Named(BTreeMap<String, GlobalSchemaEnum>),
}

impl GlobalSchemaEnum {
    /// Registers a type's structural representation into the global cache and returns its `TypeId`.
    pub fn alloc<T: 'static>(
        repr: GlobalSchemaEnum,
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
    ) -> TypeId {
        let type_id = TypeId::of::<T>();
        cache.insert(type_id, repr);
        type_id
    }

    /// Flattens the global type dependency graph into a serialization-friendly `Schema`.
    ///
    /// This resolves all `TypeId` references into numeric indices and produces a linear
    /// array of `SchemaEnum` variants.
    pub fn into_schema<T: 'static>(cache: &HashMap<TypeId, GlobalSchemaEnum>) -> Option<Schema> {
        cache.get(&TypeId::of::<T>()).map(|root_repr| {
            fn convert(
                repr: &GlobalSchemaEnum,
                schemas: &mut Vec<SchemaEnum>,
                id2idx: &mut HashMap<TypeId, usize>,
                cache: &HashMap<TypeId, GlobalSchemaEnum>,
            ) -> SchemaEnum {
                match repr {
                    GlobalSchemaEnum::I32 => SchemaEnum::I32,
                    GlobalSchemaEnum::I64 => SchemaEnum::I64,
                    GlobalSchemaEnum::Bool => SchemaEnum::Bool,
                    GlobalSchemaEnum::String => SchemaEnum::String,
                    GlobalSchemaEnum::Bytes(len) => SchemaEnum::Bytes(*len),
                    GlobalSchemaEnum::ByteBuffer => SchemaEnum::ByteBuffer,
                    GlobalSchemaEnum::Option(inner) => {
                        SchemaEnum::Option(Box::new(convert(inner, schemas, id2idx, cache)))
                    }
                    GlobalSchemaEnum::Tuple(elems) => SchemaEnum::Tuple(
                        elems
                            .iter()
                            .map(|e| convert(e, schemas, id2idx, cache))
                            .collect(),
                    ),
                    GlobalSchemaEnum::Array(inner) => {
                        SchemaEnum::Array(Box::new(convert(inner, schemas, id2idx, cache)))
                    }
                    GlobalSchemaEnum::Map(key, value) => SchemaEnum::Map(
                        Box::new(convert(key, schemas, id2idx, cache)),
                        Box::new(convert(value, schemas, id2idx, cache)),
                    ),
                    GlobalSchemaEnum::Record(fields) => {
                        let mut map = BTreeMap::new();
                        for (k, v) in fields {
                            map.insert(k.clone(), convert(v, schemas, id2idx, cache));
                        }
                        SchemaEnum::Record(map)
                    }
                    GlobalSchemaEnum::Variant(variants) => {
                        let mut map = BTreeMap::new();
                        for (tag, payload) in variants {
                            map.insert(
                                tag.clone(),
                                convert_payload(payload, schemas, id2idx, cache),
                            );
                        }
                        SchemaEnum::Variant(map)
                    }
                    GlobalSchemaEnum::Ref(type_id) => {
                        if let Some(&idx) = id2idx.get(type_id) {
                            SchemaEnum::Ref(idx)
                        } else {
                            // Pre-allocate index to support cyclic references.
                            let idx = schemas.len();
                            id2idx.insert(*type_id, idx);
                            schemas.push(SchemaEnum::I32); // Placeholder
                            let inner_repr = cache
                                .get(type_id)
                                .expect("Referenced type not found in cache");
                            let converted = convert(inner_repr, schemas, id2idx, cache);
                            schemas[idx] = converted;
                            SchemaEnum::Ref(idx)
                        }
                    }
                }
            }

            fn convert_payload(
                payload: &VariantPayloadGlobal,
                schemas: &mut Vec<SchemaEnum>,
                id2idx: &mut HashMap<TypeId, usize>,
                cache: &HashMap<TypeId, GlobalSchemaEnum>,
            ) -> VariantPayloadSchema {
                match payload {
                    VariantPayloadGlobal::Unnamed(items) => VariantPayloadSchema::Unnamed(
                        items
                            .iter()
                            .map(|i| convert(i, schemas, id2idx, cache))
                            .collect(),
                    ),
                    VariantPayloadGlobal::Named(fields) => {
                        let mut map = BTreeMap::new();
                        for (k, v) in fields {
                            map.insert(k.clone(), convert(v, schemas, id2idx, cache));
                        }
                        VariantPayloadSchema::Named(map)
                    }
                }
            }

            let mut schemas = vec![SchemaEnum::I32]; // Root node placeholder.
            let mut id2idx = HashMap::new();
            id2idx.insert(TypeId::of::<T>(), 0);
            let root_schema = convert(root_repr, &mut schemas, &mut id2idx, cache);
            schemas[0] = root_schema;
            Schema { schemas }
        })
    }
}

// ============================================================================
// Serialization-Safe Schema Definitions
// ============================================================================

/// A serialization-friendly runtime representation of a type schema.
///
/// Recursive references are represented via the `Ref(usize)` variant, which indexes
/// into the master list of schemas contained within a [`Schema`] object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaEnum {
    I32,
    I64,
    Bool,
    String,
    Bytes(usize), // Fixed-size byte array (length in bytes)
    Option(Box<SchemaEnum>),
    Tuple(Vec<SchemaEnum>),
    Array(Box<SchemaEnum>),
    Map(Box<SchemaEnum>, Box<SchemaEnum>),
    Variant(BTreeMap<String, VariantPayloadSchema>),
    Record(BTreeMap<String, SchemaEnum>),
    Ref(usize),
    ByteBuffer,
}

/// Defines the schema for a variant's payload (the data associated with an enum arm).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VariantPayloadSchema {
    Unnamed(Vec<SchemaEnum>),
    Named(BTreeMap<String, SchemaEnum>),
}

impl SchemaEnum {
    /// Validates the schema's integrity, ensuring all `Ref` indices are within the valid range
    /// of the parent `Schema` container.
    #[cfg_attr(not(target_family = "wasm"), stacksafe::stacksafe)]
    fn is_well_defined(&self, schema: &Schema) -> bool {
        match self {
            SchemaEnum::Array(inner) => inner.is_well_defined(schema),
            SchemaEnum::Record(fields) => fields.values().all(|f| f.is_well_defined(schema)),
            SchemaEnum::Variant(variants) => variants.values().all(|p| p.is_well_defined(schema)),
            SchemaEnum::Option(inner) => inner.is_well_defined(schema),
            SchemaEnum::Tuple(elems) => elems.iter().all(|e| e.is_well_defined(schema)),
            SchemaEnum::Map(key, value) => {
                key.is_well_defined(schema) && value.is_well_defined(schema)
            }
            SchemaEnum::I32
            | SchemaEnum::I64
            | SchemaEnum::Bool
            | SchemaEnum::String
            | SchemaEnum::Bytes(_)
            | SchemaEnum::ByteBuffer => true,
            SchemaEnum::Ref(idx) => *idx < schema.schemas.len(),
        }
    }

    /// Performs a structural subtyping check: returns true if `self` is a subtype of `other` (`self <: other`).
    ///
    /// The algorithm handles width subtyping for records and tuples, as well as depth subtyping
    /// for container types. Non-well-founded recursive structures (pure Ref cycles) are treated as
    /// the Top type (`Any`), meaning `Any </: T` (false) and `T <: Any` (true).
    #[cfg_attr(not(target_family = "wasm"), stacksafe::stacksafe)]
    fn subof<'a>(
        &'a self,
        other: &'a SchemaEnum,
        context: (&'a Schema, &'a Schema),
        assumptions: &mut SmallVec<[(&'a SchemaEnum, &'a SchemaEnum, usize); 8]>,
        depth: usize,
    ) -> bool {
        fn check_assumed<'a>(
            a: &'a SchemaEnum,
            b: &'a SchemaEnum,
            assumptions: &[(&'a SchemaEnum, &'a SchemaEnum, usize)],
        ) -> Option<usize> {
            assumptions
                .iter()
                .find(|(x, y, _)| std::ptr::eq(*x, a) && std::ptr::eq(*y, b))
                .map(|(_, _, push_depth)| *push_depth)
        }

        match (self, other) {
            (SchemaEnum::Ref(idx_a), SchemaEnum::Ref(idx_b)) => {
                if let Some(_push_depth) = check_assumed(self, other, assumptions) {
                    // Any <: Any
                    return true;
                }
                let a = &context.0.schemas[*idx_a];
                let b = &context.1.schemas[*idx_b];
                assumptions.push((self, other, depth));
                let res = a.subof(b, context, assumptions, depth);
                assumptions.pop();
                res
            }
            (SchemaEnum::Ref(idx_a), _) => {
                if let Some(push_depth) = check_assumed(self, other, assumptions) {
                    // If depth == push_depth, it indicates a pure Ref cycle (Top/Any).
                    // Satisfies the requirement: Any is not a subtype of T (returns false).
                    // If depth > push_depth, it means a structure has been traversed, representing guarded recursion (returns true).
                    return depth > push_depth;
                }
                let a = &context.0.schemas[*idx_a];
                assumptions.push((self, other, depth));
                let res = a.subof(other, context, assumptions, depth);
                assumptions.pop();
                res
            }
            (_, SchemaEnum::Ref(idx_b)) => {
                if let Some(_push_depth) = check_assumed(self, other, assumptions) {
                    // Right side is a pure cycle (Any). T <: Any is always true.
                    return true;
                }
                let b = &context.1.schemas[*idx_b];
                assumptions.push((self, other, depth));
                let res = self.subof(b, context, assumptions, depth);
                assumptions.pop();
                res
            }

            (a, b) => {
                let next_depth = depth + 1;

                match (a, b) {
                    (SchemaEnum::I32, SchemaEnum::I32) => true,
                    (SchemaEnum::I64, SchemaEnum::I64) => true,
                    (SchemaEnum::Bool, SchemaEnum::Bool) => true,
                    (SchemaEnum::String, SchemaEnum::String) => true,
                    (SchemaEnum::Bytes(ba), SchemaEnum::Bytes(bb)) => ba == bb,
                    (SchemaEnum::Bytes(_) | SchemaEnum::ByteBuffer, SchemaEnum::ByteBuffer) => true,

                    (SchemaEnum::I32, SchemaEnum::I64) => true,

                    (SchemaEnum::Option(inner_a), SchemaEnum::Option(inner_b)) => {
                        inner_a.subof(inner_b, context, assumptions, next_depth)
                    }

                    (SchemaEnum::Tuple(ta), SchemaEnum::Tuple(tb)) => {
                        ta.len() >= tb.len()
                            && ta
                                .iter()
                                .zip(tb.iter())
                                .all(|(va, vb)| va.subof(vb, context, assumptions, next_depth))
                    }

                    (SchemaEnum::Array(aa), SchemaEnum::Array(ab)) => {
                        aa.subof(ab, context, assumptions, next_depth)
                    }

                    (SchemaEnum::Record(fa), SchemaEnum::Record(fb)) => fb.iter().all(|(k, vb)| {
                        fa.get(k)
                            .is_some_and(|va| va.subof(vb, context, assumptions, next_depth))
                    }),

                    (SchemaEnum::Tuple(_), SchemaEnum::Record(rb)) if rb.is_empty() => true,
                    (SchemaEnum::Record(_), SchemaEnum::Tuple(tb)) if tb.is_empty() => true,

                    (SchemaEnum::Map(ka, va), SchemaEnum::Map(kb, vb)) => {
                        ka.subof(kb, context, assumptions, next_depth)
                            && kb.subof(ka, context, assumptions, next_depth)
                            && va.subof(vb, context, assumptions, next_depth)
                    }

                    (SchemaEnum::Variant(va), SchemaEnum::Variant(vb)) => {
                        va.iter().all(|(tag, payload_a)| {
                            vb.get(tag).is_some_and(|payload_b| {
                                payload_a.subof(payload_b, context, assumptions, next_depth)
                            })
                        })
                    }

                    _ => false,
                }
            }
        }
    }
}

impl VariantPayloadSchema {
    #[cfg_attr(not(target_family = "wasm"), stacksafe::stacksafe)]
    fn is_well_defined(&self, schema: &Schema) -> bool {
        match self {
            VariantPayloadSchema::Unnamed(items) => items.iter().all(|i| i.is_well_defined(schema)),
            VariantPayloadSchema::Named(fields) => {
                fields.values().all(|f| f.is_well_defined(schema))
            }
        }
    }

    #[cfg_attr(not(target_family = "wasm"), stacksafe::stacksafe)]
    fn subof<'a>(
        &'a self,
        other: &'a VariantPayloadSchema,
        context: (&'a Schema, &'a Schema),
        assumptions: &mut SmallVec<[(&'a SchemaEnum, &'a SchemaEnum, usize); 8]>,
        depth: usize,
    ) -> bool {
        match (self, other) {
            // Unnamed payloads are treated similarly to Tuples, supporting width subtyping.
            (VariantPayloadSchema::Unnamed(a), VariantPayloadSchema::Unnamed(b)) => {
                a.len() >= b.len()
                    && a.iter()
                        .take(b.len())
                        .zip(b)
                        .all(|(a, b)| a.subof(b, context, assumptions, depth + 1))
            }
            (VariantPayloadSchema::Named(fields_a), VariantPayloadSchema::Named(fields_b)) => {
                fields_b.iter().all(|(k, v_b)| {
                    fields_a
                        .get(k)
                        .is_some_and(|v_a| v_a.subof(v_b, context, assumptions, depth + 1))
                })
            }
            (VariantPayloadSchema::Named(_), VariantPayloadSchema::Unnamed(b)) if b.is_empty() => {
                true
            }
            (VariantPayloadSchema::Unnamed(_), VariantPayloadSchema::Named(b)) if b.is_empty() => {
                true
            }
            _ => false,
        }
    }
}

/// The top-level Schema object.
///
/// It encapsulates a list of [`SchemaEnum`] definitions. The first element (index 0)
/// is traditionally the "root" type of the schema.
#[derive(Debug, Clone, Serialize)]
#[serde(transparent)]
pub struct Schema {
    schemas: Vec<SchemaEnum>,
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let schemas = Vec::<SchemaEnum>::deserialize(deserializer)?;
        let schema = Schema { schemas };
        if !schema.is_well_defined() {
            return Err(serde::de::Error::custom(
                "Schema contains invalid references",
            ));
        }
        Ok(schema)
    }
}

fn format_ident(s: &str) -> String {
    let mut chars = s.chars();
    if let Some(first) = chars.next() {
        let is_first_valid = first.is_alphabetic() || first == '_';
        let is_rest_valid = chars.all(|c| c.is_alphanumeric() || c == '_');
        let reserved = matches!(
            s,
            "schema"
                | "export"
                | "i32"
                | "i64"
                | "bool"
                | "string"
                | "bytes"
                | "optional"
                | "map"
                | "variant"
                | "true"
                | "false"
                | "null"
                | "some"
        );
        if is_first_valid && is_rest_valid && !reserved {
            return s.to_string();
        }
    }
    quote_string(s)
}

fn quote_string(s: &str) -> String {
    serde_json::to_string(s).expect("serializing a string is infallible")
}

impl fmt::Display for SchemaEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaEnum::I32 => write!(f, "i32"),
            SchemaEnum::I64 => write!(f, "i64"),
            SchemaEnum::Bool => write!(f, "bool"),
            SchemaEnum::String => write!(f, "string"),
            SchemaEnum::Bytes(size) => write!(f, "bytes {}", size),
            SchemaEnum::ByteBuffer => write!(f, "bytes"),
            SchemaEnum::Option(inner) => write!(f, "optional {}", inner),
            SchemaEnum::Array(inner) => write!(f, "[{}]", inner),
            SchemaEnum::Map(k, v) => write!(f, "map {} => {}", k, v),

            SchemaEnum::Tuple(items) => {
                write!(f, "(")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, ")")
            }

            SchemaEnum::Record(fields) => {
                let mut sorted_fields: Vec<_> = fields.iter().collect();
                sorted_fields.sort_by_key(|(k, _)| *k);

                write!(f, "{{")?;
                for (i, (k, v)) in sorted_fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", format_ident(k), v)?;
                }
                write!(f, "}}")
            }

            SchemaEnum::Variant(arms) => {
                let mut sorted_arms: Vec<_> = arms.iter().collect();
                sorted_arms.sort_by_key(|(k, _)| *k);

                write!(f, "variant {{")?;
                for (i, (name, payload)) in sorted_arms.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", format_ident(name))?;

                    match payload {
                        VariantPayloadSchema::Unnamed(items) if items.is_empty() => {}
                        VariantPayloadSchema::Unnamed(items) => {
                            write!(f, "(")?;
                            for (j, item) in items.iter().enumerate() {
                                if j > 0 {
                                    write!(f, ", ")?;
                                }
                                write!(f, "{}", item)?;
                            }
                            write!(f, ")")?;
                        }
                        VariantPayloadSchema::Named(fields) => {
                            let mut sorted_fields: Vec<_> = fields.iter().collect();
                            sorted_fields.sort_by_key(|(k, _)| *k);
                            write!(f, " {{")?;
                            for (j, (k, v)) in sorted_fields.iter().enumerate() {
                                if j > 0 {
                                    write!(f, ", ")?;
                                }
                                write!(f, "{}: {}", format_ident(k), v)?;
                            }
                            write!(f, "}}")?;
                        }
                    }
                }
                write!(f, "}}")
            }

            SchemaEnum::Ref(idx) => write!(f, "T{}", idx),
        }
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, schema_enum) in self.schemas.iter().enumerate() {
            writeln!(f, "schema T{} = {};", i, schema_enum)?;
        }
        if !self.schemas.is_empty() {
            writeln!(f, "export T0;")?;
        }
        Ok(())
    }
}

impl Schema {
    /// Verifies that all internal references within the schema are valid and point to existing indices.
    pub fn is_well_defined(&self) -> bool {
        !self.schemas.is_empty() && self.schemas.iter().all(|s| s.is_well_defined(self))
    }

    /// Returns the root node of the schema (the first element).
    pub fn root(&self) -> &SchemaEnum {
        &self.schemas[0]
    }

    /// Checks if this schema is a structural subtype of another.
    pub fn subof(&self, other: &Schema) -> bool {
        self.root()
            .subof(other.root(), (self, other), &mut SmallVec::new(), 0)
    }

    /// Returns the raw list of schema definitions.
    pub fn schemas(&self) -> &Vec<SchemaEnum> {
        &self.schemas
    }

    /// Constructs a `Schema` from a list of definitions, performing integrity checks on references.
    pub fn from_schemas(schemas: Vec<SchemaEnum>) -> Option<Self> {
        let schema = Schema { schemas };
        if schema.is_well_defined() {
            Some(schema)
        } else {
            None
        }
    }
}

/// Logical equality for schemas is defined by mutual subtyping: two schemas are considered equal
/// It is NOT structural equality, the algorithm may ignore unused references in the schema definitions.
impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.subof(other) && other.subof(self)
    }
}

impl Eq for Schema {}

impl PartialOrd for Schema {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.subof(other) {
            if other.subof(self) {
                Some(std::cmp::Ordering::Equal)
            } else {
                Some(std::cmp::Ordering::Less)
            }
        } else if other.subof(self) {
            Some(std::cmp::Ordering::Greater)
        } else {
            None
        }
    }
}

// ============================================================================
// Runtime Value Definitions
// ============================================================================

/// A dynamic value tree representing data instance at runtime.
///
/// Unlike schemas, the value tree is self-contained and does not use internal indices
/// for its structure, making it easy to traverse and manipulate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Value {
    /// Represents a 32-bit signed integer value.
    I32(i32),
    /// Represents a 64-bit signed integer value.
    I64(i64),
    /// Represents a boolean value (`true` or `false`).
    Bool(bool),
    /// Represents a UTF-8 encoded string value.
    String(String),
    /// Represents a byte array of a certain size.
    Bytes(Vec<u8>),
    /// Represents a Option::Some variant, containing a boxed inner value.
    Some(Box<Value>),
    /// Represents a Option::None variant.
    None,
    /// Represents a tuple value, containing an ordered list of inner values.
    Tuple(Vec<Value>),
    /// Represents an array value, containing a list of inner values (homogeneous).
    Array(Vec<Value>),
    /// Represents a map value, containing a mapping of keys to values.
    Map(#[serde(with = "map_serde")] BTreeMap<Value, Value>),
    /// Represents a variant (enum) value, containing a tag and an associated payload.
    Variant(String, VariantPayloadValue),
    /// Represents a record (struct) value, containing a mapping of field names to values.
    Record(BTreeMap<String, Value>),
}

/// Represents the data payload associated with a specific variant in an enum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum VariantPayloadValue {
    /// Represents an unnamed (tuple-like) variant payload, containing an ordered list of values.
    Unnamed(Vec<Value>),
    /// Represents a named (struct-like) variant payload, containing a mapping of field names to values.
    Named(BTreeMap<String, Value>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::None => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            Value::I32(num) => write!(f, "{}", num),
            Value::I64(num) => write!(f, "{}i64", num),
            Value::String(s) => write!(f, "{}", quote_string(s)),
            Value::Bytes(bytes) => {
                use base64::{Engine as _, engine::general_purpose::STANDARD};
                write!(f, "b\"{}\"", STANDARD.encode(bytes))
            }
            Value::Some(val) => write!(f, "some {}", val),

            Value::Array(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }

            Value::Tuple(items) => {
                write!(f, "(")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, ")")
            }

            Value::Map(map) => {
                write!(f, "map {{")?;
                for (i, (k, v)) in map.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")
            }

            Value::Record(rec) => {
                write!(f, "{{")?;
                for (i, (k, v)) in rec.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", quote_string(k), v)?;
                }
                write!(f, "}}")
            }

            Value::Variant(tag, payload) => {
                let safe_tag = format_ident(tag);

                match payload {
                    VariantPayloadValue::Unnamed(items) => {
                        // Always retain the outer tuple: one tuple/record field is not its payload.
                        write!(f, "{}::(", safe_tag)?;
                        for (i, item) in items.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{}", item)?;
                        }
                        write!(f, ")")
                    }
                    VariantPayloadValue::Named(fields) => {
                        // 4. Named payload (struct) variant: defaults to Record format, e.g., Object::{ "a": 1 }.
                        write!(f, "{}::{{", safe_tag)?;
                        for (i, (k, v)) in fields.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{}: {}", quote_string(k), v)?;
                        }
                        write!(f, "}}")
                    }
                }
            }
        }
    }
}

impl Value {
    /// Verifies if the current value conforms to the provided schema.
    ///
    /// This check supports structural subtyping rules (e.g., width subtyping for records/tuples)
    /// and optional loose matching for cross-type interoperability.
    #[cfg_attr(not(target_family = "wasm"), stacksafe::stacksafe)]
    fn _check<'a>(
        &'a self,
        schema: &'a SchemaEnum,
        schema_ctx: &'a Schema,
        assumptions: &mut SmallVec<[(&'a Value, &'a SchemaEnum); 8]>,
        loose_match: bool,
    ) -> bool {
        fn is_assumed<'a>(
            a: &'a Value,
            b: &'a SchemaEnum,
            assumptions: &[(&'a Value, &'a SchemaEnum)],
        ) -> bool {
            assumptions
                .iter()
                .any(|(x, y)| std::ptr::eq(*x, a) && std::ptr::eq(*y, b))
        }

        match (self, schema) {
            (Value::I32(_), SchemaEnum::I32) => true,
            (Value::I64(_), SchemaEnum::I64) => true,

            // Numeric Promotion (Value Level)
            (Value::I32(_), SchemaEnum::I64) => true,

            (Value::Bool(_), SchemaEnum::Bool) => true,
            (Value::String(_), SchemaEnum::String) => true,
            (Value::Bytes(a), SchemaEnum::Bytes(b)) => a.len() == *b,
            (Value::Bytes(_), SchemaEnum::ByteBuffer) => true,

            (Value::Some(inner), SchemaEnum::Option(sch)) => {
                inner._check(sch, schema_ctx, assumptions, loose_match)
            }
            (Value::None, SchemaEnum::Option(_)) => true,

            // Tuple Validation: Supports width subtyping (extra trailing elements are ignored).
            (Value::Tuple(vals), SchemaEnum::Tuple(schs)) => {
                vals.len() >= schs.len()
                    && vals
                        .iter()
                        .take(schs.len())
                        .zip(schs)
                        .all(|(v, s)| v._check(s, schema_ctx, assumptions, loose_match))
            }

            // Loose Mode: Allows an `Array` value to satisfy a `Tuple` schema.
            (Value::Array(vals), SchemaEnum::Tuple(schs)) if loose_match => {
                vals.len() >= schs.len()
                    && vals
                        .iter()
                        .take(schs.len())
                        .zip(schs)
                        .all(|(v, s)| v._check(s, schema_ctx, assumptions, loose_match))
            }

            // Unit/Empty Interoperability
            (Value::Array(_), SchemaEnum::Record(schs)) if loose_match && schs.is_empty() => true,
            (Value::Tuple(_), SchemaEnum::Record(sch)) if sch.is_empty() => true,
            (Value::Record(_), SchemaEnum::Tuple(schs)) if schs.is_empty() => true,

            (Value::Array(vals), SchemaEnum::Array(sch)) => vals
                .iter()
                .all(|v| v._check(sch, schema_ctx, assumptions, loose_match)),

            (Value::Variant(tag, payload), SchemaEnum::Variant(variants)) => {
                variants.get(tag).is_some_and(|payload_sch| {
                    payload.check(payload_sch, schema_ctx, assumptions, loose_match)
                })
            }

            (Value::Map(entries), SchemaEnum::Map(key_sch, value_sch)) => {
                entries.iter().all(|(k, v)| {
                    k._check(key_sch, schema_ctx, assumptions, loose_match)
                        && v._check(value_sch, schema_ctx, assumptions, loose_match)
                })
            }

            // Record Validation: Checks for required fields; additional fields in the value are permitted.
            (Value::Record(fields), SchemaEnum::Record(sch_fields)) => {
                sch_fields.iter().all(|(k, v_sch)| {
                    fields
                        .get(k)
                        .is_some_and(|v| v._check(v_sch, schema_ctx, assumptions, loose_match))
                })
            }

            // De-reference and check against the resolved type.
            (this, SchemaEnum::Ref(idx)) => {
                if is_assumed(self, schema, assumptions) {
                    return true;
                }
                let inner_sch = &schema_ctx.schemas[*idx];
                assumptions.push((self, schema));
                let res = this._check(inner_sch, schema_ctx, assumptions, loose_match);
                assumptions.pop();
                res
            }
            _ => false,
        }
    }

    pub fn check(&self, schema: &Schema, loose_match: bool) -> bool {
        self._check(schema.root(), schema, &mut SmallVec::new(), loose_match)
    }
}

impl VariantPayloadValue {
    fn check<'a>(
        &'a self,
        schema: &'a VariantPayloadSchema,
        schema_ctx: &'a Schema,
        assumptions: &mut SmallVec<[(&'a Value, &'a SchemaEnum); 8]>,
        loose_match: bool,
    ) -> bool {
        match (self, schema) {
            // Check for width subtyping in unnamed variant payloads.
            (VariantPayloadValue::Unnamed(vals), VariantPayloadSchema::Unnamed(schs)) => {
                vals.len() >= schs.len()
                    && vals
                        .iter()
                        .take(schs.len())
                        .zip(schs)
                        .all(|(v, s)| v._check(s, schema_ctx, assumptions, loose_match))
            }
            (VariantPayloadValue::Named(fields), VariantPayloadSchema::Named(sch_fields)) => {
                sch_fields.iter().all(|(k, v_sch)| {
                    fields
                        .get(k)
                        .is_some_and(|v| v._check(v_sch, schema_ctx, assumptions, loose_match))
                })
            }
            (VariantPayloadValue::Named(_), VariantPayloadSchema::Unnamed(b)) if b.is_empty() => {
                true
            }
            (VariantPayloadValue::Unnamed(_), VariantPayloadSchema::Named(b)) if b.is_empty() => {
                true
            }
            _ => false,
        }
    }
}

// ============================================================================
// User-Facing Traits
// ============================================================================

/// Trait implemented by types that can provide a structural Forest [`Schema`].
pub trait AsSchema: Sized + 'static {
    /// Generates and returns the complete [`Schema`] for this type, including all dependencies.
    fn as_schema() -> Schema {
        let mut cache = HashMap::new();
        let mut rec_detector = Vec::new();
        Self::register_dependencies_and_self(&mut cache, &mut rec_detector);
        GlobalSchemaEnum::into_schema::<Self>(&cache).expect("Schema conversion failed")
    }

    #[doc(hidden)]
    fn register_dependencies_and_self(
        cache: &mut HashMap<TypeId, GlobalSchemaEnum>,
        rec_detector: &mut Vec<TypeId>,
    ) -> GlobalSchemaEnum;
}

/// Trait for types that can be serialized into and deserialized from a Forest [`Value`].
pub trait AsValue: Sized + 'static {
    /// Serializes the instance into a dynamic [`Value`] tree.
    fn as_value(&self) -> Value;

    /// Deserializes a dynamic [`Value`] tree back into an instance of the type.
    fn from_value(value: Value) -> Result<Self, DecodeError>;
}

/// Already-encoded values pass through language bindings without a Rust DTO.
impl AsValue for Value {
    fn as_value(&self) -> Value {
        self.clone()
    }

    fn from_value(value: Value) -> Result<Self, DecodeError> {
        Ok(value)
    }
}
