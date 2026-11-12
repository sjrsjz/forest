//! Self-contained symbol documents. Encoding borrows the tree; decoding resolves
//! names before constructing logical values, so local IDs never affect Eq/Ord/Hash.
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt,
    marker::PhantomData,
};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    ser::{SerializeMap, SerializeSeq},
};

use super::{PayloadKind, SchemaEnum, Value, ValueKind, VariantPayloadSchema, VariantPayloadValue};
use crate::symbol::{FIRST_DYNAMIC_SYMBOL, RESERVED_SYMBOLS, Symbol, SymbolId};

struct Symbols<'a> {
    names: Vec<&'a str>,
}

impl<'a> Symbols<'a> {
    fn new(names: BTreeSet<&'a str>) -> Self {
        Self {
            names: names
                .into_iter()
                .filter(|name| {
                    !RESERVED_SYMBOLS
                        .iter()
                        .any(|(_, reserved)| name == reserved)
                })
                .collect(),
        }
    }

    fn id<E: serde::ser::Error>(&self, name: &str) -> Result<SymbolId, E> {
        if let Some((id, _)) = RESERVED_SYMBOLS
            .iter()
            .find(|(_, reserved)| *reserved == name)
        {
            return Ok(*id);
        }
        let index = self
            .names
            .binary_search(&name)
            .map_err(|_| E::custom("uncollected structural name"))?;
        u32::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(FIRST_DYNAMIC_SYMBOL))
            .map(SymbolId)
            .ok_or_else(|| E::custom("symbol table exceeds SymbolId range"))
    }
}

fn validate_symbols<E: de::Error>(names: &[Symbol]) -> Result<(), E> {
    if names.len() > (u32::MAX - FIRST_DYNAMIC_SYMBOL) as usize + 1 {
        return Err(E::custom("symbol table exceeds SymbolId range"));
    }
    let mut seen: HashSet<&str> = RESERVED_SYMBOLS.iter().map(|(_, name)| *name).collect();
    for name in names {
        if !seen.insert(name.as_str()) {
            return Err(E::custom("duplicate or reserved symbol in document table"));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct NameSeed<'a>(&'a [Symbol]);

impl<'de> DeserializeSeed<'de> for NameSeed<'_> {
    type Value = Symbol;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Symbol, D::Error> {
        let SymbolId(id) = SymbolId::deserialize(d)?;
        if let Some((_, name)) = RESERVED_SYMBOLS
            .iter()
            .find(|(reserved, _)| reserved.0 == id)
        {
            return Ok(Symbol::from_static(name));
        }
        self.0
            .get(
                id.checked_sub(FIRST_DYNAMIC_SYMBOL)
                    .ok_or_else(|| de::Error::custom("unknown reserved symbol"))?
                    as usize,
            )
            .cloned()
            .ok_or_else(|| de::Error::custom("symbol index outside document table"))
    }
}

trait Node: Sized {
    type Tag: Serialize + DeserializeOwned;
    fn collect<'a>(&'a self, symbols: &mut BTreeSet<&'a str>);
    fn write<S: Serializer>(&self, symbols: &Symbols<'_>, s: S) -> Result<S::Ok, S::Error>;
    fn read<'de, D: Deserializer<'de>>(
        tag: Self::Tag,
        symbols: &[Symbol],
        d: D,
    ) -> Result<Self, D::Error>;
}

struct NodeRef<'a, T>(&'a T, &'a Symbols<'a>);
impl<T: Node> Serialize for NodeRef<'_, T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.write(self.1, s)
    }
}

struct NodeSeed<'a, T>(&'a [Symbol], PhantomData<fn() -> T>);
impl<T> Copy for NodeSeed<'_, T> {}
impl<T> Clone for NodeSeed<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a, T> NodeSeed<'a, T> {
    fn new(names: &'a [Symbol]) -> Self {
        Self(names, PhantomData)
    }
}

fn required<'de, A: SeqAccess<'de>, S: DeserializeSeed<'de>>(
    seq: &mut A,
    seed: S,
) -> Result<S::Value, A::Error> {
    seq.next_element_seed(seed)?
        .ok_or_else(|| de::Error::custom("incomplete binary structure"))
}

impl<'de, T: Node> DeserializeSeed<'de> for NodeSeed<'_, T> {
    type Value = T;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<T, D::Error> {
        d.deserialize_tuple(2, self)
    }
}
impl<'de, T: Node> Visitor<'de> for NodeSeed<'_, T> {
    type Value = T;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a tagged Forest node")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<T, A::Error> {
        let tag = seq
            .next_element()?
            .ok_or_else(|| de::Error::custom("missing node tag"))?;
        required(&mut seq, BodySeed::<T> { tag, names: self.0 })
    }
}
struct BodySeed<'a, T: Node> {
    tag: T::Tag,
    names: &'a [Symbol],
}
impl<'de, T: Node> DeserializeSeed<'de> for BodySeed<'_, T> {
    type Value = T;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<T, D::Error> {
        T::read(self.tag, self.names, d)
    }
}

struct ListRef<'a, T>(&'a [T], &'a Symbols<'a>);
impl<T: Node> Serialize for ListRef<'_, T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for item in self.0 {
            seq.serialize_element(&NodeRef(item, self.1))?;
        }
        seq.end()
    }
}

struct FieldsRef<'a, T>(&'a BTreeMap<Symbol, T>, &'a Symbols<'a>);
impl<T: Node> Serialize for FieldsRef<'_, T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for (name, item) in self.0 {
            map.serialize_entry(&self.1.id::<S::Error>(name)?, &NodeRef(item, self.1))?;
        }
        map.end()
    }
}

#[derive(Clone, Copy)]
struct ListSeed<S>(S);
impl<'de, S: DeserializeSeed<'de> + Copy> DeserializeSeed<'de> for ListSeed<S> {
    type Value = Vec<S::Value>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de, S: DeserializeSeed<'de> + Copy> Visitor<'de> for ListSeed<S> {
    type Value = Vec<S::Value>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a Forest sequence")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(self.0)? {
            values.push(value);
        }
        Ok(values)
    }
}

struct MapSeed<K, V>(K, V);
impl<'de, K, V> DeserializeSeed<'de> for MapSeed<K, V>
where
    K: DeserializeSeed<'de> + Copy,
    K::Value: Ord,
    V: DeserializeSeed<'de> + Copy,
{
    type Value = BTreeMap<K::Value, V::Value>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de, K, V> Visitor<'de> for MapSeed<K, V>
where
    K: DeserializeSeed<'de> + Copy,
    K::Value: Ord,
    V: DeserializeSeed<'de> + Copy,
{
    type Value = BTreeMap<K::Value, V::Value>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a Forest map")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = BTreeMap::new();
        while let Some(key) = map.next_key_seed(self.0)? {
            let value = map.next_value_seed(self.1)?;
            if values.insert(key, value).is_some() {
                return Err(de::Error::custom("duplicate encoded map key"));
            }
        }
        Ok(values)
    }
}

struct PairSeed<A, B>(A, B);
impl<'de, A: DeserializeSeed<'de>, B: DeserializeSeed<'de>> DeserializeSeed<'de>
    for PairSeed<A, B>
{
    type Value = (A::Value, B::Value);
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_tuple(2, self)
    }
}
impl<'de, A: DeserializeSeed<'de>, B: DeserializeSeed<'de>> Visitor<'de> for PairSeed<A, B> {
    type Value = (A::Value, B::Value);
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a Forest pair")
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
        Ok((required(&mut seq, self.0)?, required(&mut seq, self.1)?))
    }
}

fn collect_list<'a, T: Node>(items: &'a [T], symbols: &mut BTreeSet<&'a str>) {
    for item in items {
        item.collect(symbols);
    }
}
fn collect_fields<'a, T: Node>(fields: &'a BTreeMap<Symbol, T>, symbols: &mut BTreeSet<&'a str>) {
    for (name, item) in fields {
        symbols.insert(name);
        item.collect(symbols);
    }
}

struct ValueMapRef<'a>(&'a BTreeMap<Value, Value>, &'a Symbols<'a>);
impl Serialize for ValueMapRef<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(&NodeRef(key, self.1), &NodeRef(value, self.1))?;
        }
        map.end()
    }
}

struct Bytes<'a>(&'a [u8]);
impl Serialize for Bytes<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(self.0)
    }
}
struct BytesSeed;
impl<'de> DeserializeSeed<'de> for BytesSeed {
    type Value = Vec<u8>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_byte_buf(self)
    }
}
impl<'de> Visitor<'de> for BytesSeed {
    type Value = Vec<u8>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a byte buffer")
    }
    fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
        Ok(bytes)
    }
    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
        Ok(bytes.to_vec())
    }
}

impl Node for Value {
    type Tag = ValueKind;
    #[inline]
    fn collect<'a>(&'a self, symbols: &mut BTreeSet<&'a str>) {
        match self {
            Self::Tuple(items) | Self::Array(items) => collect_list(items, symbols),
            Self::Map(items) => {
                for (key, value) in items {
                    key.collect(symbols);
                    value.collect(symbols);
                }
            }
            Self::Variant(tag, payload) => {
                symbols.insert(tag);
                payload.collect(symbols);
            }
            Self::Record(fields) => collect_fields(fields, symbols),
            Self::I32(_) | Self::I64(_) | Self::Bool(_) | Self::String(_) | Self::Bytes(_) => {}
        }
    }
    fn write<S: Serializer>(&self, symbols: &Symbols<'_>, s: S) -> Result<S::Ok, S::Error> {
        let kind = self.kind();
        match self {
            Self::I32(v) => (kind, v).serialize(s),
            Self::I64(v) => (kind, v).serialize(s),
            Self::Bool(v) => (kind, v).serialize(s),
            Self::String(v) => (kind, v).serialize(s),
            Self::Bytes(v) => (kind, Bytes(v)).serialize(s),
            Self::Tuple(items) | Self::Array(items) => (kind, ListRef(items, symbols)).serialize(s),
            Self::Map(items) => (kind, ValueMapRef(items, symbols)).serialize(s),
            Self::Variant(tag, payload) => (
                kind,
                (symbols.id::<S::Error>(tag)?, NodeRef(payload, symbols)),
            )
                .serialize(s),
            Self::Record(fields) => (kind, FieldsRef(fields, symbols)).serialize(s),
        }
    }
    fn read<'de, D: Deserializer<'de>>(
        kind: ValueKind,
        names: &[Symbol],
        d: D,
    ) -> Result<Self, D::Error> {
        let node = NodeSeed::<Self>::new(names);
        match kind {
            ValueKind::I32 => i32::deserialize(d).map(Self::I32),
            ValueKind::I64 => i64::deserialize(d).map(Self::I64),
            ValueKind::Bool => bool::deserialize(d).map(Self::Bool),
            ValueKind::String => String::deserialize(d).map(Self::String),
            ValueKind::Bytes => BytesSeed.deserialize(d).map(Self::Bytes),
            ValueKind::Tuple => ListSeed(node).deserialize(d).map(Self::Tuple),
            ValueKind::Array => ListSeed(node).deserialize(d).map(Self::Array),
            ValueKind::Map => MapSeed(node, node).deserialize(d).map(Self::Map),
            ValueKind::Variant => PairSeed(NameSeed(names), NodeSeed::new(names))
                .deserialize(d)
                .map(|(tag, payload)| Self::Variant(tag, payload)),
            ValueKind::Record => MapSeed(NameSeed(names), node)
                .deserialize(d)
                .map(Self::Record),
        }
    }
}

macro_rules! payload_node {
    ($payload:ty, $item:ty) => {
        impl Node for $payload {
            type Tag = PayloadKind;
            #[inline]
            fn collect<'a>(&'a self, symbols: &mut BTreeSet<&'a str>) {
                match self {
                    Self::Unnamed(items) => collect_list(items, symbols),
                    Self::Named(fields) => collect_fields(fields, symbols),
                }
            }
            fn write<S: Serializer>(&self, symbols: &Symbols<'_>, s: S) -> Result<S::Ok, S::Error> {
                match self {
                    Self::Unnamed(items) => {
                        (PayloadKind::Unnamed, ListRef(items, symbols)).serialize(s)
                    }
                    Self::Named(fields) => {
                        (PayloadKind::Named, FieldsRef(fields, symbols)).serialize(s)
                    }
                }
            }
            fn read<'de, D: Deserializer<'de>>(
                kind: PayloadKind,
                names: &[Symbol],
                d: D,
            ) -> Result<Self, D::Error> {
                let node = NodeSeed::<$item>::new(names);
                match kind {
                    PayloadKind::Unnamed => ListSeed(node).deserialize(d).map(Self::Unnamed),
                    PayloadKind::Named => MapSeed(NameSeed(names), node)
                        .deserialize(d)
                        .map(Self::Named),
                }
            }
        }
    };
}
payload_node!(VariantPayloadValue, Value);
payload_node!(VariantPayloadSchema, SchemaEnum);

#[derive(Serialize, Deserialize)]
enum SchemaTag {
    I32,
    I64,
    Bool,
    String,
    Bytes,
    Tuple,
    Array,
    Map,
    Variant,
    Record,
    Ref,
    ByteBuffer,
}

impl Node for SchemaEnum {
    type Tag = SchemaTag;
    #[inline]
    fn collect<'a>(&'a self, symbols: &mut BTreeSet<&'a str>) {
        match self {
            Self::Tuple(items) => collect_list(items, symbols),
            Self::Array(inner) => inner.collect(symbols),
            Self::Map(key, value) => {
                key.collect(symbols);
                value.collect(symbols);
            }
            Self::Record(fields) => collect_fields(fields, symbols),
            Self::Variant(arms) => collect_fields(arms, symbols),
            Self::I32
            | Self::I64
            | Self::Bool
            | Self::String
            | Self::Bytes(_)
            | Self::Ref(_)
            | Self::ByteBuffer => {}
        }
    }
    fn write<S: Serializer>(&self, symbols: &Symbols<'_>, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::I32 => (SchemaTag::I32, ()).serialize(s),
            Self::I64 => (SchemaTag::I64, ()).serialize(s),
            Self::Bool => (SchemaTag::Bool, ()).serialize(s),
            Self::String => (SchemaTag::String, ()).serialize(s),
            Self::Bytes(n) => (SchemaTag::Bytes, n).serialize(s),
            Self::ByteBuffer => (SchemaTag::ByteBuffer, ()).serialize(s),
            Self::Ref(index) => (SchemaTag::Ref, index).serialize(s),
            Self::Tuple(items) => (SchemaTag::Tuple, ListRef(items, symbols)).serialize(s),
            Self::Array(inner) => (SchemaTag::Array, NodeRef(inner.as_ref(), symbols)).serialize(s),
            Self::Map(key, value) => (
                SchemaTag::Map,
                (
                    NodeRef(key.as_ref(), symbols),
                    NodeRef(value.as_ref(), symbols),
                ),
            )
                .serialize(s),
            Self::Record(fields) => (SchemaTag::Record, FieldsRef(fields, symbols)).serialize(s),
            Self::Variant(arms) => (SchemaTag::Variant, FieldsRef(arms, symbols)).serialize(s),
        }
    }
    fn read<'de, D: Deserializer<'de>>(
        kind: SchemaTag,
        names: &[Symbol],
        d: D,
    ) -> Result<Self, D::Error> {
        let node = NodeSeed::<Self>::new(names);
        match kind {
            SchemaTag::I32 => <()>::deserialize(d).map(|()| Self::I32),
            SchemaTag::I64 => <()>::deserialize(d).map(|()| Self::I64),
            SchemaTag::Bool => <()>::deserialize(d).map(|()| Self::Bool),
            SchemaTag::String => <()>::deserialize(d).map(|()| Self::String),
            SchemaTag::Bytes => usize::deserialize(d).map(Self::Bytes),
            SchemaTag::ByteBuffer => <()>::deserialize(d).map(|()| Self::ByteBuffer),
            SchemaTag::Ref => usize::deserialize(d).map(Self::Ref),
            SchemaTag::Tuple => ListSeed(node).deserialize(d).map(Self::Tuple),
            SchemaTag::Array => node.deserialize(d).map(|v| Self::Array(Box::new(v))),
            SchemaTag::Map => PairSeed(node, node)
                .deserialize(d)
                .map(|(k, v)| Self::Map(Box::new(k), Box::new(v))),
            SchemaTag::Record => MapSeed(NameSeed(names), node)
                .deserialize(d)
                .map(Self::Record),
            SchemaTag::Variant => MapSeed(NameSeed(names), NodeSeed::new(names))
                .deserialize(d)
                .map(Self::Variant),
        }
    }
}

pub(super) fn serialize_value<S: Serializer>(value: &Value, s: S) -> Result<S::Ok, S::Error> {
    let mut names = BTreeSet::new();
    value.collect(&mut names);
    let symbols = Symbols::new(names);
    (&symbols.names, NodeRef(value, &symbols)).serialize(s)
}
pub(super) fn serialize_schema<S: Serializer>(
    schemas: &[SchemaEnum],
    s: S,
) -> Result<S::Ok, S::Error> {
    let mut names = BTreeSet::new();
    collect_list(schemas, &mut names);
    let symbols = Symbols::new(names);
    (&symbols.names, ListRef(schemas, &symbols)).serialize(s)
}

trait DocumentRoot: Sized {
    fn read<'de, D: Deserializer<'de>>(names: &[Symbol], d: D) -> Result<Self, D::Error>;
}
impl DocumentRoot for Value {
    fn read<'de, D: Deserializer<'de>>(names: &[Symbol], d: D) -> Result<Self, D::Error> {
        NodeSeed::new(names).deserialize(d)
    }
}
impl DocumentRoot for Vec<SchemaEnum> {
    fn read<'de, D: Deserializer<'de>>(names: &[Symbol], d: D) -> Result<Self, D::Error> {
        ListSeed(NodeSeed::new(names)).deserialize(d)
    }
}
struct RootSeed<'a, T>(&'a [Symbol], PhantomData<fn() -> T>);
impl<'de, T: DocumentRoot> DeserializeSeed<'de> for RootSeed<'_, T> {
    type Value = T;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<T, D::Error> {
        T::read(self.0, d)
    }
}
struct DocumentVisitor<T>(PhantomData<fn() -> T>);
impl<'de, T: DocumentRoot> Visitor<'de> for DocumentVisitor<T> {
    type Value = T;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a Forest symbol document")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<T, A::Error> {
        let names: Vec<Symbol> = seq
            .next_element()?
            .ok_or_else(|| de::Error::custom("missing symbol table"))?;
        validate_symbols::<A::Error>(&names)?;
        required(&mut seq, RootSeed(&names, PhantomData))
    }
}
pub(super) fn deserialize_value<'de, D: Deserializer<'de>>(d: D) -> Result<Value, D::Error> {
    d.deserialize_tuple(2, DocumentVisitor(PhantomData))
}
pub(super) fn deserialize_schema<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<SchemaEnum>, D::Error> {
    d.deserialize_tuple(2, DocumentVisitor(PhantomData))
}
