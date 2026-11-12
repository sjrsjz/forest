//! Structural names reserved by the binary format. IDs are local to a document.

use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

/// Immutable structural name. Static declarations allocate nothing; decoded
/// names share document-owned text without giving local IDs logical identity.
#[derive(Clone)]
pub struct Symbol(Text);

#[derive(Clone)]
enum Text {
    Static(&'static str),
    Shared(Arc<str>),
}

impl Symbol {
    #[inline]
    pub const fn from_static(text: &'static str) -> Self {
        Self(Text::Static(text))
    }
    #[inline]
    pub fn as_str(&self) -> &str {
        match &self.0 {
            Text::Static(text) => text,
            Text::Shared(text) => text,
        }
    }
}
impl From<String> for Symbol {
    #[inline]
    fn from(text: String) -> Self {
        Self(Text::Shared(text.into()))
    }
}
impl From<&str> for Symbol {
    #[inline]
    fn from(text: &str) -> Self {
        Self(Text::Shared(text.into()))
    }
}
impl From<Symbol> for String {
    #[inline]
    fn from(symbol: Symbol) -> Self {
        symbol.as_str().into()
    }
}
impl Deref for Symbol {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl Borrow<str> for Symbol {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
impl AsRef<str> for Symbol {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl PartialEq for Symbol {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for Symbol {}
impl PartialOrd for Symbol {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Symbol {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}
impl Hash for Symbol {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}
impl fmt::Debug for Symbol {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}
impl fmt::Display for Symbol {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl Serialize for Symbol {
    #[inline]
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}
impl<'de> Deserialize<'de> for Symbol {
    #[inline]
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_str(SymbolVisitor)
    }
}

struct SymbolVisitor;
impl<'de> serde::de::Visitor<'de> for SymbolVisitor {
    type Value = Symbol;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a structural name")
    }
    fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Symbol, E> {
        Ok(text.into())
    }
    fn visit_string<E: serde::de::Error>(self, text: String) -> Result<Symbol, E> {
        Ok(text.into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SymbolId(pub u32);

pub const NONE_TAG: &str = "None";
pub const SOME_TAG: &str = "Some";
pub const NONE_SYMBOL: SymbolId = SymbolId(0);
pub const SOME_SYMBOL: SymbolId = SymbolId(1);

/// Reserved names and IDs have one authority; entries are omitted from the wire table.
pub const RESERVED_SYMBOLS: &[(SymbolId, &str)] =
    &[(NONE_SYMBOL, NONE_TAG), (SOME_SYMBOL, SOME_TAG)];
pub const FIRST_DYNAMIC_SYMBOL: u32 = {
    let mut next = 0;
    let mut index = 0;
    while index < RESERVED_SYMBOLS.len() {
        let after = RESERVED_SYMBOLS[index].0.0 + 1;
        if after > next {
            next = after;
        }
        index += 1;
    }
    next
};
