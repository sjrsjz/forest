//! Immutable configuration ACL metadata and pure permission queries.
//!
//! The host constructs this table from owned declarations and supplies the real
//! subject to [`AclTable::check`]. Tokens passed here are values, not credentials.
//! This module does not authenticate callers or enforce lifecycle restrictions.
//!
//! ```
//! use forest_util::{
//!     acl::{Access, AccessPolicy, AclEntry, AclTable, SubjectPattern},
//!     token::Token,
//! };
//!
//! let owner: Token = "/module/example".parse()?;
//! let key: Token = "/counter".parse()?;
//! let acl = AclTable::new([AclEntry {
//!     entity: owner.clone(),
//!     key: key.clone(),
//!     owner: owner.clone(),
//!     access: AccessPolicy {
//!         readers: vec![SubjectPattern::SelfModule],
//!         writers: vec![SubjectPattern::SelfModule],
//!     },
//! }])?;
//! assert!(acl.check(&owner, &owner, &key, Access::Read));
//! assert!(!acl.check(&owner, &owner, &"/missing".parse()?, Access::Read));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::{collections::HashMap, fmt, hash::Hash};

use crate::token::{Segment, Token, TokenPattern};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubjectPattern<K = Segment> {
    /// The declaration's owning module instance, never the current caller.
    SelfModule,
    Path(TokenPattern<K>),
}

/// Both lists must be provided explicitly; an empty list grants no access.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessPolicy<K = Segment> {
    pub readers: Vec<SubjectPattern<K>>,
    pub writers: Vec<SubjectPattern<K>>,
}

/// Host-bound declaration metadata. Entity and owner are separate addresses:
/// configuration on a Bot/user entity may belong to a module instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclEntry<K = Segment> {
    pub entity: Token<K>,
    pub key: Token<K>,
    pub owner: Token<K>,
    pub access: AccessPolicy<K>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuplicateEntry<K = Segment> {
    pub entity: Token<K>,
    pub key: Token<K>,
}

impl<K: fmt::Debug> fmt::Display for DuplicateEntry<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "duplicate ACL entry: {:?}, {:?}",
            self.entity, self.key
        )
    }
}

impl<K: fmt::Debug> std::error::Error for DuplicateEntry<K> {}

#[derive(Clone, Debug)]
struct Rule<K> {
    owner: Token<K>,
    access: AccessPolicy<K>,
}

/// Exact (entity, key) lookup followed by subject matching. No live mutations.
#[derive(Clone, Debug)]
pub struct AclTable<K = Segment> {
    entries: HashMap<Token<K>, HashMap<Token<K>, Rule<K>>>,
}

impl<K> Default for AclTable<K> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<K: Clone + Eq + Hash> AclTable<K> {
    /// Builds a whole table. Duplicate targets are rejected rather than silently
    /// replacing or merging access rules; no partial table is returned.
    pub fn new(entries: impl IntoIterator<Item = AclEntry<K>>) -> Result<Self, DuplicateEntry<K>> {
        let mut table = Self::default();
        for AclEntry {
            entity,
            key,
            owner,
            access,
        } in entries
        {
            let keys = table.entries.entry(entity.clone()).or_default();
            match keys.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    return Err(DuplicateEntry {
                        entity,
                        key: entry.key().clone(),
                    });
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(Rule { owner, access });
                }
            }
        }
        Ok(table)
    }
}

impl<K: Eq + Hash> AclTable<K> {
    /// Returns false for every unregistered (entity, key) pair, even if a
    /// different entry grants the subject access through a wildcard.
    /// This query allocates nothing and never inserts or modifies a rule.
    pub fn check(
        &self,
        subject: &Token<K>,
        entity: &Token<K>,
        key: &Token<K>,
        access: Access,
    ) -> bool {
        let Some(rule) = self.entries.get(entity).and_then(|keys| keys.get(key)) else {
            return false;
        };
        let patterns = match access {
            Access::Read => &rule.access.readers,
            Access::Write => &rule.access.writers,
        };
        patterns.iter().any(|pattern| match pattern {
            SubjectPattern::SelfModule => subject == &rule.owner,
            SubjectPattern::Path(pattern) => pattern.matches(subject),
        })
    }
}
