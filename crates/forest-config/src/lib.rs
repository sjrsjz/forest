//! Host-owned object configuration with mandatory defaults and atomic updates.
//!
//! Targets are addresses, not authorization. The scheduler must enforce source,
//! ACL and lifecycle restrictions before exposing this host-only store to guests.
//! SQLite is authoritative; operations run on one connection's background thread.
//! Reads never initialize data. Successful writes have committed their transaction.
//!
//! ```
//! use forest_config::{ConfigStore, Key};
//! use forest_util::token::Token;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let owner: Token = "/module/example".parse()?;
//! let counter = Key::<i64>::new("/counter".parse()?);
//! let store = ConfigStore::open("forest.sqlite3").await?;
//! store.register(owner.clone(), [counter.declare(&0)?]).await?;
//! let target = owner.clone();
//! let key = counter.clone();
//! store.update(move |view| {
//!     let value = view.get(&target, &key)?;
//!     Ok(((), vec![key.set(target, &(value + 1))]))
//! }).await?;
//! assert_eq!(store.get(&owner, &counter).await?, 1);
//! store.reset(&owner, counter.path()).await?;
//! store.close().await?;
//! # Ok(())
//! # }
//! ```

mod entry;
mod snapshot;
mod sql;
mod store;

pub use entry::{Change, Declaration, Entry, Key};
pub use snapshot::{EntitySnapshot, EntrySnapshot, Snapshot};
pub use store::{ConfigStore, ReadView};

use forest_serde::adt::DecodeError;
use forest_util::token::{PathParseError, Token};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("entity already registered: {entity}")]
    DuplicateEntity { entity: Token },
    #[error("unknown entity: {entity}")]
    UnknownEntity { entity: Token },
    #[error("duplicate declaration: {key}")]
    DuplicateKey { key: Token },
    #[error("undeclared entry {key} in {entity}")]
    UnknownKey { entity: Token, key: Token },
    #[error("default value does not conform to declared schema: {key}")]
    InvalidDefault { key: Token },
    #[error("value does not conform to declared schema: {key} in {entity}")]
    InvalidValue { entity: Token, key: Token },
    #[error("typed key does not match declared schema: {key} in {entity}")]
    KeyTypeMismatch { entity: Token, key: Token },
    #[error("typed value decoding failed: {0}")]
    Decode(#[from] DecodeError),
    #[error("invalid stored path {path:?}: {reason}")]
    InvalidPath {
        path: String,
        #[source]
        reason: PathParseError,
    },
    #[error("SQLite operation failed: {0}")]
    Database(#[from] tokio_rusqlite::rusqlite::Error),
    #[error("database worker failed: {0}")]
    Worker(#[from] tokio_rusqlite::Error),
    #[error("configuration JSON encoding or decoding failed: {0}")]
    Json(#[from] serde_json::Error),
}
