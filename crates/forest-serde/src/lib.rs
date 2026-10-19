//! This crate provides the core traits and implementations for the Forest serialization framework, including the `AsSchema` and `AsValue` traits,
//! as well as default implementations for common Rust types. It also re-exports the procedural macros for deriving schemas and values.

/// The `adt` module contains the core traits and default implementations for various Rust types.
extern crate self as forest_serde;

pub mod adt;

pub mod parser;

/// Derive macros for automatically generating `AsSchema` and `AsValue` implementations for structs and enums.
pub use forest_serde_macros::Schema as DeriveSchema;
pub use forest_serde_macros::Value as DeriveValue;
