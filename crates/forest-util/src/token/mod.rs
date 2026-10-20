//! Source paths and whole-path wildcard matching.
//!
//! A [`Token`] is an identity value, not proof of identity or a bearer credential.
//! The scheduler must bind the actual source independently of guest-provided data.
//! This module performs no authentication, authorization, or I/O.
//!
//! Paths are generic over their segment type. Text configuration uses [`Segment`]:
//!
//! ```
//! use forest_util::token::{Token, TokenPattern};
//!
//! let source: Token = "/module/example".parse()?;
//! let pattern: TokenPattern = "/module/*".parse()?;
//! assert!(pattern.matches(&source));
//! # Ok::<(), forest_util::token::PathParseError>(())
//! ```

mod path;
mod pattern;
#[cfg(feature = "serde")]
mod serde;
mod text;

pub use path::Token;
pub use pattern::{PatternSegment, TokenPattern};
pub use text::{PathParseError, Segment, SegmentError};
