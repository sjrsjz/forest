//! Typed OneBot v11 public actions with asynchronous, replaceable transport.
//!
//! Default features require no network runtime and can be used in Wasm.
//! Enable `http` for the native HTTP(S) transport. This crate creates no runtime,
//! listener, or background task and does not perform Forest authorization.

pub mod action;
mod client;
pub mod cq;
#[cfg(feature = "forest")]
pub mod forest;
pub mod message;
pub mod response;

#[cfg(feature = "http")]
pub mod http;

pub use client::{
    ApiFailure, CallError, CallMode, Client, Outcome, ProtocolError, Status, Transport,
};
