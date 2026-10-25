// The prost-generated service code (gaip.v1.rs) returns tonic::Status by
// value, and CI's clippy flags `result_large_err` inside the generated
// file — which we cannot edit. Crate-wide allow, scoped to this lint.
#![allow(clippy::result_large_err)]

//! GAIP transport bindings.
//!
//! The payload contract does not change with the transport: a binding
//! moves [`WireMessage`](gaip_core::WireMessage) bytes and nothing else.
//! Each binding here is tested against a real peer, not against a mock
//! that agrees with itself.

pub mod grpc;
pub mod nats;
