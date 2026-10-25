//! GAIP transport bindings.
//!
//! The payload contract does not change with the transport: a binding
//! moves [`WireMessage`](gaip_core::WireMessage) bytes and nothing else.
//! Each binding here is tested against a real peer, not against a mock
//! that agrees with itself.

pub mod nats;
