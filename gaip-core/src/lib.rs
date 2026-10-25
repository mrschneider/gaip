//! # GAIP — Gaiko Agentic Interoperability Protocol (core)
//!
//! CORBA's GIOP solved one problem and bequeathed three ideas that the
//! agent world is re-learning the hard way:
//!
//! 1. **A wire format, not a framework.** CDR made a big-endian
//!    mainframe and a little-endian x86 agree. The agent equivalent is
//!    not endianness — it is *semantic and structural drift* between a
//!    LangGraph dict, a Java DTO and a Rust zero-copy struct. GAIP's
//!    answer is a strictly versioned envelope plus a JSON-Schema contract
//!    (`schema/gaip-envelope.schema.json`), so a participant never has
//!    to know which runtime produced a message.
//! 2. **A closed vocabulary.** GIOP allowed eight message types and no
//!    invented verbs. Frameworks invent `on_tool_start`, `agent_step`,
//!    `run_completion`... and then every pair of frameworks needs a
//!    translator. GAIP has exactly [`MessageType::ALL`] — six universal
//!    primitives — and *refuses* anything else instead of guessing.
//! 3. **Transport bindings, plural.** GIOP was the spec, IIOP was one
//!    binding. GAIP ships the payload contract once and binds it to a
//!    framed TCP stream, an SSE event stream, or (planned) gRPC and a
//!    log-based bus — the envelope does not change.
//!
//! The crate is IO-free on purpose: [`codec`] turns envelopes into bytes,
//! [`lifecycle`] enforces what may follow what, and a transport only has
//! to move bytes.
//!
//! ```no_run
//! use gaip_core::{Envelope, MessageType};
//!
//! let env = Envelope::task_request(
//!     "msg_1",
//!     "task_1",
//!     "urn:agent:python:langgraph:planner-v2",
//!     "urn:agent:rust:tokio:analytics-engine-v1",
//!     serde_json::json!({"intent": "INVENTORY_OPTIMIZATION"}),
//! );
//! assert_eq!(env.message_type, MessageType::TaskRequest);
//! ```

pub mod codec;
pub mod context;
pub mod envelope;
pub mod error;
pub mod idl;
pub mod ior;
pub mod lifecycle;
pub mod message;
pub mod naming;
pub mod policy;
pub mod security;
pub mod sse;
pub mod trader;
pub mod urn;

pub use context::{ContextList, ServiceContext};
pub use envelope::{Envelope, MessageType};
pub use error::GaipError;
pub use idl::{FieldKind, InterfaceRepository, TypeSpec};
pub use ior::Ior;
pub use lifecycle::{TaskRegistry, TaskState};
pub use message::{LocateStatus, ReplyStatus, SystemMinor, WireMessage};
pub use naming::NamingService;
pub use policy::{Policies, SyncScope};
pub use security::{Credentials, IdentityAssertion};
pub use trader::{Offer, Query, Trader};
pub use urn::AgentUrn;

/// The only protocol version this crate speaks. A message carrying a
/// different *major* version is refused, not "best-effort parsed".
pub const PROTOCOL_VERSION: &str = "1.0";
