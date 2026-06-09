//! Async SignalR client utilities for native Rust applications.
//!
//! The crate is centered around [`SignalRClient`], which handles negotiation,
//! hub method calls, server-to-client callbacks, and server streaming.
//! Configuration is applied with [`ConnectionConfiguration`] through
//! [`SignalRClient::connect_with`].
//!
//! # Quick start
//!
//! ```no_run
//! use ac_signalr_client::Protocol;
//! use ac_signalr_client::SignalRClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     let mut client = SignalRClient::connect_with("localhost", "chathub", |config| {
//!         config.with_port(5000);
//!         config.unsecure();
//!         config.with_protocol(Protocol::Json);
//!     })
//!     .await?;
//!
//!     client.send("Ping".to_string()).await?;
//!     client.disconnect_gracefully().await?;
//!     Ok(())
//! }
//! ```
//!
//! See the README and the `examples/` directory for end-to-end usage patterns,
//! including MessagePack, skip-negotiation, and reconnect configuration.
#![warn(missing_docs, rustdoc::broken_intra_doc_links)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::ignored_unit_patterns,
    clippy::implicit_clone,
    clippy::map_unwrap_or,
    clippy::missing_errors_doc,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::single_match_else,
    clippy::too_many_lines,
    clippy::trivially_copy_pass_by_ref,
    clippy::uninlined_format_args,
    clippy::used_underscore_binding
)]

mod client;
mod communication;
mod completer;
mod execution;
mod protocol;

pub use client::ConnectionConfiguration;
pub use client::InvocationContext;
pub use client::Protocol;
pub use client::SignalRClient;
pub use completer::CompletedFuture;
pub use completer::ManualFuture;
pub use completer::ManualStream;
pub use execution::ArgumentConfiguration;
pub use execution::CallbackHandler;
pub use protocol::RECORD_SEPARATOR;
