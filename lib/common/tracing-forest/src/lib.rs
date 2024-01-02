//! [![github-img]][github-url] [![crates-img]][crates-url] [![docs-img]][docs-url]
//!
//! [github-url]: https://github.com/QnnOkabayashi/tracing-forest
//! [crates-url]: https://crates.io/crates/tracing-forest
//! [docs-url]: crate
//! [github-img]: https://img.shields.io/badge/github-8da0cb?style=for-the-badge&labelColor=555555&logo=github
//! [crates-img]: https://img.shields.io/badge/crates.io-fc8d62?style=for-the-badge&labelColor=555555&logo=rust
//! [docs-img]: https://img.shields.io/badge/docs.rs-66c2a5?style=for-the-badge&labelColor=555555&logoColor=white&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K
//!
//! Preserve contextual coherence among trace data from concurrent tasks.
//!
//! # Overview
//!
//! [`tracing`] is a framework for instrumenting programs to collect structured
//! and async-aware diagnostics via the [`Subscriber`] trait. The
//! [`tracing-subscriber`] crate provides tools for composing [`Subscriber`]s
//! from smaller units. This crate extends [`tracing-subscriber`] by providing
//! [`ForestLayer`], a [`Layer`] that preserves contextual coherence of trace
//! data from concurrent tasks when logging.
//!
//! This crate is intended for programs running many nontrivial and disjoint
//! tasks concurrently, like server backends. Unlike [other `Subscriber`s][crate#contextual-coherence-in-action]
//! which simply keep track of the context of an event, `tracing-forest` preserves
//! the contextual coherence when writing logs even in parallel contexts, allowing
//! readers to easily trace a sequence of events from the same task.
//!
//! `tracing-forest` is intended for authoring applications.
//!
//! [`tracing-subscriber`]: tracing_subscriber
//! [`Layer`]: tracing_subscriber::layer::Layer
//! [`Subscriber`]: tracing::subscriber::Subscriber
//!
//! # Getting started
//!
//! The easiest way to get started is to enable all features. Do this by
//! adding the following to your `Cargo.toml` file:
//! ```toml
//! tracing-forest = { version = "0.1.6", features = ["full"] }
//! ```
//! Then, add [`tracing_forest::init`](crate::init) to your main function:
//! ```
//! fn main() {
//!     // Initialize a default `ForestLayer` subscriber
//!     tracing_forest::init();
//!     // ...
//! }
//! ```
//! This crate also provides tools for much more advanced configurations:
//! ```
//! use tracing_forest::{traits::*, util::*};
//!
//! #[tokio::main]
//! async fn main() {
//!     tracing_forest::worker_task()
//!         .set_global(true)
//!         .map_sender(|sender| sender.or_stderr())
//!         .build_on(|subscriber| subscriber
//!             .with(EnvFilter::from_default_env())
//!             .with(LevelFilter::INFO)
//!         )
//!         .on(async {
//!             // -- snip --
//!         })
//!         .await;
//! }
//! ```
//! For useful configuration abstractions, see the [`runtime` module documentation][runtime].
//!
//! # Contextual coherence in action
//!
//! Similar to this crate, the [`tracing-tree`] crate collects and writes trace
//! data as a tree. Unlike this crate, it doesn't maintain contextual coherence
//! in parallel contexts.
//!
//! Observe the below program, which simulates serving multiple clients concurrently.
//! ```
//! # async fn some_expensive_operation() {}
//! # mod tracing_tree {
//! #     #[derive(Default)]
//! #     pub struct HierarchicalLayer;
//! #     impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for HierarchicalLayer {}
//! # }
//! use tracing::info;
//! use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
//! use tracing_tree::HierarchicalLayer;
//!
//! #[tracing::instrument]
//! async fn conn(id: u32) {
//!     for i in 0..3 {
//!         some_expensive_operation().await;
//!         info!(id, "step {}", i);
//!     }
//! }
//!
//! #[tokio::main(flavor = "multi_thread")]
//! async fn main() {
//!     // Use a `tracing-tree` subscriber
//!     Registry::default()
//!         .with(HierarchicalLayer::default())
//!         .init();
//!
//!     let connections: Vec<_> = (0..3)
//!         .map(|id| tokio::spawn(conn(id)))
//!         .collect();
//!
//!     for conn in connections {
//!         conn.await.unwrap();
//!     }
//! }
//! ```
//! `tracing-tree` isn't intended for concurrent use, and this is demonstrated
//! by the output of the program:
//! ```log
//! conn id=2
//! conn id=0
//! conn id=1
//!   23ms  INFO step 0, id=2
//!   84ms  INFO step 0, id=1
//!   94ms  INFO step 1, id=2
//!   118ms  INFO step 0, id=0
//!   130ms  INFO step 1, id=1
//!   193ms  INFO step 2, id=2
//!
//!   217ms  INFO step 1, id=0
//!   301ms  INFO step 2, id=1
//!
//!   326ms  INFO step 2, id=0
//!
//! ```
//! We can instead use `tracing-forest` as a drop-in replacement for `tracing-tree`.
//! ```
//! use tracing::info;
//! use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};
//! use tracing_forest::ForestLayer;
//!
//! #[tracing::instrument]
//! async fn conn(id: u32) {
//!     // -- snip --
//! }
//!
//! #[tokio::main(flavor = "multi_thread")]
//! async fn main() {
//!     // Use a `tracing-forest` subscriber
//!     Registry::default()
//!         .with(ForestLayer::default())
//!         .init();
//!
//!     // -- snip --
//! }
//! ```
//! Now we can easily trace what happened:
//! ```log
//! INFO     conn [ 150µs | 100.00% ] id: 1
//! INFO     ┝━ ｉ [info]: step 0 | id: 1
//! INFO     ┝━ ｉ [info]: step 1 | id: 1
//! INFO     ┕━ ｉ [info]: step 2 | id: 1
//! INFO     conn [ 343µs | 100.00% ] id: 0
//! INFO     ┝━ ｉ [info]: step 0 | id: 0
//! INFO     ┝━ ｉ [info]: step 1 | id: 0
//! INFO     ┕━ ｉ [info]: step 2 | id: 0
//! INFO     conn [ 233µs | 100.00% ] id: 2
//! INFO     ┝━ ｉ [info]: step 0 | id: 2
//! INFO     ┝━ ｉ [info]: step 1 | id: 2
//! INFO     ┕━ ｉ [info]: step 2 | id: 2
//! ```
//!
//! [`tracing-tree`]: https://crates.io/crates/tracing-tree
//!
//! # Categorizing events with tags
//!
//! This crate allows attaching supplemental categorical information to events with [`Tag`]s.
//!
//! Without tags, it's difficult to distinguish where events are occurring in a system.
//! ```log
//! INFO     ｉ [info]: some info for the admin
//! ERROR    🚨 [error]: the request timed out
//! ERROR    🚨 [error]: the db has been breached
//! ```
//!
//! Tags help make this distinction more visible.
//! ```log
//! INFO     ｉ [admin.info]: some info for the admin
//! ERROR    🚨 [request.error]: the request timed out
//! ERROR    🔐 [security.critical]: the db has been breached
//! ```
//!
//! See the [`tag` module-level documentation](mod@crate::tag) for details.
//!
//! # Attaching `Uuid`s to trace data
//!
//! When the `uuid` feature is enabled, the `ForestLayer` will automatically attach
//! [`Uuid`]s to trace data. Events will adopt the UUID of their span, or the "nil"
//! UUID at the root level. Spans will adopt the UUID of parent spans, or generate
//! a new UUID at the root level.
//!
//! A span's `Uuid` can also be passed in manually to override adopting the parent's
//! `Uuid` by passing it in as a field named `uuid`:
//! ```
//! # use tracing::info_span;
//! # use uuid::Uuid;
//! let id = Uuid::new_v4();
//!
//! let span = info_span!("my_span", uuid = %id);
//! ```
//!
//! It can also be retreived from the most recently entered span with
//! [`tracing_forest::id`](crate::id):
//! ```
//! # use tracing::info_span;
//! # use uuid::Uuid;
//! # tracing_forest::init();
//! let id = Uuid::new_v4();
//!
//! info_span!("my_span", uuid = %id).in_scope(|| {
//!     let current_id = tracing_forest::id();
//!
//!     assert!(id == current_id);
//! });
//! ```
//!
//! # Immediate logs
//!
//! Since `tracing-forest` stores trace data in memory until the root span finishes,
//! it can be a long time until a log is written. This may not be acceptable in
//! certain use cases.
//!
//! To resolve this, the `immediate` field can be used on an event to print the
//! event and its parent spans to stderr. Unlike `eprintln!`, the event will
//! still appear in the trace tree written once the root span closes.
//!
//! ## Example
//!
//! ```
//! use tracing::{info, trace_span};
//!
//! tracing_forest::init();
//!
//! trace_span!("my_span").in_scope(|| {
//!     info!("first");
//!     info!("second");
//!     info!(immediate = true, "third, but immediately");
//! });
//! ```
//! ```log
//! INFO     ｉ IMMEDIATE ｉ my_span > third, but immediately
//! TRACE    my_span [ 125µs | 100.000% ]
//! INFO     ┝━ ｉ [info]: first
//! INFO     ┝━ ｉ [info]: second
//! INFO     ┕━ ｉ [info]: third, but immediately
//! ```
//!
//! # Feature flags
//!
//! This crate uses feature flags to reduce dependency bloat.
//!
//! * `full`: Enables all features listed below.
//! * `uuid`: Enables spans to carry operation IDs.
//! * `chrono`: Enables timestamps on trace data.
//! * `ansi`: Enables ANSI terminal colors.
//! * `smallvec`: Enables some performance optimizations.
//! * `tokio`: Enables [`worker_task`] and [`capture`].
//! * `serde`: Enables log trees to be serialized, which is [useful for formatting][serde_fmt].
//! * `env-filter`: Re-exports [`EnvFilter`] from the [`util`] module.
//!
//! By default, only `smallvec` in enabled.
//!
//! [`Uuid`]: uuid::Uuid
//! [serde_fmt]: crate::printer::Formatter#examples
//! [`EnvFilter`]: tracing_subscriber::EnvFilter

#![doc(issue_tracker_base_url = "https://github.com/QnnOkabayashi/tracing-forest/issues")]
#![cfg_attr(
    docsrs,
    // Allows displaying cfgs/feature flags in the documentation.
    feature(doc_cfg),
    // Allows adding traits to RustDoc's list of "notable traits"
    // feature(doc_notable_trait),
    // Fail the docs build if any intra-docs links are broken
    deny(rustdoc::broken_intra_doc_links),
)]
#![deny(warnings)]
#![warn(unused_extern_crates)]
#![warn(missing_docs)]

pub mod printer;
pub mod processor;
pub mod tag;
pub mod tree;
#[macro_use]
mod cfg;
mod fail;
mod layer;

pub use layer::{init, test_init, ForestLayer};
pub use printer::{Formatter, PrettyPrinter, Printer};
pub use processor::Processor;
pub use tag::Tag;

cfg_tokio! {
    pub mod runtime;
    pub use runtime::{capture, worker_task};
}

cfg_uuid! {
    pub use layer::id::id;
}

/// Bring traits from this crate, `tracing`, and `tracing_subscriber` into scope
/// anonymously.
pub mod traits {
    pub use crate::Processor as _;
    pub use tracing::Instrument as _;
    pub use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
}

/// Bring Tracing's event and span macros into scope, along with other sensible defaults.
pub mod util {
    #[doc(no_inline)]
    pub use crate::ForestLayer;
    #[doc(no_inline)]
    pub use tracing::metadata::LevelFilter;
    #[doc(no_inline)]
    pub use tracing::{
        debug, debug_span, error, error_span, info, info_span, trace, trace_span, warn, warn_span,
        Event, Level,
    };
    #[cfg(feature = "env-filter")]
    #[doc(no_inline)]
    pub use tracing_subscriber::EnvFilter;
}
