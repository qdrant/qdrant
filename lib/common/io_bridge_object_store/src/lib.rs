//! Sync ↔ async bridge that lets the sync [`common::universal_io::UniversalRead`]
//! surface wrap inherently async object-store backends (AWS S3, GCS, Azure).
//!
//! # Components
//!
//! | Type             | Role                                                      |
//! |------------------|-----------------------------------------------------------|
//! | [`AsyncRead`]    | Trait implemented per source; defines `read_range` etc.   |
//! | [`BlobBackend`]  | Trait: a typed `ObjectStore` + a builder from a `Config`. |
//! | [`BlobSource<S>`]| Per-object handle; generic over any [`BlobBackend`].      |
//! | [`BlobFile<A>`]  | Sync `UniversalRead` wrapper over an `AsyncRead`.         |
//! | [`BridgeRuntime`]| Owns a Tokio runtime + a worker thread + request channel. |
//! | `PipelineInner`  | Per-pipeline reply channel and slot bookkeeping.          |
//!
//! Concrete backends live under [`backends`]: [`backends::aws`],
//! [`backends::gcp`], and [`backends::azure`]. Each module declares its own
//! `Config` struct and a manual [`BlobBackend`] impl.
//!
//! # Data flow
//!
//! The caller (sync) lives on the left, the [`BridgeRuntime`] worker thread
//! (async) lives on the right. The two MPSC channels run between them, with
//! request going right and reply going left.
//!
//! ```text
//!     CALLER THREAD  (sync)                         WORKER THREAD  (async, Tokio)
//!     ═════════════════════                         ═════════════════════════════
//!
//!   ┌────────────────────────────────┐
//!   │ BlobFile<A>                    │
//!   │   inner   : A                  │
//!   │   runtime : BridgeRuntime      │
//!   └────────────────────────────────┘
//!                  │
//!                  ▼
//!   ┌────────────────────────────────┐
//!   │ PipelineInner<U>               │
//!   │   tx, rx  : reply channel      │
//!   │   pending : AHashMap<u64, U>   │
//!   │   next_slot : u64              │
//!   └────────────────────────────────┘
//!                  │ schedule(rt, user_data, future)
//!                  ▼
//!   ┌────────────────────────────────┐                    ┌────────────────────────────────┐
//!   │ BridgeRequest {                │   request channel  │ worker_loop:                   │
//!   │   future,                      │ ─────────────────► │   req = request_rx             │
//!   │   tx: reply_tx.clone(),        │ rt.tx().try_send() │           .recv().await        │
//!   │   slot,                        │                    │                                │
//!   │ }                              │                    │   tokio::spawn(async move {    │
//!   └────────────────────────────────┘                    │     bytes = req.future.await   │
//!                                                         │     req.tx.send(               │
//!                                                         │       BridgeResponse {         │
//!   ┌────────────────────────────────┐                    │         slot,                  │
//!   │ wait():                        │    reply channel   │         bytes,                 │
//!   │   response =                   │ ◄───────────────── │       },                       │
//!   │     reply_rx.blocking_recv()   │  spawned task uses │     ).await                    │
//!   │   user_data =                  │  req.tx (clone of  │   })                           │
//!   │     pending.remove(&slot)      │  pipeline reply_tx)└────────────────────────────────┘
//!   │   items = cast(response.bytes?)│
//!   │   return Some((user_data,      │
//!   │                items))         │
//!   └────────────────────────────────┘
//!
//!   Single-read fast path: BlobFile::read calls runtime.block_on(inner.read_range(..))
//!   directly on the Tokio handle — no pipeline, no slot, no worker hop.
//! ```
//!
//! # Design notes
//!
//! - **Two channels.** Each [`BridgeRuntime`] owns one request channel; each
//!   pipeline owns one reply channel. A [`BridgeRequest`] carries the reply
//!   sender as a return address, so the worker has no routing table — it
//!   replies on the sender it was handed.
//!
//! - **Runtime decoupling.** A pipeline does not store a [`BridgeRuntime`].
//!   The runtime is supplied per `schedule` call, allowing one pipeline to
//!   dispatch to multiple runtimes and one runtime to serve many pipelines.
//!
//! - **Slot correlation.** Every scheduled request is tagged with a monotonic
//!   `u64` slot. The pipeline keeps an `AHashMap<slot, user_data>` so that
//!   responses arriving out of order can still be paired with their caller
//!   context.
//!
//! - **No `dyn` in the source.** [`BlobSource<S>`] holds `Arc<S>`, not
//!   `Arc<dyn ObjectStore>`. The only remaining type erasure is the boxed
//!   future in [`BridgeRequest::future`] — required because struct fields
//!   cannot hold `impl Trait` and the channel must carry one concrete type.

mod backend;
pub mod backends;
mod file;
mod pipeline;
mod read;
mod runtime;
mod source;

#[cfg(test)]
mod tests;

pub use backend::BlobBackend;
pub use file::BlobFile;
pub use pipeline::{BorrowedBlobPipeline, OwnedBlobPipeline};
pub use read::AsyncRead;
pub use runtime::{BridgeRequest, BridgeResponse, BridgeRuntime};
pub use source::BlobSource;
