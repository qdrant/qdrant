//! Sync ↔ async bridge that lets the sync [`common::universal_io::UniversalRead`]
//! surface wrap inherently async object-store backends (AWS S3, GCS, Azure,
//! local filesystem).
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
//! [`backends::gcp`], [`backends::azure`], [`backends::local`]. Each one
//! declares its own `Config` struct and is wired up via the
//! [`impl_blob_backend!`] macro.
//!
//! # Data flow
//!
//! ```text
//!  ┌──────────────────────── caller thread ─────────────────────────┐
//!  │                                                                │
//!  │   BlobFile<A>                                                  │
//!  │     ├─ inner   : A           (e.g. BlobSource<AmazonS3>)       │
//!  │     └─ runtime : BridgeRuntime                                 │
//!  │                                                                │
//!  │   single read   ── runtime.block_on(inner.read_range(r)) ──┐   │
//!  │                                                            │   │
//!  │   batched read  ── PipelineInner<U>::schedule ─┐           │   │
//!  │     ├─ tx, rx     : reply channel              │           │   │
//!  │     ├─ pending    : AHashMap<slot, user_data>  │           │   │
//!  │     └─ next_slot  : u64                        │           │   │
//!  │                                                ▼           │   │
//!  │                                  BridgeRequest {           │   │
//!  │                                    future,                 │   │
//!  │                                    tx: reply_tx.clone(),   │   │
//!  │                                    slot,                   │   │
//!  │                                  }                         │   │
//!  └────────────────────────────────────────┬───────────────────┼───┘
//!                                           │                   │
//!         request channel (per runtime) ────┤                   │
//!                                           ▼                   │
//!  ┌──────────────────── BridgeRuntime worker ──────────────────┼───┐
//!  │                                                            │   │
//!  │   worker_loop:                                             │   │
//!  │     while let Some(req) = request_rx.recv().await {        │   │
//!  │         tokio::spawn(async move {                          │   │
//!  │             let bytes = req.future.await; // network GET   │   │
//!  │             req.tx                                         │   │
//!  │                .send(BridgeResponse { slot, bytes })       │   │
//!  │                .await;                                     │   │
//!  │         });                                                │   │
//!  │     }                                                      │   │
//!  └────────────────────────────────────────┬───────────────────┼───┘
//!                                           │                   │
//!           reply channel (per pipeline) ───┤                   │
//!                                           ▼                   ▼
//!  ┌──────────────────────── caller thread ─────────────────────────┐
//!  │                                                                │
//!  │   wait():                                                      │
//!  │     let response  = rx.blocking_recv();                        │
//!  │     let user_data = pending.remove(&response.slot);            │
//!  │     let items     = bytemuck::try_cast_slice(&response.bytes?);│
//!  │     return Some((user_data, items));                           │
//!  │                                                                │
//!  └────────────────────────────────────────────────────────────────┘
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
