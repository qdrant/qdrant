//! Sync ↔ async bridge that lets the sync [`common::universal_io::UniversalRead`]
//! surface wrap inherently async object-store backends (S3, GCS, …).
//!
//! # Components
//!
//! | Type             | Role                                                      |
//! |------------------|-----------------------------------------------------------|
//! | [`AsyncRead`]    | Trait implemented per backend; defines `read_range` etc.  |
//! | [`S3Source`]     | Concrete `AsyncRead` impl over `object_store::aws`.       |
//! | [`BlobFile<A>`]  | Sync `UniversalRead` wrapper over an `AsyncRead`.         |
//! | [`BridgeRuntime`]| Owns a Tokio runtime + a worker thread + request channel. |
//! | `PipelineInner`  | Per-pipeline reply channel and slot bookkeeping.          |
//!
//! # Data flow
//!
//! ```text
//!  ┌──────────────────────── caller thread ─────────────────────────┐
//!  │                                                                │
//!  │   BlobFile<A>                                                  │
//!  │     ├─ inner   : A           (e.g. S3Source)                   │
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
//!  │             let bytes = req.future.await; // S3 GET runs   │   │
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
//!   simply replies on the sender it was handed.
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
//! - **Type erasure at the channel boundary only.** [`AsyncRead::read_range`]
//!   returns an unboxed `impl Future<Output = Result<Bytes>> + Send + 'static`.
//!   Boxing into `Pin<Box<dyn Future>>` happens once, inside `schedule`,
//!   because the [`BridgeRequest::future`] struct field cannot hold
//!   `impl Trait`.

mod config;
mod file;
mod pipeline;
mod read;
mod runtime;
pub mod s3;

#[cfg(test)]
mod tests;

pub use config::{S3Config, S3Credentials, build_object_store};
pub use file::BlobFile;
pub use pipeline::{BorrowedBlobPipeline, OwnedBlobPipeline};
pub use read::AsyncRead;
pub use runtime::{BridgeRequest, BridgeResponse, BridgeRuntime};
pub use s3::S3Source;
