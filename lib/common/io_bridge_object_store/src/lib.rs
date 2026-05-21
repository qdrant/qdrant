//! Sync ↔ async bridge that lets the sync [`common::universal_io::UniversalRead`]
//! surface wrap inherently async object-store backends (AWS S3, GCS, Azure).
//!
//! # Components
//!
//! | Type             | Role                                                      |
//! |------------------|-----------------------------------------------------------|
//! | [`AsyncRead`]    | Trait implemented per backend; defines `read_range` etc.  |
//! | [`BlobBackend`]  | Trait: a typed `ObjectStore` + a builder from a `Config`. |
//! | `Arc<S>`         | The [`AsyncRead`] handle; any [`BlobBackend`] `S`.        |
//! | [`BlobFile<A>`]  | Sync `UniversalRead` wrapper over an `AsyncRead` + path.  |
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
//!   │   path    : PathBuf            │
//!   └────────────────────────────────┘
//!                  │
//!                  ▼
//!   ┌──────────────────────────────────────────┐
//!   │ PipelineInner<T, U>                      │
//!   │   tx, rx  : reply channel                │
//!   │   pending : AHashMap<u64, (U, Vec<T>)>   │
//!   │   next_slot : u64                        │
//!   └──────────────────────────────────────────┘
//!                  │ schedule(rt, user_data, Vec<T>, future)
//!                  │  ─ allocates Vec<T> of exact size
//!                  │  ─ moves it into `pending`
//!                  │  ─ derives a SendBytePtr captured by `future`
//!                  ▼
//!   ┌────────────────────────────────┐                    ┌────────────────────────────────┐
//!   │ BridgeRequest {                │   request channel  │ worker_loop:                   │
//!   │   future,                      │ ─────────────────► │   req = request_rx             │
//!   │   tx: reply_tx.clone(),        │ rt.tx().try_send() │           .recv().await        │
//!   │   slot,                        │                    │                                │
//!   │ }                              │                    │   tokio::spawn(async move {    │
//!   └────────────────────────────────┘                    │     // writes into the pending │
//!                                                         │     // Vec<T> via SendBytePtr  │
//!                                                         │     // + AlignedBufWriter      │
//!                                                         │     status = req.future.await  │
//!                                                         │     req.tx.send(               │
//!   ┌────────────────────────────────┐                    │       BridgeResponse {         │
//!   │ wait():                        │    reply channel   │         slot,                  │
//!   │   response =                   │ ◄───────────────── │         result: status,        │
//!   │     reply_rx.blocking_recv()   │  spawned task uses │       },                       │
//!   │   (user_data, buf) =           │  req.tx (clone of  │     ).await                    │
//!   │     pending.remove(&slot)      │  pipeline reply_tx)│   })                           │
//!   │   response.result?             │                    └────────────────────────────────┘
//!   │   return Some((user_data, buf))│
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
//! - **No `dyn` in the backend.** [`AsyncRead`] is implemented for `Arc<S>`,
//!   not `Arc<dyn ObjectStore>`. The only remaining type erasure is the boxed
//!   future in [`BridgeRequest::future`] — required because struct fields
//!   cannot hold `impl Trait` and the channel must carry one concrete type.
//!
//! - **No `Bytes` aggregation in the pipeline.** The pipeline owns the typed
//!   destination `Vec<T>` for every in-flight request. The future writes the
//!   stream chunks straight into that buffer through a `SendBytePtr` + an
//!   `AlignedBufWriter`, and the channel reply carries only a `Result<()>`.
//!   This removes the two redundant copies that an "aggregate to `Bytes`,
//!   then cast to `Vec<T>`" path would do.

mod backend;
pub mod backends;
mod file;
mod pipeline;
mod read;
mod runtime;
mod source;
mod writer;

#[cfg(test)]
mod tests;

pub use backend::BlobBackend;
pub use file::BlobFile;
pub use pipeline::{BorrowedBlobPipeline, OwnedBlobPipeline};
pub use read::AsyncRead;
pub use runtime::{BridgeRequest, BridgeResponse, BridgeRuntime};
