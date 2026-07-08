//! Backend-agnostic sync ↔ async bridge for the sync
//! [`common::universal_io::UniversalRead`] surface.
//!
//! This crate owns the machinery that lets the *synchronous* universal-IO traits
//! wrap an *inherently async* storage backend, without knowing what that backend
//! is. A backend is anything that implements [`AsyncRead`]: it describes the
//! async work (range reads, metadata) as `Send + 'static` futures, and this crate
//! drives those futures on a [`BridgeRuntime`] and exposes the result through the
//! sync [`BlobFile`] / [`BlobFs`] wrappers.
//!
//! Concrete backends live in sibling crates, each implementing [`AsyncRead`] for
//! its own handle type:
//! - `io_bridge_object_store` — AWS S3, GCS, Azure (via the `object_store` crate).
//! - `io_bridge_uio_grpc` — a direct gRPC connection to a Qdrant peer (via `uio-grpc-client`).
//!
//! # Components
//!
//! | Type             | Role                                                      |
//! |------------------|-----------------------------------------------------------|
//! | [`AsyncRead`]    | Trait implemented per backend; defines `read_range` etc.  |
//! | [`BlobFile<A>`]  | Sync `UniversalRead` wrapper over an `AsyncRead` + path.  |
//! | [`BlobFs<A>`]    | Sync `UniversalReadFs` wrapper opening `BlobFile`s.       |
//! | [`BridgeRuntime`]| Cheap-to-clone owner of a Tokio runtime.                  |
//! | `PipelineInner`  | Reply channel + slot-keyed caller `user_data` slab.       |
//!
//! # Data flow
//!
//! The caller (sync) lives on the left, the Tokio runtime (async) on the right.
//! There is no intermediary dispatcher thread or request channel: a single read
//! is driven inline via [`Handle::block_on`](tokio::runtime::Handle::block_on),
//! and a batched read is handed to the runtime with
//! [`Handle::spawn`](tokio::runtime::Handle::spawn). Spawned tasks report their
//! result over the pipeline's single reply channel.
//!
//! ```text
//!     CALLER THREAD  (sync)                       RUNTIME WORKER THREADS  (async, Tokio)
//!     ═════════════════════                       ═════════════════════════════════════
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
//!   │ PipelineInner<U>                         │
//!   │   tx, rx : reply channel                 │
//!   │   slots  : Slab<U>                       │
//!   │           (slot -> user_data)            │
//!   └──────────────────────────────────────────┘
//!                  │ schedule(rt, user_data, future)
//!                  │  ─ assigns a slot, stores user_data
//!                  ▼                                      ┌────────────────────────────────┐
//!   ┌────────────────────────────────┐  rt.handle()       │ spawned task:                  │
//!   │ reply_tx = tx.clone()          │  .spawn(task)      │   // future allocates the      │
//!   │ slots.insert(user)  -> slot    │ ─────────────────► │   // destination aligned buf,  │
//!   └────────────────────────────────┘                    │   // copies chunks into it,    │
//!                                                         │   // and returns it as output  │
//!                                                         │   buf = future.await           │
//!                                                         │   reply_tx.send(               │
//!   ┌────────────────────────────────┐    reply channel   │     BridgeResponse{slot, buf}) │
//!   │ wait():                        │ ◄───────────────── │   ).await                      │
//!   │   response =                   │  spawned task uses └────────────────────────────────┘
//!   │     reply_rx.blocking_recv()   │  reply_tx (clone of pipeline reply_tx)
//!   │   user_data =                  │
//!   │     slots.remove(slot)         │
//!   │   buf = response.result?       │
//!   │   return Some((user_data, buf))│
//!   └────────────────────────────────┘
//!
//!   Single-read fast path: BlobFile::read calls runtime.block_on(inner.read_range(..))
//!   directly on the calling thread — no pipeline, no slot, no spawn.
//! ```
//!
//! # Design notes
//!
//! - **One reply channel per pipeline.** Each pipeline owns a reply channel and
//!   clones its sender into every spawned task, which uses it as a return
//!   address — so there is no routing table.
//!
//! - **Runtime decoupling.** A pipeline does not store a [`BridgeRuntime`].
//!   The runtime is supplied per `schedule` call, allowing one pipeline to
//!   dispatch to multiple runtimes and one runtime to serve many pipelines.
//!
//! - **Slot correlation.** Every scheduled request is tagged with a slot id
//!   assigned by a [`Slab`](slab::Slab) holding the caller `user_data`, so
//!   responses arriving out of order can still be paired with their caller
//!   context.
//!
//! - **No `dyn` at the backend boundary.** [`AsyncRead`] is a generic bound, not
//!   a trait object. Spawning takes the future by generic type, so no boxing or
//!   type erasure is needed at the dispatch boundary either.
//!
//! - **No `Bytes` aggregation in the pipeline.** The future allocates the
//!   destination aligned byte buffer itself, copies stream chunks straight
//!   into it, and returns the buffer as its output. The reply channel carries
//!   the buffer back as a plain `AVec<u8>`. This removes the two redundant
//!   copies that an "aggregate to `Bytes`, then copy out" path would do.

mod file;
mod fs;
mod pipeline;
mod read;
mod runtime;

/// Dedicated log target for the blob-backend latency traces emitted across this
/// crate. All of them are `trace!` under this single target, so they are silent
/// by default and can be toggled as one group at runtime without a rebuild — e.g.
/// `POST /logger {"log_level": "io_bridge::latency=trace"}` or
/// `RUST_LOG=io_bridge::latency=trace`.
pub(crate) const LATENCY_LOG_TARGET: &str = "io_bridge::latency";

#[cfg(test)]
mod tests;

pub use file::BlobFile;
pub use fs::BlobFs;
pub use pipeline::BlobReadPipeline;
pub use read::AsyncRead;
pub use runtime::BridgeRuntime;
