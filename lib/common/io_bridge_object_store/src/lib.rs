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
//! | [`BridgeRuntime`]| Cheap-to-clone owner of a Tokio runtime.                  |
//! | `PipelineInner`  | Reply channel + slot-keyed caller `user_data` slab.       |
//!
//! Concrete backends live under [`backends`]: [`backends::aws`],
//! [`backends::gcp`], and [`backends::azure`]. Each module declares its own
//! `Config` struct and a manual [`BlobBackend`] impl.
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
//!   │ PipelineInner<B, U>                      │
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
//! - **No `dyn` in the backend.** [`AsyncRead`] is implemented for `Arc<S>`,
//!   not `Arc<dyn ObjectStore>`. Spawning takes the future by generic type, so
//!   no boxing or type erasure is needed at the dispatch boundary either.
//!
//! - **No `Bytes` aggregation in the pipeline.** The future allocates the
//!   destination aligned byte buffer itself, copies stream chunks straight
//!   into it, and returns the buffer as its output. The reply channel carries
//!   the buffer back as a plain `AVec<u8>`. This removes the two redundant
//!   copies that an "aggregate to `Bytes`, then copy out" path would do.

mod backend;
pub mod backends;
mod file;
mod fs;
mod pipeline;
mod read;
mod runtime;
mod source;

#[cfg(test)]
mod tests;

pub use backend::BlobBackend;
pub use file::BlobFile;
pub use fs::BlobFs;
pub use pipeline::{BorrowedBlobPipeline, OwnedBlobPipeline};
pub use read::AsyncRead;
pub use runtime::BridgeRuntime;
