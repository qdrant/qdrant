//! A read-only Qdrant edge shard, compiled to `wasm32-wasip2` and queried straight from object
//! storage.
//!
//! # Why WASI, and not the browser
//!
//! [`UniversalRead`](common::universal_io::UniversalRead) — the interface every segment component
//! reads through — is *synchronous*. WASI gives the guest synchronous sockets and a synchronous
//! filesystem, so that interface is satisfiable directly: no async runtime, no threads, and none of
//! the sync/async bridging `io_bridge` does for the native blob backends.
//!
//! A browser cannot do this. There, `fetch` only resolves when the JS event loop runs, so blocking
//! the calling thread deadlocks; the read path would have to move to a Web Worker blocking on
//! `Atomics.wait`, or to OPFS sync access handles. That is separate work — this crate deliberately
//! stops at the target where the existing synchronous interface already fits.
//!
//! # Shape
//!
//! [`object_store`] downloads every object under a prefix into [`MemFs`](mem_fs::MemFs), and
//! [`MemEdgeShard`](shard::MemEdgeShard) opens a `ReadOnlyEdgeShard` over it. Reads are then slices
//! of linear memory.
//!
//! Preloading is a simplification, not a requirement: because the reads already block, a backend
//! issuing an HTTP Range request per read drops straight into the same [`UniversalRead`] slot and
//! removes both the up-front download and the resident-set ceiling.
//!
//! Nothing here is `cfg`-gated on the target — the same code runs natively, which is what
//! `tests/mem_fs_round_trip.rs` and a native `cargo run` exercise.

pub mod mem_fs;
pub mod object_store;
pub mod shard;
