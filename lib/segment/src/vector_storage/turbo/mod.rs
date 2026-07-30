//! TurboQuant-backed dense vector storage.
//!
//! The dense TQ storages are *primary* storages that keep only the TurboQuant
//! (TQ) encoded vectors plus their own deletion state — as opposed to the
//! secondary `QuantizedVectorStorage` layer that sits beside a full-precision
//! storage. Mirroring the reference dense storages, the layout is a *type*, not
//! a runtime flag:
//! - [`TurboVectorStorageImpl`] — single-file, non-appendable data, read
//!   through a [`UniversalRead`](common::universal_io::UniversalRead) backend
//!   `S` (mmap / io_uring),
//! - [`AppendableMmapTurboVectorStorage`] — chunked mmap, appendable.
//!
//! Both implement only [`VectorStorageRead`](crate::vector_storage::VectorStorageRead)
//! + `VectorStorage` (+ the TQ read traits), **not** `DenseVectorStorage<T>`.

pub mod appendable_turbo_vector_storage;
pub mod multi_turbo;
pub mod read_only;
pub(crate) mod shared;
pub mod turbo_vector_storage;

pub use self::appendable_turbo_vector_storage::{
    AppendableMmapTurboVectorStorage, open_appendable_turbo_vector_storage,
};
pub use self::shared::turbo_storage_roundtrip;
pub use self::turbo_vector_storage::{
    TurboVectorStorageImpl, open_turbo_vector_storage, open_turbo_vector_storage_with_uring,
};
