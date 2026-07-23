//! TurboQuant-backed multivector storage.
//!
//! Multi counterpart of the dense TurboQuant storages, mirroring the reference
//! [`multi_dense`](crate::vector_storage::multi_dense) module. Multivectors are
//! always stored in the appendable chunked layout, so — unlike the dense
//! storage — there is a single writable backend and no layout split (no volatile
//! or single-file variant).

pub mod appendable_mmap_multi_turbo_vector_storage;
pub mod read_only;

pub use self::appendable_mmap_multi_turbo_vector_storage::{
    AppendableMmapMultiTurboVectorStorage, open_appendable_turbo_multi_vector_storage,
};
pub(crate) use self::appendable_mmap_multi_turbo_vector_storage::OFFSETS_PATH;
