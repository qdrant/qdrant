//! Construction and loading of [`Segment`]s and their components.
//!
//! The module is split by the thing being built:
//! - [`paths`] — on-disk path/name conventions for a segment's sub-stores
//! - [`vector_storage`] — dense and sparse vector storage opening
//! - [`payload_storage`] — payload storage opening
//! - [`id_tracker`] — id tracker opening
//! - [`vector_index`] — dense vector index open/build
//! - [`sparse_vector_index`] — sparse vector index open/build (per weight type)
//! - [`create_segment`] — assembling a whole [`Segment`] from the above
//! - [`segment`] — public load/build/normalize entry points
//! - [`legacy_state`] — migration of pre-v0.6 on-disk segment state
//!
//! [`Segment`]: crate::segment::Segment

use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

mod create_segment;
mod id_tracker;
mod legacy_state;
mod paths;
mod payload_storage;
mod segment;
mod sparse_vector_index;
mod vector_index;
mod vector_storage;

pub(crate) use id_tracker::create_mutable_id_tracker;
pub(crate) use paths::get_payload_index_path;
pub use paths::{
    PAYLOAD_INDEX_PATH, VECTOR_INDEX_PATH, VECTOR_STORAGE_PATH, get_vector_index_path,
    get_vector_name_with_prefix, get_vector_storage_path,
};
pub(crate) use payload_storage::create_payload_storage;
pub use segment::{build_segment, load_segment, normalize_segment_dir};
#[cfg(feature = "testing")]
pub use sparse_vector_index::create_sparse_vector_index_test;
pub(crate) use sparse_vector_index::open_or_create_sparse_vector_index;
pub use vector_index::VectorIndexBuildArgs;
pub(crate) use vector_index::{VectorIndexOpenArgs, build_vector_index};
pub(crate) use vector_storage::{create_sparse_vector_storage, open_vector_storage};

/// Wrap a value in a shared, interior-mutable cell — the ubiquitous storage
/// handle type throughout segment assembly.
fn sp<T>(t: T) -> Arc<AtomicRefCell<T>> {
    Arc::new(AtomicRefCell::new(t))
}
