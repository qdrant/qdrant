//! Vector index parked *unopened* by a request-specific
//! [`LoadProfile`](crate::data_types::load_profile::LoadProfile).
//!
//! A cold placement is not enough for the vector index: on a remote backend
//! even a cold HNSW open must mirror the whole links file, because
//! `GraphLinksView` requires one contiguous byte slice — the cache can only
//! lend a borrowed slice once every block is locally present. The only way not
//! to fetch the links is not to open the index at all.
//!
//! [`DeferredVectorIndex`] holds everything the open needs and runs it on
//! first use, so the profile's contract holds: a request that unexpectedly
//! scores a deferred vector still works — it just pays the open then.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use atomic_refcell::AtomicRefCell;
use common::universal_io::Populate;

use super::VectorIndexReadEnum;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::UniversalReadExt;
use crate::index::read_only::ReadOnlyVectorIndexOpenArgs;
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::types::VectorDataConfig;
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// Which open [`DeferredVectorIndex::get`] runs on first use.
pub(super) enum DeferredIndexKind {
    /// [`VectorIndexReadEnum::open`] with this vector config.
    Dense(VectorDataConfig),
    /// [`VectorIndexReadEnum::open_sparse`]; its config is read from disk.
    Sparse,
}

/// A vector index whose open is deferred to its first use.
///
/// Constructed instead of the real index when the load profile says the
/// request never scores this vector (see
/// [`LoadProfile::vector_index_deferred`](crate::data_types::load_profile::LoadProfile::vector_index_deferred)).
pub struct DeferredVectorIndex<S: UniversalReadExt + 'static> {
    /// The index, opened by the first accessor that needs it. On a race both
    /// threads may open; the first to finish wins and the loser's copy is
    /// dropped (same arbitration as `ReadOnlyRoaringFlags::bitmap`).
    index: OnceLock<VectorIndexReadEnum<S>>,

    // Open arguments, retained until the first use. `fs` is the segment's raw
    // backend — the per-segment `CachedReadFs` only lives for the eager open —
    // so a deferred open fetches without the prefetch pool.
    fs: S::Fs,
    path: PathBuf,
    kind: DeferredIndexKind,
    id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
    quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
}

impl<S: UniversalReadExt + 'static> DeferredVectorIndex<S> {
    pub(super) fn new(
        fs: S::Fs,
        path: PathBuf,
        kind: DeferredIndexKind,
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
        payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
        quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
    ) -> Self {
        Self {
            index: OnceLock::new(),
            fs,
            path,
            kind,
            id_tracker,
            vector_storage,
            payload_index,
            quantized_vectors,
        }
    }

    /// The index, opened on the first call.
    ///
    /// The open keeps the cold placement the profile chose: this component was
    /// not expected to be used, so it comes up as lazily as the backend allows.
    pub(super) fn get(&self) -> OperationResult<&VectorIndexReadEnum<S>> {
        if let Some(index) = self.index.get() {
            return Ok(index);
        }

        let args = ReadOnlyVectorIndexOpenArgs {
            fs: &self.fs,
            path: &self.path,
            id_tracker: self.id_tracker.clone(),
            vector_storage: self.vector_storage.clone(),
            payload_index: self.payload_index.clone(),
            quantized_vectors: self.quantized_vectors.clone(),
        };
        let index = match &self.kind {
            DeferredIndexKind::Dense(vector_config) => {
                VectorIndexReadEnum::open(vector_config, args, Some(Populate::No))?
            }
            DeferredIndexKind::Sparse => {
                VectorIndexReadEnum::open_sparse(args, Some(Populate::No))?
            }
        };

        Ok(self.index.get_or_init(|| index))
    }

    /// The index, only if a previous [`get`](Self::get) already opened it.
    ///
    /// For accessors that must not trigger the (potentially remote) open —
    /// telemetry, sizes, cache maintenance — which answer with a conservative
    /// default while the index is unopened.
    pub(super) fn opened(&self) -> Option<&VectorIndexReadEnum<S>> {
        self.index.get()
    }
}
