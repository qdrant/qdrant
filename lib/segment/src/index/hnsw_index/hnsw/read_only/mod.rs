// NOTE: This module does NOT compile yet. It is the "naive reuse" version of the
// read-only HNSW index that delegates search to the existing
// [`HNSWIndexReadView`] search implementation. It is here to show the shape; two
// things still block compilation:
//   1. The view struct reuses the same `I`/`V` type params for both the
//      standalone storages and the (always in-RAM) payload view, so building the
//      view in `with_view` fails (`id_tracker` wants `ReadOnlyIdTrackerEnum<S>`,
//      the payload view forces `IdTrackerEnum`).
//   2. The search body builds scorers from the concrete `VectorStorageEnum` /
//      `QuantizedVectors` (`new_raw_scorer`, `FilteredScorer::new`, ...), which
//      do not accept `VectorStorageReadEnum<S>` / `QuantizedVectorsRead<S>`.
mod read;

use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;

use super::read_view::HNSWIndexReadView;
use super::telemetry::HNSWSearchesTelemetry;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// Read-only, generic-over-storage counterpart of [`HNSWIndex`].
///
/// The graph itself stays a plain [`GraphLayers`] (it materializes into RAM or
/// mmap on load via [`GraphLayers::load`] over a [`UniversalRead`] filesystem),
/// so only the id tracker, vector storage and quantized vectors are
/// parameterized by the backing storage `S`.
///
/// [`HNSWIndex`]: super::super::HNSWIndex
pub struct ReadOnlyHNSWIndex<S: UniversalRead> {
    id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
    payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
    config: HnswGraphConfig,
    path: PathBuf,
    graph: GraphLayers,
    searches_telemetry: HNSWSearchesTelemetry,
    is_on_disk: bool,
}

/// Read-only view over a [`ReadOnlyHNSWIndex`].
///
/// The top-level backends are read-only ([`ReadOnlyIdTrackerEnum`] /
/// [`VectorStorageReadEnum`] / [`ReadOnlyQuantizedVectors`]), while the payload
/// index view is still built over the in-memory enums of [`StructPayloadIndex`].
type ReadView<'a, S> = HNSWIndexReadView<
    'a,
    ReadOnlyIdTrackerEnum<S>,
    VectorStorageReadEnum<S>,
    ReadOnlyQuantizedVectors<S>,
    StructPayloadIndexReadView<
        'a,
        ReadOnlyPayloadStorage<S>,
        ReadOnlyIdTrackerEnum<S>,
        VectorStorageReadEnum<S>,
        ReadOnlyFieldIndex<S>,
    >,
>;

impl<S: UniversalRead> ReadOnlyHNSWIndex<S> {
    pub fn is_on_disk(&self) -> bool {
        self.is_on_disk
    }

    /// Borrow all backing storages and hand a read view to `f`, mirroring
    /// [`HNSWIndex::with_view`].
    ///
    /// [`HNSWIndex::with_view`]: super::super::HNSWIndex::with_view
    pub fn with_view<R>(&self, f: impl FnOnce(ReadView<'_, S>) -> R) -> R {
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let quantized_vectors = self.quantized_vectors.borrow();
        let payload_index = self.payload_index.borrow();

        payload_index.with_view(|payload_index_view| {
            let read_view = HNSWIndexReadView {
                id_tracker: &*id_tracker,
                vector_storage: &*vector_storage,
                quantized_vectors: quantized_vectors.as_ref(),
                payload_index: payload_index_view,
                config: &self.config,
                graph: &self.graph,
                searches_telemetry: &self.searches_telemetry,
            };
            f(read_view)
        })
    }
}
