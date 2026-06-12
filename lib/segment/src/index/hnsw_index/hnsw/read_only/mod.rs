mod read;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;

use super::read_view::HNSWIndexReadView;
use super::telemetry::HNSWSearchesTelemetry;
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::HnswConfig;
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// Read-only, generic-over-storage counterpart of [`HNSWIndex`].
///
/// The graph itself stays a plain [`GraphLayers`] (it materializes into RAM on
/// load via [`GraphLayers::load_universal`] over a [`UniversalRead`] filesystem),
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
    /// Read-only mirror of `HNSWIndex::open`: loads the graph through `fs`.
    pub fn open(
        fs: &S::Fs,
        path: &Path,
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
        quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
        payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
        hnsw_config: HnswConfig,
    ) -> OperationResult<Self> {
        let config_path = HnswGraphConfig::get_config_path(path);
        let config = match HnswGraphConfig::load_universal(fs, &config_path)? {
            Some(config) => config,
            None => {
                let vector_storage = vector_storage.borrow();
                let available_vectors = vector_storage.available_vector_count();
                let full_scan_threshold = vector_storage
                    .size_of_available_vectors_in_bytes()
                    .checked_div(available_vectors)
                    .and_then(|avg_vector_size| {
                        hnsw_config
                            .full_scan_threshold
                            .saturating_mul(BYTES_IN_KB)
                            .checked_div(avg_vector_size)
                    })
                    .unwrap_or(1);

                HnswGraphConfig::new(
                    hnsw_config.m,
                    hnsw_config.ef_construct,
                    full_scan_threshold,
                    hnsw_config.max_indexing_threads,
                    hnsw_config.payload_m,
                    available_vectors,
                )
            }
        };

        let graph = GraphLayers::load_universal(fs, path)?;

        Ok(Self {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            config,
            path: path.to_owned(),
            graph,
            searches_telemetry: HNSWSearchesTelemetry::new(),
        })
    }

    /// Reports the loaded graph's actual residency, not the config intent.
    pub fn is_on_disk(&self) -> bool {
        self.graph.is_on_disk()
    }

    /// Read underlying graph data from disk into the disk cache.
    pub fn populate(&self) -> OperationResult<()> {
        self.graph.populate()
    }

    /// Drop the graph's disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.graph.clear_cache()
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
