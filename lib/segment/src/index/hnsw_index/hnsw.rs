use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::{MmapFs, Populate, UniversalReadFs};

use self::telemetry::HNSWSearchesTelemetry;
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerEnum;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph::{HnswGraph, HnswLinksStorage};
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_links::GraphLinksResidency;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::{HnswConfig, Memory};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

mod build;
#[cfg(feature = "gpu")]
mod gpu_build;
mod old_index;
pub mod read_only;
mod read_view;
mod telemetry;
mod vector_index_impl;

use self::read_view::{HNSWIndexReadView, HNSWIndexReadViewEnum};

const HNSW_USE_HEURISTIC: bool = true;
const FINISH_MAIN_GRAPH_LOG_MESSAGE: &str = "Finish main graph in time";

/// Build first N points in HNSW graph using only a single thread, to avoid
/// disconnected components in the graph.
#[cfg(debug_assertions)]
pub const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 32;
#[cfg(not(debug_assertions))]
pub const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

const LINK_COMPRESSION_CONVERT_EXISTING: bool = false;

#[derive(Debug)]
pub struct HNSWIndex {
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    config: HnswGraphConfig,
    path: PathBuf,
    graph: HnswGraph<HnswLinksStorage>,
    searches_telemetry: HNSWSearchesTelemetry,
    is_on_disk: bool,
}

pub struct HnswIndexOpenArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub hnsw_config: HnswConfig,
}

impl HNSWIndex {
    pub fn open(args: HnswIndexOpenArgs<'_>) -> OperationResult<Self> {
        let HnswIndexOpenArgs {
            path,
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            hnsw_config,
        } = args;

        let config = load_or_derive_config(&MmapFs, path, &hnsw_config, &vector_storage)?;

        let do_convert = LINK_COMPRESSION_CONVERT_EXISTING;

        let (memory, residency) = graph_residency(&hnsw_config, None);
        let is_on_disk = memory.is_on_disk();

        let graph = HnswGraph::Direct(GraphLayers::load(path, residency, do_convert)?);

        Ok(HNSWIndex {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            config,
            path: path.to_owned(),
            graph,
            searches_telemetry: HNSWSearchesTelemetry::new(),
            is_on_disk,
        })
    }

    pub fn is_on_disk(&self) -> bool {
        self.is_on_disk
    }

    /// Heap RAM held by the graph links, in bytes.
    ///
    /// Non-zero when the links are materialized in RAM rather than backed by
    /// a live mmap handle (freshly built index, or a non-borrowable universal-IO
    /// backend); such links are invisible to page-cache residency probes.
    pub fn links_heap_size_bytes(&self) -> usize {
        self.graph.links_heap_size_bytes()
    }

    #[cfg(test)]
    pub(super) fn graph(&self) -> &GraphLayers {
        self.graph.graph_layers()
    }

    pub fn get_quantized_vectors(&self) -> Arc<AtomicRefCell<Option<QuantizedVectors>>> {
        self.quantized_vectors.clone()
    }

    /// Read underlying data from disk into disk cache.
    pub fn populate(&self) -> OperationResult<()> {
        self.graph.populate()
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            id_tracker: _,
            vector_storage: _,
            quantized_vectors: _,
            payload_index: _,
            config: _,
            path: _,
            graph,
            searches_telemetry: _,
            is_on_disk: _,
        } = self;
        graph.clear_cache()?;
        Ok(())
    }

    pub fn with_view<R>(&self, f: impl FnOnce(HNSWIndexReadViewEnum<'_>) -> R) -> R {
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

fn load_or_derive_config(
    fs: &impl UniversalReadFs,
    path: &Path,
    hnsw_config: &HnswConfig,
    vector_storage: &AtomicRefCell<impl VectorStorageRead>,
) -> OperationResult<HnswGraphConfig> {
    let config_path = HnswGraphConfig::get_config_path(path);
    if let Some(config) = HnswGraphConfig::load_universal(fs, &config_path)? {
        return Ok(config);
    }

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

    Ok(HnswGraphConfig::new(
        hnsw_config.m,
        hnsw_config.ef_construct,
        full_scan_threshold,
        hnsw_config.max_indexing_threads,
        hnsw_config.payload_m,
        available_vectors,
    ))
}

/// Effective placement of the graph links and their residency: the `memory`
/// parameter (falling back to the deprecated `on_disk` flag), degraded at load
/// time by the node-wide low-memory mode.
///
/// A `populate_override` (from a request-specific
/// [`LoadProfile`](crate::data_types::load_profile::LoadProfile)) demotes the
/// residency (see [`Memory::with_populate_override`]) — the graph links support
/// every residency over the same files, so even a `pinned` graph can be demoted
/// to a lazy cold view. The returned [`Memory`] stays config-derived: it
/// describes the configuration, not the per-open placement.
fn graph_residency(
    hnsw_config: &HnswConfig,
    populate_override: Option<Populate>,
) -> (Memory, GraphLinksResidency) {
    let memory = hnsw_config.memory_placement().clamp_to_low_memory();

    let residency = match memory.with_populate_override(populate_override) {
        // Keep the links cold: lazily loaded from disk, cached with usage
        Memory::Cold => GraphLinksResidency::Cold,
        // Pre-populate the links into the page cache on load
        Memory::Cached => GraphLinksResidency::Cached,
        // Materialize the links on heap, so they are never evicted by cache pressure
        Memory::Pinned => GraphLinksResidency::Pinned,
    };

    (memory, residency)
}
