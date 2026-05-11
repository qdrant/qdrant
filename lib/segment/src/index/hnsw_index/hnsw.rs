use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use self::telemetry::HNSWSearchesTelemetry;
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerEnum;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::HnswConfig;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

mod build;
#[cfg(feature = "gpu")]
mod gpu_build;
mod old_index;
mod search;
mod telemetry;
mod vector_index_impl;

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
    graph: GraphLayers,
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

        let config_path = HnswGraphConfig::get_config_path(path);
        let config = if config_path.exists() {
            HnswGraphConfig::load(&config_path)?
        } else {
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
        };

        let do_convert = LINK_COMPRESSION_CONVERT_EXISTING;

        let is_on_disk = hnsw_config.on_disk.unwrap_or(false);

        let graph = GraphLayers::load(path, is_on_disk, do_convert)?;

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

    #[cfg(test)]
    pub(super) fn graph(&self) -> &GraphLayers {
        &self.graph
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
}
