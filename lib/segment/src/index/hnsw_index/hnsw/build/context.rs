use std::sync::atomic::AtomicBool;

use common::progress_tracker::ProgressTracker;

use crate::id_tracker::IdTrackerEnum;
use crate::index::hnsw_index::HnswM;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// What a graph build reads. Borrowed, so `Copy`.
#[derive(Clone, Copy)]
pub(super) struct GraphBuildContext<'a> {
    pub id_tracker: &'a IdTrackerEnum,
    pub vector_storage: &'a VectorStorageEnum,
    pub quantized_vectors: Option<&'a QuantizedVectors>,
    /// Progress of building this vector index. Builders add the phases of their work to it.
    pub progress: &'a ProgressTracker,
    /// Main graph: `m`, `m0` and N entry points.
    pub main_params: GraphParams,
    /// Payload block graphs: `payload_m` and 1 entry point.
    pub block_params: GraphParams,
    pub stopped: &'a AtomicBool,
}

/// Parameters of one kind of graph.
#[derive(Clone, Copy)]
pub(super) struct GraphParams {
    pub hnsw_m: HnswM,
    pub ef_construct: usize,
    /// Number of entry points the graph keeps.
    pub entry_points_num: usize,
}
