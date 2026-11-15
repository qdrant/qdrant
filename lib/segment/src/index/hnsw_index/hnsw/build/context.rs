use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::progress_tracker::ProgressTracker;

use crate::id_tracker::{IdTrackerEnum, IdTrackerRead as _};
use crate::index::hnsw_index::HnswM;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{NotDeletedChecker, VectorStorageEnum};

/// Inputs to a graph build and ownership of its main-graph progress phases.
pub(super) struct GraphBuildContext<'a> {
    pub id_tracker: &'a IdTrackerEnum,
    /// Vectors to score. The build reads its deleted flags from `vector_deleted`, not from
    /// here, so a storage without vectors can stand in when scoring is quantized only.
    pub vector_storage: &'a VectorStorageEnum,
    /// One flag per vector not to link: the storage's deleted flags on core, the flags core
    /// sent on a remote indexer. Missing flags count as not deleted, as in a storage.
    pub vector_deleted: &'a BitSlice,
    /// Main-graph phases, created before building starts. `take()` transfers ownership to
    /// the CPU method, which explicitly drops each tracker when its phase ends. GPU success
    /// takes and drops them unstarted; GPU failure leaves them here for the CPU fallback.
    /// `None` means there is no main graph, or ownership has already been taken. It does not
    /// decide whether to build: use `has_main_graph()` for that. Taking these phases twice
    /// for a main graph is a caller error. Dropping the context on error also finishes any
    /// phases still owned here, so they cannot outlive the build.
    pub progress: Option<MainGraphProgress>,
    pub quantized_vectors: Option<&'a QuantizedVectors>,
    /// Main graph: `m`, `m0` and N entry points.
    pub main_params: GraphParams,
    /// Payload block graphs: `payload_m` and 1 entry point.
    pub block_params: GraphParams,
    pub stopped: &'a AtomicBool,
}

impl<'a> GraphBuildContext<'a> {
    /// Whether the main graph gets built. Without it (`m == 0`), only payload blocks link points.
    pub fn has_main_graph(&self) -> bool {
        self.main_params.hnsw_m.m > 0
    }

    /// Which points the build may link and score: neither the point nor its vector deleted.
    pub fn not_deleted(&self) -> NotDeletedChecker<'a> {
        NotDeletedChecker {
            point_deleted: self.id_tracker.deleted_point_bitslice(),
            vec_deleted: self.vector_deleted,
        }
    }
}

/// Parameters of one kind of graph.
#[derive(Clone, Copy)]
pub(super) struct GraphParams {
    pub hnsw_m: HnswM,
    pub ef_construct: usize,
    /// Number of entry points the graph keeps.
    pub entry_points_num: usize,
}

/// Main-graph phases owned by the context until a build method takes them.
/// Dropping a tracker marks its phase finished, even if it was never started.
pub(super) struct MainGraphProgress {
    /// Healing the old graph. Explicitly dropped before inserting missing points;
    /// dropped unstarted when no healing is needed.
    pub migrate: ProgressTracker,
    /// CPU insertion. Explicitly dropped after insertion, or unstarted on GPU success.
    pub main_graph: ProgressTracker,
}
