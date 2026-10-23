use std::sync::atomic::AtomicU64;
use std::time::Instant;

use common::types::PointOffsetType;
use log::debug;
use rand::RngExt as _;

use super::block::Block;
use super::context::MainGraphProgress;
use super::graph_builder::{GraphBuilderMethod, StepError};
use super::local_builder::LocalBuilder;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::hnsw::FINISH_MAIN_GRAPH_LOG_MESSAGE;
use crate::index::hnsw_index::pipnn::{self, PipnnParams};
use crate::vector_storage::matrix_scorer::{MatrixScorer, new_matrix_scorer};

/// Fewest points for a PiPNN main graph: the smallest size where PiPNN was measured to build
/// faster than HNSW. At 5,000 points it was slower, and level 0 graphs of about 1,025 to
/// 4,000 points come out much worse than HNSW's. Lower it once the crossover is measured.
const PIPNN_MIN_POINTS: usize = 100_000;

#[cfg(feature = "testing")]
thread_local! {
    /// Set by [`with_pipnn_min_points`]. Thread-local, so tests running in parallel don't see
    /// each other's: the selection runs on the thread that calls the build.
    static MIN_POINTS_OVERRIDE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Fewest points for a PiPNN main graph on this thread.
fn min_points() -> usize {
    #[cfg(feature = "testing")]
    if let Some(min_points) = MIN_POINTS_OVERRIDE.get() {
        return min_points;
    }
    PIPNN_MIN_POINTS
}

/// Run `f` with PiPNN's minimum segment size lowered to `min_points` on this thread, so that
/// tests can build small segments with PiPNN. `usize::MAX` keeps every build on HNSW.
#[cfg(feature = "testing")]
pub fn with_pipnn_min_points<R>(min_points: usize, f: impl FnOnce() -> R) -> R {
    /// Restores the previous value, also when `f` panics.
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            MIN_POINTS_OVERRIDE.set(self.0);
        }
    }
    let _restore = Restore(MIN_POINTS_OVERRIDE.replace(Some(min_points)));
    f()
}

/// Builds every level of the main graph with PiPNN: from overlapping leaves scored with matrix
/// products, without searching the graph. Payload blocks are built with HNSW.
pub(super) struct PipnnBuilder<'a> {
    pub local: LocalBuilder<'a>,
    /// Scores what HNSW scores: the quantized vectors where configured.
    scorer: MatrixScorer<'a>,
}

impl<'a> PipnnBuilder<'a> {
    /// PiPNN for the main graph of `local`, if it applies: the main graph is still to be built,
    /// it has at least `PIPNN_MIN_POINTS` points, and their vectors have a matrix scorer.
    /// Otherwise `local` comes back.
    ///
    /// Boxed: `LocalBuilder` is large enough to trip `clippy::result_large_err`.
    pub fn new(local: LocalBuilder<'a>) -> Result<Self, Box<LocalBuilder<'a>>> {
        // No main graph (`m == 0`), or it is built already, and a GPU fell back for a block:
        // blocks are HNSW either way
        if local.ctx.progress.is_none() {
            return Err(Box::new(local));
        }
        if local.points.len() < min_points() {
            // Most segments, so not logged
            return Err(Box::new(local));
        }
        let Some(scorer) = new_matrix_scorer(local.ctx.vector_storage, local.ctx.quantized_vectors)
        else {
            debug!("No PiPNN: no matrix scorer for these vectors");
            return Err(Box::new(local));
        };
        Ok(Self { local, scorer })
    }
}

impl<'a> GraphBuilderMethod<'a> for PipnnBuilder<'a> {
    fn build_main(mut self) -> Result<Self, StepError<'a>> {
        // `new` only picks PiPNN for a main graph still to be built
        let progress = self
            .local
            .ctx
            .progress
            .take()
            .expect("main graph built once");
        let MainGraphProgress {
            migrate,
            main_graph,
        } = progress;
        // Nothing to heal
        drop(migrate);

        // The first draw after the levels, which `LocalBuilder::new` drew from the same rng
        let params = PipnnParams::new(self.local.rng.random());

        let timer = Instant::now();
        // Parts of the builder, so that the build does not borrow the rng, which is not `Sync`
        let local = &self.local;
        let (ctx, points, graph) = (&local.ctx, &local.points, &local.graph);
        let links = local.pool.install(|| {
            pipnn::build_links(
                ctx.vector_storage,
                ctx.quantized_vectors,
                ctx.id_tracker.deleted_point_bitslice(),
                &self.scorer,
                points,
                |point| graph.get_point_level(point),
                ctx.main_params.hnsw_m,
                &params,
                ctx.stopped,
                &main_graph,
            )
        })?;
        let links = links.ok_or_else(|| {
            OperationError::service_error("PiPNN can't sketch the vectors it was selected for")
        })?;
        check_process_stopped(ctx.stopped)?;
        // In point order: the entry points keep the first point they see of each top level, so
        // only a fixed order makes the graph independent of the threads
        for (&point, point_links) in std::iter::zip(points, links) {
            graph.add_new_point(point, point_links);
        }
        drop(main_graph);
        debug!("{FINISH_MAIN_GRAPH_LOG_MESSAGE} {:?}", timer.elapsed());
        Ok(self)
    }

    fn build_block(
        mut self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> Result<Self, StepError<'a>> {
        let block = Block::new(&self.local.ctx, points);
        let local_ids = (0..points.len() as PointOffsetType).collect::<Vec<_>>();
        block.insert(&local_ids, self.local.pool, counter)?;
        block.merge_into(&mut self.local.graph);
        Ok(self)
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        Ok(self.local.finalize())
    }

    fn seeds_payload_blocks(&self) -> bool {
        // Never picked with an old index
        false
    }
}
