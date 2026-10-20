mod seeds;

use std::ops::Deref as _;
use std::sync::atomic::AtomicU64;

use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use log::debug;
use rand::Rng;
use rayon::ThreadPool;

use self::seeds::BlockSeeds;
use super::block::Block;
use super::context::GraphBuildContext;
use super::graph_builder::GraphBuilderMethod;
use super::hnsw_builder::HnswBuilder;
use super::local_builder::{LocalBuilder, LocalMethod};
use super::payload_blocks::FieldBlocks;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_layers_healer::GraphLayersHealer;
use crate::index::hnsw_index::hnsw::old_index::{OldIndex, payload_links_reusable};
use crate::vector_storage::VectorStorageRead as _;

/// Progress phase of healing the old graph.
const MIGRATE_PHASE: &str = "migrate";

/// Heals the graph of an older segment, then inserts the points it lacks with HNSW.
///
/// Payload blocks start from the links their points had to each other in the old graph, if it
/// has payload links of the same size; see [`BlockSeeds`]. The rest is built with HNSW.
pub(super) struct HealingBuilder<'a> {
    hnsw: HnswBuilder<'a>,
    old: &'a OldIndex<'a>,
    /// Progress of healing the old graph, created with the builder so that it is listed before
    /// the main graph. `None` without a main graph, or once taken by `build_main`.
    migrate_progress: Option<ProgressTracker>,
    /// `None` if the payload links of the old graph cannot be reused.
    seeds: Option<BlockSeeds<'a>>,
    /// Points of the blocks built, for the log.
    block_points: usize,
    /// Of them, the points that kept their links from the old graph.
    seeded_points: usize,
}

impl<'a> HealingBuilder<'a> {
    /// Points keep their level from the old graph.
    pub fn new(
        ctx: GraphBuildContext<'a>,
        pool: &'a ThreadPool,
        rng: &'a mut dyn Rng,
        old: &'a OldIndex<'a>,
    ) -> OperationResult<Self> {
        let migrate_progress = ctx
            .has_main_graph()
            .then(|| ctx.progress.subtask(MIGRATE_PHASE));
        let hnsw = HnswBuilder(LocalBuilder::new(ctx, pool, rng, Some(old))?);

        let payload_m = ctx.block_params.hnsw_m;
        let seeds = payload_links_reusable(payload_m, &old.index.config)
            .then(|| BlockSeeds::new(old, payload_m, ctx.vector_storage.total_vector_count()));

        Ok(Self {
            hnsw,
            old,
            migrate_progress,
            seeds,
            block_points: 0,
            seeded_points: 0,
        })
    }
}

impl GraphBuilderMethod for HealingBuilder<'_> {
    fn build_main(&mut self) -> OperationResult<()> {
        if !self.hnsw.0.ctx.has_main_graph() {
            return Ok(());
        }
        let migrate_progress = self
            .migrate_progress
            .take()
            .unwrap_or_else(|| self.hnsw.0.ctx.progress.subtask(MIGRATE_PHASE));
        let main_graph_progress = self.hnsw.0.take_main_graph_progress();
        let local = &mut self.hnsw.0;
        let LocalBuilder {
            ctx,
            pool,
            graph,
            points,
            ..
        } = local;
        let old = self.old;

        migrate_progress.start();

        let timer = std::time::Instant::now();

        old.populate_links();

        let mut healer =
            GraphLayersHealer::new(old.graph(), &old.old_to_new, ctx.main_params.ef_construct);
        let old_vector_storage = old.index.vector_storage.borrow();
        let old_quantized_vectors = old.index.quantized_vectors.borrow();

        let counter = migrate_progress.track_progress(Some(healer.to_heal_count() as u64));
        healer.heal(
            pool,
            &old_vector_storage,
            old_quantized_vectors.as_ref(),
            ctx.stopped,
            counter.deref(),
        )?;
        check_process_stopped(ctx.stopped)?;
        healer.save_into_builder(graph);

        let missing_points = points
            .iter()
            .copied()
            .filter(|&point_id| old.new_to_old[point_id as usize].is_none())
            .collect::<Vec<_>>();

        debug!("Migrated in {:?}", timer.elapsed());
        drop(migrate_progress);

        self.hnsw.insert(&missing_points, main_graph_progress)
    }

    fn build_field_blocks(&mut self, field: FieldBlocks<'_>) -> OperationResult<()> {
        self.build_blocks(field)
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        if self.seeds.is_some() {
            debug!(
                "seeded {} of {} payload block points from the old graph",
                self.seeded_points, self.block_points,
            );
        }
        self.hnsw.finalize()
    }

    fn seeds_payload_blocks(&self) -> bool {
        self.seeds.is_some()
    }
}

impl<'a> LocalMethod<'a> for HealingBuilder<'a> {
    fn local(&mut self) -> &mut LocalBuilder<'a> {
        self.hnsw.local()
    }

    /// Points that kept enough links to the block in the old graph start with them, the rest
    /// are inserted by search.
    fn build_block(
        &mut self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        let Some(seeds) = &mut self.seeds else {
            return self.hnsw.build_block(points, counter);
        };
        self.old.populate_links();
        let local = &mut self.hnsw.0;
        let block = Block::new(local.ctx, points);
        let to_insert = seeds.seed_block(&block, &local.graph, local.pool, counter)?;
        self.block_points += points.len();
        self.seeded_points += points.len() - to_insert.len();
        block.insert(&to_insert, local.pool, counter)?;
        block.merge_into(&mut local.graph);
        Ok(())
    }
}
