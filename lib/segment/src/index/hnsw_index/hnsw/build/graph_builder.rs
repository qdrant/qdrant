use std::sync::atomic::AtomicU64;

use common::types::PointOffsetType;
use rand::Rng;
use rayon::ThreadPool;

use super::context::GraphBuildContext;
#[cfg(feature = "gpu")]
use super::gpu_builder::{GpuBuilder, GpuState};
use super::healing_builder::HealingBuilder;
use super::hnsw_builder::HnswBuilder;
use super::local_builder::LocalBuilder;
use super::payload_blocks::FieldBlocks;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
#[cfg(feature = "gpu")]
use crate::index::hnsw_index::hnsw::SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
#[cfg(feature = "gpu")]
use crate::vector_storage::VectorStorageRead as _;

/// How the graphs of a vector index get built.
pub(super) enum GraphBuilder<'a> {
    Hnsw(HnswBuilder<'a>),
    Healing(HealingBuilder<'a>),
    #[cfg(feature = "gpu")]
    Gpu(Box<GpuBuilder<'a>>),
}

/// The steps of a graph build. Implemented by every method.
pub(super) trait GraphBuilderMethod {
    /// Build the main graph once, consuming its progress phases from the context.
    /// Without a main graph (`m == 0`), nothing is linked.
    fn build_main(&mut self) -> OperationResult<()>;

    /// Link one payload block in its own id space and merge it into the graph.
    fn build_block(
        &mut self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()>;

    /// The finished graph, plus the number of indexed vectors.
    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)>;

    /// Whether payload blocks start from the links of the old graph. The shared code then
    /// keeps the block ranges of the old index, so that the blocks match the old ones.
    fn seeds_payload_blocks(&self) -> bool;
}

/// What `select` decides with.
pub(super) struct GraphBuildOptions<'a> {
    /// The graph of an older segment that `OldIndexCandidate::evaluate` found worth healing.
    pub old_index: Option<&'a OldIndex<'a>>,
    pub gpu_device: Option<&'a LockedGpuDevice<'a>>,
    /// Whether any payload block graphs will be built. Without a main graph and blocks,
    /// nothing is uploaded anywhere.
    pub has_payload_blocks: bool,
}

impl<'a> GraphBuilder<'a> {
    /// Select the method to use.
    ///
    /// The methods are tried in this order:
    /// 1. Small segments, or nothing to build: HNSW, or healing if there is an old graph.
    ///    Nothing else would pay off.
    /// 2. A GPU: GPU, even if the old graph could be healed. Points keep their levels from the
    ///    old graph, and what fails on the GPU is built by the method of rules 3 and 4, which
    ///    the GPU method keeps for the whole build: the old graph is healed if the GPU fails.
    /// 3. An old graph worth healing: healing.
    /// 4. Otherwise, HNSW.
    pub fn select(
        ctx: GraphBuildContext<'a>,
        pool: &'a ThreadPool,
        rng: &'a mut dyn Rng,
        options: GraphBuildOptions<'a>,
    ) -> OperationResult<Self> {
        let GraphBuildOptions {
            old_index,
            gpu_device,
            has_payload_blocks,
        } = options;

        let local = LocalBuilder::new(ctx, pool, rng, old_index)?;

        #[cfg(feature = "gpu")]
        if let Some(gpu_device) = gpu_device
            && (local.ctx.has_main_graph() || has_payload_blocks)
            && local.ctx.vector_storage.total_vector_count() >= SINGLE_THREADED_HNSW_BUILD_THRESHOLD
            && let Some(gpu) = GpuState::new(&local.ctx, gpu_device)?
        {
            return Ok(Self::Gpu(Box::new(GpuBuilder {
                cpu: Self::without_gpu(local, old_index),
                gpu,
            })));
        }
        #[cfg(not(feature = "gpu"))]
        let _ = (gpu_device, has_payload_blocks);

        Ok(Self::without_gpu(local, old_index))
    }

    /// The method `select` picks without a GPU: healing if there is an old graph worth
    /// healing, HNSW otherwise. `local` has the levels of `old_index`.
    fn without_gpu(local: LocalBuilder<'a>, old_index: Option<&'a OldIndex<'a>>) -> Self {
        match old_index {
            Some(old_index) => Self::Healing(HealingBuilder::from_local(local, old_index)),
            None => Self::Hnsw(HnswBuilder(local)),
        }
    }

    /// Link the blocks of one field worth building, given the graph built so far. Call once per
    /// field, in build order.
    pub fn build_field_blocks(&mut self, field: FieldBlocks<'_>) -> OperationResult<()> {
        let FieldBlocks {
            progress,
            skip,
            blocks,
            loader,
        } = field;
        let counter = progress.track_progress(None);
        for block in blocks {
            check_process_stopped(self.local().ctx.stopped)?;
            // Read the points of one block at a time, and only of blocks small enough to build
            if !self.local().fits(block.cardinality) {
                continue;
            }
            let Some(block) = loader.load(block)? else {
                continue;
            };
            if self.local().should_build(&block, skip) {
                self.build_block(&block.points, &counter)?;
            }
        }
        Ok(())
    }

    /// The state the method builds on.
    pub(super) fn local(&mut self) -> &mut LocalBuilder<'a> {
        match self {
            GraphBuilder::Hnsw(builder) => &mut builder.0,
            GraphBuilder::Healing(builder) => builder.local(),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.cpu.local(),
        }
    }
}

impl GraphBuilderMethod for GraphBuilder<'_> {
    fn build_main(&mut self) -> OperationResult<()> {
        match self {
            GraphBuilder::Hnsw(builder) => builder.build_main(),
            GraphBuilder::Healing(builder) => builder.build_main(),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.build_main(),
        }
    }

    fn build_block(
        &mut self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        match self {
            GraphBuilder::Hnsw(builder) => builder.build_block(points, counter),
            GraphBuilder::Healing(builder) => builder.build_block(points, counter),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.build_block(points, counter),
        }
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        match self {
            GraphBuilder::Hnsw(builder) => builder.finalize(),
            GraphBuilder::Healing(builder) => builder.finalize(),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.finalize(),
        }
    }

    fn seeds_payload_blocks(&self) -> bool {
        match self {
            GraphBuilder::Hnsw(builder) => builder.seeds_payload_blocks(),
            GraphBuilder::Healing(builder) => builder.seeds_payload_blocks(),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.seeds_payload_blocks(),
        }
    }
}
