use rand::Rng;
use rayon::ThreadPool;

use super::context::GraphBuildContext;
#[cfg(feature = "gpu")]
use super::gpu_builder::{GpuBuilder, GpuState};
use super::healing_builder::HealingBuilder;
use super::hnsw_builder::HnswBuilder;
use super::local_builder::LocalBuilder;
use super::payload_blocks::FieldBlocks;
use crate::common::operation_error::OperationResult;
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
    /// Build the main graph.
    fn build_main(&mut self) -> OperationResult<()>;

    /// Link the blocks of one field. Call once per field, in build order.
    fn build_field_blocks(&mut self, field: FieldBlocks) -> OperationResult<()>;

    /// The finished graph, plus the number of indexed vectors.
    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)>;
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
    /// 2. A GPU: GPU, even if the old graph could be healed. Points still keep their levels
    ///    from the old graph.
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

        #[cfg(feature = "gpu")]
        if let Some(gpu_device) = gpu_device
            && (ctx.main_params.hnsw_m.m > 0 || has_payload_blocks)
            && ctx.vector_storage.total_vector_count() >= SINGLE_THREADED_HNSW_BUILD_THRESHOLD
            && let Some(gpu) = GpuState::new(ctx, gpu_device)?
        {
            let hnsw = HnswBuilder(LocalBuilder::new(ctx, pool, rng, old_index)?);
            return Ok(Self::Gpu(Box::new(GpuBuilder(hnsw, gpu))));
        }
        #[cfg(not(feature = "gpu"))]
        let _ = (gpu_device, has_payload_blocks);

        Ok(match old_index {
            Some(old_index) => Self::Healing(HealingBuilder::new(ctx, pool, rng, old_index)?),
            None => Self::Hnsw(HnswBuilder(LocalBuilder::new(ctx, pool, rng, None)?)),
        })
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

    fn build_field_blocks(&mut self, field: FieldBlocks) -> OperationResult<()> {
        match self {
            GraphBuilder::Hnsw(builder) => builder.build_field_blocks(field),
            GraphBuilder::Healing(builder) => builder.build_field_blocks(field),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.build_field_blocks(field),
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
}
