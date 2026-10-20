use std::sync::atomic::AtomicU64;

use common::types::PointOffsetType;
use rand::Rng;
use rayon::ThreadPool;

use super::context::GraphBuildContext;
#[cfg(feature = "gpu")]
use super::gpu_builder::{GpuBuilder, GpuState};
use super::healing_builder::HealingBuilder;
use super::hnsw_builder::HnswBuilder;
use super::local_builder::{LocalBuilder, LocalMethod};
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
    fn build_field_blocks(&mut self, field: FieldBlocks<'_>) -> OperationResult<()>;

    /// The finished graph, plus the number of indexed vectors.
    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)>;

    /// Whether payload blocks start from the links of the old graph. The shared code then
    /// keeps the block ranges of the old index, so that the blocks match the old ones.
    fn seeds_payload_blocks(&self) -> bool;
}

/// The methods that build on the CPU only: what `select` picks without a GPU, and what builds
/// the graphs the GPU fails to build.
pub(super) enum CpuBuilder<'a> {
    Hnsw(HnswBuilder<'a>),
    Healing(HealingBuilder<'a>),
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
    /// 2. A GPU: GPU, even if the old graph could be healed. What fails on the GPU is built
    ///    by the method of rules 3 and 4, so points keep their levels from the old graph, and
    ///    the old graph is healed if the GPU fails.
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

        let cpu = CpuBuilder::new(ctx, pool, rng, old_index)?;

        #[cfg(feature = "gpu")]
        if let Some(gpu_device) = gpu_device
            && (ctx.has_main_graph() || has_payload_blocks)
            && ctx.vector_storage.total_vector_count() >= SINGLE_THREADED_HNSW_BUILD_THRESHOLD
            && let Some(gpu) = GpuState::new(ctx, gpu_device)?
        {
            return Ok(Self::Gpu(Box::new(GpuBuilder(cpu, gpu))));
        }
        #[cfg(not(feature = "gpu"))]
        let _ = (gpu_device, has_payload_blocks);

        Ok(Self::from(cpu))
    }
}

impl<'a> From<CpuBuilder<'a>> for GraphBuilder<'a> {
    fn from(cpu: CpuBuilder<'a>) -> Self {
        match cpu {
            CpuBuilder::Hnsw(builder) => GraphBuilder::Hnsw(builder),
            CpuBuilder::Healing(builder) => GraphBuilder::Healing(builder),
        }
    }
}

impl<'a> CpuBuilder<'a> {
    /// Healing if there is an old graph worth healing, HNSW otherwise.
    fn new(
        ctx: GraphBuildContext<'a>,
        pool: &'a ThreadPool,
        rng: &'a mut dyn Rng,
        old_index: Option<&'a OldIndex<'a>>,
    ) -> OperationResult<Self> {
        Ok(match old_index {
            Some(old_index) => Self::Healing(HealingBuilder::new(ctx, pool, rng, old_index)?),
            None => Self::Hnsw(HnswBuilder(LocalBuilder::new(ctx, pool, rng, None)?)),
        })
    }
}

impl GraphBuilderMethod for CpuBuilder<'_> {
    fn build_main(&mut self) -> OperationResult<()> {
        match self {
            CpuBuilder::Hnsw(builder) => builder.build_main(),
            CpuBuilder::Healing(builder) => builder.build_main(),
        }
    }

    fn build_field_blocks(&mut self, field: FieldBlocks<'_>) -> OperationResult<()> {
        match self {
            CpuBuilder::Hnsw(builder) => builder.build_field_blocks(field),
            CpuBuilder::Healing(builder) => builder.build_field_blocks(field),
        }
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        match self {
            CpuBuilder::Hnsw(builder) => builder.finalize(),
            CpuBuilder::Healing(builder) => builder.finalize(),
        }
    }

    fn seeds_payload_blocks(&self) -> bool {
        match self {
            CpuBuilder::Hnsw(builder) => builder.seeds_payload_blocks(),
            CpuBuilder::Healing(builder) => builder.seeds_payload_blocks(),
        }
    }
}

impl<'a> LocalMethod<'a> for CpuBuilder<'a> {
    fn local(&mut self) -> &mut LocalBuilder<'a> {
        match self {
            CpuBuilder::Hnsw(builder) => builder.local(),
            CpuBuilder::Healing(builder) => builder.local(),
        }
    }

    fn build_block(
        &mut self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        match self {
            CpuBuilder::Hnsw(builder) => builder.build_block(points, counter),
            CpuBuilder::Healing(builder) => builder.build_block(points, counter),
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

    fn build_field_blocks(&mut self, field: FieldBlocks<'_>) -> OperationResult<()> {
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

    fn seeds_payload_blocks(&self) -> bool {
        match self {
            GraphBuilder::Hnsw(builder) => builder.seeds_payload_blocks(),
            GraphBuilder::Healing(builder) => builder.seeds_payload_blocks(),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.seeds_payload_blocks(),
        }
    }
}
