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
use super::pipnn_builder::PipnnBuilder;
#[cfg(feature = "gpu")]
use crate::common::operation_error::CancelledError;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
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
    Pipnn(PipnnBuilder<'a>),
    #[cfg(feature = "gpu")]
    Gpu(Box<GpuBuilder<'a>>),
}

/// The steps of a graph build. Implemented by every method.
///
/// A step takes the builder and returns it. A step the GPU can't build returns its state
/// instead, and `GraphBuilder::run` builds it with another method.
pub(super) trait GraphBuilderMethod<'a>: Sized {
    /// Build the main graph once, consuming its progress phases from the context.
    /// Without a main graph (`m == 0`), nothing is linked.
    fn build_main(self) -> Result<Self, StepError<'a>>;

    /// Link one payload block in its own id space and merge it into the graph.
    fn build_block(
        self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> Result<Self, StepError<'a>>;

    /// The finished graph, plus the number of indexed vectors.
    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)>;

    /// Whether payload blocks start from the links of the old graph. The shared code then
    /// keeps the block ranges of the old index, so that the blocks match the old ones.
    fn seeds_payload_blocks(&self) -> bool;
}

/// Why a step returned no builder.
#[cfg(feature = "gpu")]
pub(super) enum StepError<'a> {
    /// Boxed: `LocalBuilder` is large enough to trip `clippy::result_large_err`.
    Fallback(Box<Fallback<'a>>),
    Failed(OperationError),
}

/// Without a GPU, no step falls back.
#[cfg(not(feature = "gpu"))]
pub(super) type StepError<'a> = OperationError;

#[cfg(feature = "gpu")]
impl From<OperationError> for StepError<'_> {
    fn from(err: OperationError) -> Self {
        StepError::Failed(err)
    }
}

#[cfg(feature = "gpu")]
impl From<CancelledError> for StepError<'_> {
    fn from(err: CancelledError) -> Self {
        StepError::Failed(OperationError::from(err))
    }
}

/// A step the GPU couldn't build. Nothing was linked; the state comes back for the next method.
#[cfg(feature = "gpu")]
pub(super) struct Fallback<'a> {
    pub local: LocalBuilder<'a>,
    /// So that later steps still try the GPU.
    pub gpu: GpuState,
    /// Picks the next method, as in `select`.
    pub old_index: Option<&'a OldIndex<'a>>,
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
    ///    old graph, and what fails on the GPU is built by the method of rules 3 to 5, see
    ///    [`Self::run`]: the old graph is healed if the GPU fails.
    /// 3. An old graph worth healing: healing.
    /// 4. PiPNN, if it applies, see [`PipnnBuilder::new`].
    /// 5. Otherwise, HNSW.
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
                local,
                gpu,
                old_index,
            })));
        }
        #[cfg(not(feature = "gpu"))]
        let _ = (gpu_device, has_payload_blocks);

        Ok(Self::without_gpu(local, old_index))
    }

    /// The method `select` picks without a GPU: healing if there is an old graph worth
    /// healing, otherwise PiPNN if it applies, HNSW if not. `local` has the levels of
    /// `old_index`.
    fn without_gpu(local: LocalBuilder<'a>, old_index: Option<&'a OldIndex<'a>>) -> Self {
        match old_index {
            // Healing wins, also over PiPNN, which can't build incrementally
            Some(old_index) => Self::Healing(HealingBuilder::from_local(local, old_index)),
            None => match PipnnBuilder::new(local) {
                Ok(builder) => Self::Pipnn(builder),
                Err(local) => Self::Hnsw(HnswBuilder(*local)),
            },
        }
    }

    /// Run `step`. A step the GPU can't build is built by the method `select` picks without the
    /// GPU, on the state the GPU returns, and the next step tries the GPU again.
    #[cfg(feature = "gpu")]
    pub fn run(self, step: impl Fn(Self) -> Result<Self, StepError<'a>>) -> OperationResult<Self> {
        match step(self) {
            Ok(builder) => Ok(builder),
            Err(StepError::Failed(err)) => Err(err),
            Err(StepError::Fallback(fallback)) => {
                let Fallback {
                    local,
                    gpu,
                    old_index,
                } = *fallback;
                // Only the GPU falls back, so this runs `step` once more
                let local = Self::without_gpu(local, old_index).run(step)?.into_local();
                Ok(Self::Gpu(Box::new(GpuBuilder {
                    local,
                    gpu,
                    old_index,
                })))
            }
        }
    }

    /// Run `step`. Without a GPU, no step falls back.
    #[cfg(not(feature = "gpu"))]
    pub fn run(self, step: impl Fn(Self) -> Result<Self, StepError<'a>>) -> OperationResult<Self> {
        step(self)
    }

    /// Link the blocks of one field worth building, given the graph built so far, each with
    /// [`Self::run`]. Call once per field, in build order.
    pub fn build_field_blocks(mut self, field: FieldBlocks<'_>) -> OperationResult<Self> {
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
                self = self.run(|builder| builder.build_block(&block.points, &counter))?;
            }
        }
        Ok(self)
    }

    /// The state the method builds on.
    fn local(&mut self) -> &mut LocalBuilder<'a> {
        match self {
            GraphBuilder::Hnsw(builder) => &mut builder.0,
            GraphBuilder::Healing(builder) => builder.local(),
            GraphBuilder::Pipnn(builder) => &mut builder.local,
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => &mut builder.local,
        }
    }

    /// The state the method builds on, for the GPU to build on again.
    #[cfg(feature = "gpu")]
    fn into_local(self) -> LocalBuilder<'a> {
        match self {
            GraphBuilder::Hnsw(builder) => builder.0,
            GraphBuilder::Healing(builder) => builder.into_local(),
            GraphBuilder::Pipnn(builder) => builder.local,
            GraphBuilder::Gpu(builder) => builder.local,
        }
    }
}

impl<'a> GraphBuilderMethod<'a> for GraphBuilder<'a> {
    fn build_main(self) -> Result<Self, StepError<'a>> {
        match self {
            GraphBuilder::Hnsw(builder) => builder.build_main().map(GraphBuilder::Hnsw),
            GraphBuilder::Healing(builder) => builder.build_main().map(GraphBuilder::Healing),
            GraphBuilder::Pipnn(builder) => builder.build_main().map(GraphBuilder::Pipnn),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder
                .build_main()
                .map(|builder| GraphBuilder::Gpu(Box::new(builder))),
        }
    }

    fn build_block(
        self,
        points: &[PointOffsetType],
        counter: &AtomicU64,
    ) -> Result<Self, StepError<'a>> {
        match self {
            GraphBuilder::Hnsw(builder) => {
                builder.build_block(points, counter).map(GraphBuilder::Hnsw)
            }
            GraphBuilder::Healing(builder) => builder
                .build_block(points, counter)
                .map(GraphBuilder::Healing),
            GraphBuilder::Pipnn(builder) => builder
                .build_block(points, counter)
                .map(GraphBuilder::Pipnn),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder
                .build_block(points, counter)
                .map(|builder| GraphBuilder::Gpu(Box::new(builder))),
        }
    }

    fn finalize(self) -> OperationResult<(GraphLayersBuilder, usize)> {
        match self {
            GraphBuilder::Hnsw(builder) => builder.finalize(),
            GraphBuilder::Healing(builder) => builder.finalize(),
            GraphBuilder::Pipnn(builder) => builder.finalize(),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.finalize(),
        }
    }

    fn seeds_payload_blocks(&self) -> bool {
        match self {
            GraphBuilder::Hnsw(builder) => builder.seeds_payload_blocks(),
            GraphBuilder::Healing(builder) => builder.seeds_payload_blocks(),
            GraphBuilder::Pipnn(builder) => builder.seeds_payload_blocks(),
            #[cfg(feature = "gpu")]
            GraphBuilder::Gpu(builder) => builder.seeds_payload_blocks(),
        }
    }
}
