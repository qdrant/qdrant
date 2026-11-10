//! Indexer side of a remote build: the same graph builders as a local build, run on what the
//! indexer read from core.

use std::sync::atomic::AtomicBool;

use common::progress_tracker::ProgressTracker;
use rand::Rng;

use super::block_filter::BlockSkip;
use super::build_thread_pool;
use super::context::{GraphBuildContext, GraphParams, MainGraphProgress};
use super::graph_builder::{GraphBuildOptions, GraphBuilder, GraphBuilderMethod};
use super::payload_blocks::PayloadBlock;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerEnum;
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::remote::wire::{BlockPlan, RemoteGraphParams};
use crate::vector_storage::VectorStorageEnum;

/// Build the graph `params` and `plan` ask for, over `vector_storage`, whose deleted vectors
/// are the points not to link. `id_tracker` maps every offset of the storage.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_requested_graph(
    params: &RemoteGraphParams,
    plan: &BlockPlan,
    vector_storage: &VectorStorageEnum,
    id_tracker: &IdTrackerEnum,
    num_threads: usize,
    gpu_device: Option<&LockedGpuDevice<'_>>,
    rng: &mut dyn Rng,
    stopped: &AtomicBool,
    progress: &ProgressTracker,
) -> OperationResult<GraphLayersBuilder> {
    let mut ctx = GraphBuildContext {
        id_tracker,
        vector_storage,
        quantized_vectors: None,
        progress: None,
        main_params: GraphParams {
            hnsw_m: params.main_m(),
            ef_construct: params.ef_construct,
            entry_points_num: params.entry_points_num,
        },
        block_params: GraphParams {
            hnsw_m: params.block_m(),
            ef_construct: params.ef_construct,
            entry_points_num: 1,
        },
        stopped,
    };
    ctx.progress = ctx.has_main_graph().then(|| MainGraphProgress {
        migrate: progress.subtask("migrate"),
        main_graph: progress.subtask("main_graph"),
    });
    let has_payload_blocks = plan.num_blocks() > 0;
    let field_progress: Vec<_> = plan
        .fields
        .iter()
        .map(|field| progress.subtask(format!("additional_links:{}", field.field)))
        .collect();

    let pool = build_thread_pool(num_threads.max(1))?;
    let mut builder = GraphBuilder::select(
        ctx,
        &pool,
        rng,
        GraphBuildOptions {
            old_index: None,
            gpu_device,
            has_payload_blocks,
            // Never forward the task again
            remote: None,
        },
    )?;

    builder.build_main()?;

    for (field, field_progress) in plan.fields.iter().zip(field_progress) {
        field_progress.start();
        let skip = if field.skip_if_connected {
            BlockSkip::IfConnected
        } else {
            BlockSkip::Never
        };
        builder.build_loaded_field_blocks(
            &field_progress,
            skip,
            field.blocks.iter().map(|block| PayloadBlock {
                cardinality: block.cardinality,
                points: block.points.clone(),
            }),
        )?;
    }

    let (graph, _indexed) = builder.finalize()?;
    Ok(graph)
}
