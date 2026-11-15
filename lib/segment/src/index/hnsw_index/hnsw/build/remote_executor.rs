//! Indexer side of a remote build: the same graph builders as a local build, run on what the
//! indexer read from core.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

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
use crate::index::hnsw_index::remote::blocks::StreamedBlock;
use crate::index::hnsw_index::remote::wire::{RemoteGraphParams, RemoteIndexParams};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// Build the graph `params` asks for, over `vector_storage`, whose deleted vectors are the
/// points not to link, scoring with `quantized_vectors` if given, as a local build does.
/// `id_tracker` maps every offset of the storage. After the main graph, links the payload
/// blocks `next_blocks` returns, batch by batch, in order, until it returns `None`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_requested_graph(
    params: &RemoteGraphParams,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: Option<&QuantizedVectors>,
    id_tracker: &IdTrackerEnum,
    num_threads: usize,
    gpu_device: Option<&LockedGpuDevice<'_>>,
    rng: &mut dyn Rng,
    stopped: &AtomicBool,
    progress: &ProgressTracker,
    next_blocks: &mut dyn FnMut() -> OperationResult<Option<Vec<StreamedBlock>>>,
) -> OperationResult<GraphLayersBuilder> {
    let RemoteIndexParams::Hnsw(hnsw) = &params.index;
    let mut ctx = GraphBuildContext {
        id_tracker,
        vector_storage,
        quantized_vectors,
        progress: None,
        main_params: GraphParams {
            hnsw_m: hnsw.main_m(),
            ef_construct: hnsw.ef_construct,
            entry_points_num: hnsw.entry_points_num,
        },
        block_params: GraphParams {
            hnsw_m: hnsw.block_m(),
            ef_construct: hnsw.ef_construct,
            entry_points_num: 1,
        },
        stopped,
    };
    ctx.progress = ctx.has_main_graph().then(|| MainGraphProgress {
        migrate: progress.subtask("migrate"),
        main_graph: progress.subtask("main_graph"),
    });

    let pool = build_thread_pool(num_threads.max(1))?;
    let mut builder = GraphBuilder::select(
        ctx,
        &pool,
        rng,
        GraphBuildOptions {
            old_index: None,
            gpu_device,
            has_payload_blocks: params.has_payload_blocks,
            // Never forward the task again
            remote: None,
        },
    )?;

    builder.build_main()?;

    // Blocks of a field come together, fields in build order
    let mut field: Option<(String, BlockSkip, ProgressTracker, Arc<AtomicU64>)> = None;
    while let Some(blocks) = next_blocks()? {
        for block in blocks {
            let StreamedBlock {
                field: name,
                skip_if_connected,
                cardinality,
                points,
            } = block;
            if field.as_ref().is_none_or(|(current, ..)| *current != name) {
                let field_progress = progress.subtask(format!("additional_links:{name}"));
                field_progress.start();
                let counter = field_progress.track_progress(None);
                let skip = BlockSkip::from_skip_if_connected(skip_if_connected);
                field = Some((name, skip, field_progress, counter));
            }
            let Some((_, skip, _, counter)) = &field else {
                unreachable!("set above");
            };
            builder.build_loaded_field_blocks(
                counter,
                *skip,
                std::iter::once(PayloadBlock {
                    cardinality,
                    points,
                }),
            )?;
        }
    }
    drop(field);

    let (graph, _indexed) = builder.finalize()?;
    Ok(graph)
}
