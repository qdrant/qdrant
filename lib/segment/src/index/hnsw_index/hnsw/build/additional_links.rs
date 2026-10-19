mod block;
mod seeds;

use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use log::debug;
use rand::Rng;
use rayon::ThreadPool;

use self::block::Block;
use self::seeds::BlockSeeds;
use super::block_filter::BlockFilter;
use super::context::GraphBuildContext;
use super::payload_blocks::{field_payload_blocks, load_payload_block};
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuInsertContext;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
#[cfg(feature = "gpu")]
use crate::index::hnsw_index::hnsw::HNSW_USE_HEURISTIC;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::types::PayloadFieldSchema;
use crate::vector_storage::VectorStorageRead;

/// Build per-payload-block subgraphs for every field in `indexed_fields` and merge them
/// into `graph_layers_builder`.
///
/// With an `old_index` (whose payload links have the same `m`), each block starts from the
/// links its points had to each other in the old graph; see [`BlockSeeds`].
#[allow(clippy::too_many_arguments)]
#[cfg_attr(
    not(feature = "gpu"),
    allow(unused_variables, clippy::needless_pass_by_ref_mut)
)]
pub(super) fn build_additional_links<R: Rng + ?Sized>(
    ctx: GraphBuildContext,
    payload_index: &StructPayloadIndex,
    gpu_insert_context: &mut Option<GpuInsertContext>,
    graph_layers_builder: &mut GraphLayersBuilder,
    config: &HnswGraphConfig,
    indexed_fields: Vec<(ProgressTracker, JsonPath, PayloadFieldSchema)>,
    old_index: Option<&OldIndex<'_>>,
    progress_additional_links: ProgressTracker,
    pool: &ThreadPool,
    rng: &mut R,
) -> OperationResult<()> {
    let GraphBuildContext {
        id_tracker,
        vector_storage,
        quantized_vectors,
        block_params,
        stopped,
        ..
    } = ctx;
    let total_vector_count = vector_storage.total_vector_count();

    progress_additional_links.start();

    let all_points = id_tracker
        .point_mappings()
        .iter_internal_excluding(vector_storage.deleted_vector_bitslice())
        .collect::<Vec<_>>();
    // Created from the graph at the first block
    let mut block_filter = None;

    let mut seeds = old_index
        .map(|old_index| BlockSeeds::new(old_index, block_params.hnsw_m, total_vector_count));
    let mut seeded_points = 0;
    let mut block_points = 0;

    // The GPU stores vectors and links by segment id, so it builds blocks in a segment-sized
    // builder. This one only carries the block graph's settings: `payload_m`, `ef_construct`
    // and level 0 for every point.
    #[cfg(feature = "gpu")]
    let gpu_block_reference = gpu_insert_context.is_some().then(|| {
        GraphLayersBuilder::new_with_params(
            total_vector_count,
            block_params.hnsw_m,
            block_params.ef_construct,
            block_params.entry_points_num,
            HNSW_USE_HEURISTIC,
            false,
        )
    });

    for (index_pos, (field_progress, field, payload_schema)) in
        indexed_fields.into_iter().enumerate()
    {
        field_progress.start();

        debug!("building additional index for field {field}");

        let blocks = field_payload_blocks(
            &field,
            &payload_schema,
            payload_index,
            old_index,
            config,
            stopped,
        )?;

        // Tenant blocks are always built
        let skip_if_connected = index_pos > 0 && !payload_index.is_tenant(&field);

        let counter = field_progress.track_progress(None);

        for payload_block in blocks {
            check_process_stopped(stopped)?;

            let filter = block_filter.get_or_insert_with(|| {
                BlockFilter::new(ctx, graph_layers_builder, &all_points, rng)
            });
            // Read the points of one block at a time, and only of blocks small enough to build
            if !filter.fits(payload_block.cardinality) {
                continue;
            }
            let Some(payload_block) = load_payload_block(
                payload_block,
                payload_index,
                vector_storage,
                config,
                stopped,
            )?
            else {
                continue;
            };
            if !filter.should_build(graph_layers_builder, &payload_block, skip_if_connected, rng) {
                continue;
            }

            let points_to_index = &payload_block.points;

            #[cfg(feature = "gpu")]
            if let (Some(gpu_insert_context), Some(reference_graph)) =
                (gpu_insert_context.as_mut(), &gpu_block_reference)
                && let Some(gpu_graph) =
                    crate::index::hnsw_index::hnsw::gpu_build::build_block_on_gpu(
                        id_tracker,
                        vector_storage,
                        quantized_vectors,
                        gpu_insert_context,
                        reference_graph,
                        points_to_index,
                        stopped,
                    )?
            {
                graph_layers_builder.merge_from_other(gpu_graph);
                continue;
            }

            // Points that kept enough links from the old graph start with them, the rest are
            // inserted by search
            let block = Block::new(ctx, points_to_index);
            let to_insert = match seeds.as_mut() {
                Some(seeds) => seeds.seed_block(&block, graph_layers_builder, pool, &counter)?,
                None => (0..points_to_index.len() as PointOffsetType).collect(),
            };
            block_points += points_to_index.len();
            seeded_points += points_to_index.len() - to_insert.len();
            block.insert(&to_insert, pool, &counter)?;
            block.merge_into(graph_layers_builder);
        }
    }
    if seeds.is_some() {
        debug!("seeded {seeded_points} of {block_points} payload block points from the old graph");
    }
    Ok(())
}
