//! The steps remote indexing adds to `HNSWIndex::build` and `GraphBuilder::select`.
//!
//! Without the `remote-indexing` feature, `no_remote_steps.rs` stands in with the same items,
//! which do nothing. The build flow itself needs no `cfg`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use atomic_refcell::AtomicRefCell;
use common::progress_tracker::ProgressTracker;
use log::debug;

use super::block_filter::{BlockSkip, max_block_size};
use super::graph_builder::GraphBuilder;
use super::payload_blocks::{BlockLoader, PayloadBlock, field_payload_blocks};
pub(super) use super::remote_builder::RemoteBuildArgs;
use super::remote_builder::{RemoteBuilder, worth_remote};
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_algorithm::MainGraphAlgorithm;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::hnsw_index::remote::blocks::BlockStream;
use crate::index::hnsw_index::remote::layout::RemoteVectorLayout;
use crate::index::hnsw_index::remote::metrics::REMOTE_INDEXING_METRICS;
use crate::index::hnsw_index::remote::wire::{BlockPlan, PlannedBlock, VectorSource};
use crate::index::hnsw_index::remote::{TaskScope, remote_indexer};
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::{PayloadFieldSchema, PayloadKeyType};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead as _};

/// The payload blocks a remote build prepared before it fell back.
pub(super) type FallbackBlocks = BlockPlan;

/// What a remote build needs, if a remote indexer is configured, the storage can be built
/// remotely, and the vector data is large enough (D3, D11). Whether the build actually goes
/// remote is decided by `GraphBuilder::select`.
pub(super) fn build_args(
    path: &Path,
    vector_storage: &Arc<AtomicRefCell<VectorStorageEnum>>,
    vector_storage_ref: &VectorStorageEnum,
    quantized_vectors: &Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    quantized_vectors_ref: Option<&QuantizedVectors>,
    progress: &ProgressTracker,
) -> Option<RemoteBuildArgs> {
    let indexer = remote_indexer()?;
    let layout = RemoteVectorLayout::of(vector_storage_ref)?;
    // `total_vector_count × dim × element size`, like `indexing_threshold_kb`
    let vector_bytes = vector_storage_ref.size_of_available_vectors_in_bytes() as u64;
    if !worth_remote(indexer.config(), vector_bytes) {
        return None;
    }

    // Send the quantized codes if the build scores from them alone (O2), as scalar, binary and
    // TurboQuant quantization do: the indexer then builds what a local build would, from a
    // fraction of the bytes. Product quantization encodes its queries from the original
    // vectors: the storage's vectors are sent, and the indexer builds without PQ.
    let builds_from_codes = |quantized: &&QuantizedVectors| {
        vector_storage_ref.total_vector_count() == 0 || quantized.builds_from_codes(0)
    };
    let quantization = quantized_vectors_ref
        .filter(builds_from_codes)
        .and_then(QuantizedVectors::remote_quantization);
    let (source, quantized_vectors) = match &quantization {
        Some(_) => (VectorSource::Quantized, Some(quantized_vectors.clone())),
        None => (VectorSource::Storage, None),
    };

    Some(RemoteBuildArgs {
        indexer,
        vector_storage: vector_storage.clone(),
        layout,
        scope: TaskScope {
            index_path: path.to_owned(),
            vector_name: vector_name_from_index_path(path),
        },
        vector_bytes,
        source,
        quantization,
        quantized_vectors,
        progress: progress.subtask("remote_build"),
    })
}

/// Whether `old_index` is worth healing locally rather than building remotely from scratch:
/// it holds at least `min_healing_coverage` of the `point_count` points to index (D12).
pub(super) fn heals_locally(
    remote: &RemoteBuildArgs,
    old_index: &OldIndex<'_>,
    point_count: usize,
) -> bool {
    let coverage = old_index.coverage(point_count);
    let min_coverage = remote.indexer.config().min_healing_coverage;
    debug!("old graph covers {coverage:.3} of the points, heals locally from {min_coverage:.3}");
    coverage >= min_coverage
}

/// A remote build goes ahead, although there is an old graph: it covers too few of the points.
pub(super) fn remote_over_healing() {
    REMOTE_INDEXING_METRICS
        .remote_over_healing
        .fetch_add(1, Ordering::Relaxed);
    debug!("old graph covers too few points to heal, building remotely");
}

/// A remote build was possible, but an old graph is worth healing, which stays local (D12).
pub(super) fn kept_local_for_healing() {
    REMOTE_INDEXING_METRICS
        .local_for_healing
        .fetch_add(1, Ordering::Relaxed);
    debug!("old graph worth healing, building locally");
}

/// The remote method, with `cpu` as its local fallback.
pub(super) fn remote_builder<'a>(
    cpu: GraphBuilder<'a>,
    args: RemoteBuildArgs,
    has_payload_blocks: bool,
    main_graph: MainGraphAlgorithm,
) -> GraphBuilder<'a> {
    GraphBuilder::Remote(Box::new(RemoteBuilder::new(
        cpu,
        args,
        has_payload_blocks,
        main_graph,
    )))
}

/// Before `build_main`, for a remote builder: submit the task, so that the indexer builds the
/// main graph while the payload blocks of `fields` are prepared here and streamed to it.
/// Local builders read the blocks themselves after `build_main`.
pub(super) fn start<'f>(
    builder: &mut GraphBuilder<'_>,
    fields: impl IntoIterator<Item = (&'f PayloadKeyType, &'f PayloadFieldSchema)>,
    payload_index: &StructPayloadIndex,
    vector_storage: &VectorStorageEnum,
    config: &HnswGraphConfig,
    stopped: &AtomicBool,
) -> OperationResult<()> {
    let Some(remote) = builder.remote() else {
        return Ok(());
    };
    let stream = remote.start();
    let has_main_graph = remote.cpu.local().ctx.has_main_graph();
    stream_payload_blocks(
        fields,
        payload_index,
        vector_storage,
        config,
        has_main_graph,
        stopped,
        &stream,
    )?;
    stream.finish();
    Ok(())
}

/// After `build_main`: whether the payload blocks are linked already, by the indexer.
pub(super) fn blocks_linked(builder: &mut GraphBuilder<'_>) -> bool {
    let linked = builder
        .remote()
        .is_some_and(|remote| remote.blocks_linked());
    if linked {
        debug!("payload blocks linked by the remote indexer");
    }
    linked
}

/// After `build_main`: the payload blocks a remote build prepared before it fell back, for the
/// local build to link. `None` if there are none to reuse.
pub(super) fn fallback_blocks(builder: &mut GraphBuilder<'_>) -> Option<FallbackBlocks> {
    builder.remote()?.fallback_blocks()
}

/// Link the blocks of `field` from `plan`, instead of reading them again.
pub(super) fn build_fallback_field_blocks(
    builder: &mut GraphBuilder<'_>,
    plan: &FallbackBlocks,
    field: &PayloadKeyType,
    progress: &ProgressTracker,
    skip: BlockSkip,
) -> OperationResult<()> {
    let counter = progress.track_progress(None);
    let blocks = plan
        .field(&field.to_string())
        .into_iter()
        .flat_map(|planned| {
            planned.blocks.iter().map(|block| PayloadBlock {
                cardinality: block.cardinality,
                points: block.points.clone(),
            })
        });
    builder.build_loaded_field_blocks(&counter, skip, blocks)
}

/// A lower bound on the average number of level 0 links per point of the finished main graph,
/// as [`BlockFilter`](super::block_filter::BlockFilter) measures it.
///
/// A point of the graph has at least `m0 / 4` links on average. The measured average is over
/// all `total` points though, and only the `indexed` ones have links: deleted points and
/// points without this vector have none.
fn min_average_links(m0: usize, indexed: usize, total: usize) -> usize {
    if total == 0 {
        return 0;
    }
    m0 * indexed / (4 * total)
}

/// Prepare the payload blocks of a remote build and append them to `stream`, in build order,
/// while the indexer builds the main graph. Does not finish the stream.
///
/// Leaves out mostly deleted blocks, and blocks too large to ever be built. The local size
/// limit depends on the average connectivity of the finished main graph, which is remote, so
/// this uses a bound that is not below it, see [`min_average_links`]. The indexer applies the
/// exact limit, and the connectivity check (O1).
fn stream_payload_blocks<'f>(
    fields: impl IntoIterator<Item = (&'f PayloadKeyType, &'f PayloadFieldSchema)>,
    payload_index: &StructPayloadIndex,
    vector_storage: &VectorStorageEnum,
    config: &HnswGraphConfig,
    has_main_graph: bool,
    stopped: &AtomicBool,
    stream: &BlockStream,
) -> OperationResult<()> {
    // The limit of `BlockFilter`, with a lower bound on connectivity
    let total_vector_count = vector_storage.total_vector_count();
    let max_block_size = max_block_size(
        total_vector_count,
        min_average_links(
            config.m0,
            vector_storage.available_vector_count(),
            total_vector_count,
        ),
        has_main_graph,
    );

    let loader = BlockLoader {
        payload_index,
        vector_storage,
        config,
        stopped,
    };
    for (index_pos, (field, schema)) in fields.into_iter().enumerate() {
        let skip = BlockSkip::for_field(index_pos, payload_index.is_tenant(field));
        let field_name = field.to_string();
        for block in field_payload_blocks(field, schema, payload_index, None, config, stopped)? {
            if block.cardinality > max_block_size {
                continue;
            }
            if let Some(PayloadBlock {
                cardinality,
                points,
            }) = loader.load(block)?
            {
                stream.push(
                    &field_name,
                    skip.skip_if_connected(),
                    PlannedBlock {
                        cardinality,
                        points,
                    },
                );
            }
        }
    }
    Ok(())
}

/// The vector name of a vector index directory, see `get_vector_index_path`. Names may
/// contain `/`, so it is whatever follows the `vector_index` component.
fn vector_name_from_index_path(path: &Path) -> String {
    use crate::segment_constructor::VECTOR_INDEX_PATH;

    let components: Vec<_> = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let prefix = format!("{VECTOR_INDEX_PATH}-");
    for (i, component) in components.iter().enumerate().rev() {
        let first = if component == VECTOR_INDEX_PATH {
            String::new()
        } else if let Some(name) = component.strip_prefix(&prefix) {
            name.to_string()
        } else {
            continue;
        };
        return std::iter::once(first)
            .chain(components[i + 1..].iter().cloned())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("/");
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Core must not leave out a block that a local build would link. A local build measures
    /// the average links over all points, with and without the vector.
    #[test]
    fn block_limit_follows_the_share_of_indexed_points() {
        let m0 = 32;
        let total = 100_000;
        // Every point has the vector: at least `m0 / 4` links on average
        assert_eq!(min_average_links(m0, total, total), 8);
        assert_eq!(
            max_block_size(total, min_average_links(m0, total, total), true),
            50_000,
        );

        // A tenth of the points has the vector, with 30 links each: a local build measures 3
        // links on average, so its limit is above the point count and it links every block
        let indexed = total / 10;
        let local_limit = max_block_size(total, 3, true);
        assert!(local_limit > total);
        let core_limit = max_block_size(total, min_average_links(m0, indexed, total), true);
        assert!(core_limit >= local_limit);

        assert_eq!(min_average_links(m0, 0, 0), 0);
    }
}
