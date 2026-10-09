use std::ops::Deref as _;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use common::bitvec::BitSlice;
#[cfg(target_os = "linux")]
use common::cpu::linux_low_thread_priority;
use common::types::PointOffsetType;
use fs_err as fs;
use log::debug;
use rand::Rng;
use rayon::ThreadPool;

use super::old_index::OldIndexCandidate;
use super::telemetry::HNSWSearchesTelemetry;
use super::{HNSW_USE_HEURISTIC, HNSWIndex, HnswIndexOpenArgs, derive_config};
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph::HnswGraph;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_links::{GraphLinksFormatParam, StorageGraphLinksVectors};
use crate::segment_constructor::VectorIndexBuildArgs;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

mod additional_links;
#[cfg(feature = "gpu")]
mod gpu_builder;
mod graph_builder;
mod hnsw_builder;
mod pipnn_builder;
mod remote_builder;

use self::additional_links::{additional_links_fields, build_additional_links};
use self::graph_builder::{GraphBuildContext, GraphBuilder as _, GraphBuilderEnum};

impl HNSWIndex {
    pub fn build<R: Rng + ?Sized>(
        open_args: HnswIndexOpenArgs<'_>,
        build_args: VectorIndexBuildArgs<'_, R>,
    ) -> OperationResult<Self> {
        if HnswGraphConfig::get_config_path(open_args.path).exists()
            || GraphLayers::get_path(open_args.path).exists()
        {
            log::warn!(
                "HNSW index already exists at {:?}, skipping building",
                open_args.path
            );
            debug_assert!(false);
            return Self::open(open_args);
        }

        let HnswIndexOpenArgs {
            path,
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            hnsw_config,
        } = open_args;
        let VectorIndexBuildArgs {
            permit,
            old_indices,
            gpu_device,
            rng,
            stopped,
            hnsw_global_config,
            feature_flags,
            inline_vectors,
            progress,
        } = build_args;

        fs::create_dir_all(path)?;

        let id_tracker_ref = id_tracker.borrow();
        let vector_storage_ref = vector_storage.borrow();
        let quantized_vectors_ref = quantized_vectors.borrow();
        let payload_index_ref = payload_index.borrow();

        let total_vector_count = vector_storage_ref.total_vector_count();
        let mut config = derive_config(&hnsw_config, &*vector_storage_ref, total_vector_count);

        let build_main_graph = config.m > 0;
        if !build_main_graph {
            debug!("skip building main HNSW graph");
        }

        let payload_m = HnswM::new(
            config.payload_m.unwrap_or(config.m),
            config.payload_m0.unwrap_or(config.m0),
        );

        // Progress subtasks
        let additional_links_fields = additional_links_fields(&payload_index_ref, payload_m);

        let old_index = old_indices
            .iter()
            .filter_map(|old_index| {
                OldIndexCandidate::evaluate(
                    &feature_flags,
                    old_index,
                    &config,
                    hnsw_global_config,
                    &vector_storage_ref,
                    &quantized_vectors_ref,
                    id_tracker_ref.deref(),
                )
            })
            .max_by_key(|old_index| old_index.valid_points);

        // Build main index graph
        let deleted_bitslice = vector_storage_ref.deleted_vector_bitslice();

        debug!(
            "building HNSW for {total_vector_count} vectors with {} CPUs",
            permit.num_cpus,
        );

        let num_entries = std::cmp::max(
            1,
            total_vector_count
                .checked_div(config.full_scan_threshold)
                .unwrap_or(0)
                * 10,
        );

        let pool = build_thread_pool(permit.num_cpus as usize)?;
        let context = GraphBuildContext {
            id_tracker: id_tracker_ref.deref(),
            vector_storage: &vector_storage_ref,
            quantized_vectors: &quantized_vectors_ref,
            pool: &pool,
            progress: &progress,
            gpu_device,
            stopped,
        };

        let mut graph_builder = GraphBuilderEnum::select(
            context,
            build_main_graph || !additional_links_fields.is_empty(),
            num_entries,
        )?;

        let mut graph_layers_builder = GraphLayersBuilder::new(
            total_vector_count,
            HnswM::new(config.m, config.m0),
            config.ef_construct,
            num_entries,
            HNSW_USE_HEURISTIC,
        );

        let old_index = old_index.map(|old_index| old_index.reuse(total_vector_count));

        let mut indexed_vectors = 0;
        for vector_id in id_tracker_ref
            .point_mappings()
            .iter_internal_excluding(deleted_bitslice)
        {
            check_process_stopped(stopped)?;
            indexed_vectors += 1;

            let level = old_index
                .as_ref()
                .and_then(|old_index| old_index.point_level(vector_id))
                .unwrap_or_else(|| graph_layers_builder.get_random_layer(rng));
            graph_layers_builder.set_levels(vector_id, level);
        }

        if build_main_graph {
            graph_layers_builder =
                graph_builder.build_main_graph(context, graph_layers_builder, old_index)?;
        } else {
            drop(old_index);
        }

        if !additional_links_fields.is_empty() {
            let indexed_payload_vectors = build_additional_links(
                context,
                &payload_index_ref,
                &mut graph_builder,
                &mut graph_layers_builder,
                &config,
                payload_m,
                additional_links_fields,
                rng,
            )?;

            debug_assert!(indexed_vectors >= indexed_payload_vectors || config.m == 0);
            indexed_vectors = indexed_vectors.max(indexed_payload_vectors);
            debug_assert!(indexed_payload_vectors <= total_vector_count);
        } else {
            debug!("skip building additional HNSW links");
        }

        config.indexed_vector_count.replace(indexed_vectors);

        // Always skip loading graph to RAM on build
        // as it will be discarded anyway
        let cold = true;

        let graph = save_graph(
            graph_layers_builder,
            path,
            inline_vectors,
            &vector_storage_ref,
            quantized_vectors_ref.as_ref(),
            cold,
        )?;
        debug_assert!(
            deleted_points_unlinked(&graph, deleted_bitslice),
            "Deleted point in the graph"
        );

        debug!("finish additional payload field indexing");

        config.save(&HnswGraphConfig::get_config_path(path))?;

        drop(id_tracker_ref);
        drop(vector_storage_ref);
        drop(quantized_vectors_ref);
        drop(payload_index_ref);

        Ok(HNSWIndex {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            config,
            path: path.to_owned(),
            graph: HnswGraph::Direct(Arc::new(graph)),
            searches_telemetry: HNSWSearchesTelemetry::new(),
        })
    }
}

/// Rayon pool for graph construction. On Linux its threads run at low priority so they
/// interfere less with serving traffic.
fn build_thread_pool(num_threads: usize) -> OperationResult<ThreadPool> {
    let pool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("hnsw-build-{idx}"))
        .num_threads(num_threads)
        .spawn_handler(|thread| {
            let mut b = thread::Builder::new();
            if let Some(name) = thread.name() {
                b = b.name(name.to_owned());
            }
            if let Some(stack_size) = thread.stack_size() {
                b = b.stack_size(stack_size);
            }
            b.spawn(|| {
                #[cfg(target_os = "linux")]
                if let Err(err) = linux_low_thread_priority() {
                    log::debug!(
                        "Failed to set low thread priority for HNSW building, ignoring: {err}"
                    );
                }

                thread.run()
            })?;
            Ok(())
        })
        .build()?;
    Ok(pool)
}

/// Write the graph links to `path`. With `inline_vectors`, the quantized vectors are stored
/// next to the links, which the storages must be able to provide.
fn save_graph(
    graph_layers_builder: GraphLayersBuilder,
    path: &Path,
    inline_vectors: bool,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: Option<&QuantizedVectors>,
    cold: bool,
) -> OperationResult<GraphLayers> {
    let graph_links_vectors = inline_vectors
        .then(|| {
            StorageGraphLinksVectors::try_new(vector_storage, quantized_vectors).ok_or_else(|| {
                OperationError::service_error(
                    "Inline vectors requested, but the storages cannot provide them",
                )
            })
        })
        .transpose()?;
    let format_param = match graph_links_vectors.as_ref() {
        Some(v) => GraphLinksFormatParam::CompressedWithVectors(v),
        None => GraphLinksFormatParam::Compressed,
    };
    graph_layers_builder.into_graph_layers(path, format_param, cold)
}

/// Whether no deleted point has links on level 0.
fn deleted_points_unlinked(graph: &GraphLayers, deleted_bitslice: &BitSlice) -> bool {
    deleted_bitslice
        .iter_ones()
        .all(|idx| graph.links.links_empty(idx as PointOffsetType, 0))
}
