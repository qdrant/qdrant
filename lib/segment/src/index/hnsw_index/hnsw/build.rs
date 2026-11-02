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

use super::old_index::{OldIndexCandidate, payload_hnsw_m, payload_links_reusable};
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
mod block_filter;
mod context;
mod main_graph;
mod payload_blocks;

use self::additional_links::build_additional_links;
use self::context::{GraphBuildContext, GraphParams};
use self::main_graph::build_main_graph_on_cpu;
use self::payload_blocks::payload_block_fields;

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

        let mut build_main_graph = config.m > 0;
        if !build_main_graph {
            debug!("skip building main HNSW graph");
        }

        let payload_m = payload_hnsw_m(&config);

        // Progress subtasks
        let progress_migrate = build_main_graph.then(|| progress.subtask("migrate"));
        let progress_main_graph = build_main_graph.then(|| progress.subtask("main_graph"));
        let payload_block_fields = payload_block_fields(&payload_index_ref, payload_m);
        let progress_payload_blocks = (!payload_block_fields.is_empty()).then(|| {
            let progress_additional_links = progress.subtask("additional_links");
            let fields = payload_block_fields
                .into_iter()
                .map(|(field, payload_schema)| {
                    let subtask_name = format!("{}:{field}", payload_schema.name());
                    let field_progress = progress_additional_links.subtask(subtask_name);
                    (field_progress, field, payload_schema)
                })
                .collect::<Vec<_>>();
            (progress_additional_links, fields)
        });

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

        #[cfg(feature = "gpu")]
        let gpu_name_postfix = if let Some(gpu_device) = gpu_device {
            format!(" and GPU {}", gpu_device.device().name())
        } else {
            Default::default()
        };
        #[cfg(not(feature = "gpu"))]
        let gpu_name_postfix = "";
        #[cfg(not(feature = "gpu"))]
        let _ = gpu_device;
        debug!(
            "building HNSW for {total_vector_count} vectors with {} CPUs{gpu_name_postfix}",
            permit.num_cpus,
        );

        let num_entries = std::cmp::max(
            1,
            total_vector_count
                .checked_div(config.full_scan_threshold)
                .unwrap_or(0)
                * 10,
        );
        let ctx = GraphBuildContext {
            id_tracker: &id_tracker_ref,
            vector_storage: &vector_storage_ref,
            quantized_vectors: quantized_vectors_ref.as_ref(),
            main_params: GraphParams {
                hnsw_m: HnswM::new(config.m, config.m0),
                ef_construct: config.ef_construct,
                entry_points_num: num_entries,
            },
            block_params: GraphParams {
                hnsw_m: payload_m,
                ef_construct: config.ef_construct,
                entry_points_num: 1,
            },
            stopped,
        };

        let mut graph_layers_builder = GraphLayersBuilder::new(
            total_vector_count,
            ctx.main_params.hnsw_m,
            ctx.main_params.ef_construct,
            ctx.main_params.entry_points_num,
            HNSW_USE_HEURISTIC,
        );

        let pool = build_thread_pool(permit.num_cpus as usize)?;

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

        // Try to build graphs on GPU if possible.
        #[cfg(feature = "gpu")]
        let (gpu_vectors, gpu_graph) = super::gpu_build::upload_and_build_main_graph(
            gpu_device,
            build_main_graph,
            progress_payload_blocks.is_some(),
            id_tracker_ref.deref(),
            &vector_storage_ref,
            quantized_vectors_ref.as_ref(),
            &graph_layers_builder,
            deleted_bitslice,
            num_entries,
            stopped,
        )?;
        #[cfg(not(feature = "gpu"))]
        let gpu_graph: Option<GraphLayersBuilder> = None;

        if let Some(gpu_graph) = gpu_graph {
            graph_layers_builder = gpu_graph;
            build_main_graph = false;
        }

        check_process_stopped(stopped)?;

        if build_main_graph {
            build_main_graph_on_cpu(
                ctx,
                old_index.as_ref(),
                &graph_layers_builder,
                progress_migrate.unwrap(),
                progress_main_graph.unwrap(),
                &pool,
            )?;
        }

        let payload_seed = old_index
            .as_ref()
            .filter(|old_index| payload_links_reusable(&config, &old_index.index.config));

        if let Some((progress_additional_links, indexed_fields)) = progress_payload_blocks {
            #[cfg(feature = "gpu")]
            let mut gpu_insert_context = super::gpu_build::create_gpu_insert_context(
                gpu_vectors.as_ref(),
                payload_m,
                config.ef_construct,
            )?;
            #[cfg(not(feature = "gpu"))]
            let mut gpu_insert_context = None;

            build_additional_links(
                ctx,
                &payload_index_ref,
                &mut gpu_insert_context,
                &mut graph_layers_builder,
                &config,
                indexed_fields,
                payload_seed,
                progress_additional_links,
                &pool,
                rng,
            )?;
        } else {
            debug!("skip building additional HNSW links");
        }
        drop(old_index);

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
