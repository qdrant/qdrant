use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::thread::JoinHandle;

use common::types::PointOffsetType;
use parking_lot::Mutex;
use rayon::ThreadPool;

use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::gpu::batched_points::BatchedPoints;
use crate::index::hnsw_index::gpu::cpu_level_builder::build_level_on_cpu;
use crate::index::hnsw_index::gpu::create_graph_layers_builder;
use crate::index::hnsw_index::gpu::gpu_level_builder::build_level_on_gpu;
use crate::index::hnsw_index::gpu::gpu_search_context::GpuSearchContext;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::payload_storage::FilterContext;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{RawScorer, VectorStorageEnum};

#[allow(clippy::too_many_arguments)]
pub fn build_hnsw_on_gpu<'a>(
    pool: &ThreadPool,
    reference_graph: &GraphLayersBuilder,
    debug_messenger: Option<&dyn gpu::DebugMessenger>,
    groups_count: usize,
    vector_storage: &VectorStorageEnum,
    quantized_storage: Option<&QuantizedVectors>,
    entry_points_num: usize,
    force_half_precision: bool,
    min_cpu_linked_points_count: usize,
    ids: Vec<PointOffsetType>,
    points_scorer_builder: impl Fn(
            PointOffsetType,
        )
            -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
        + Send
        + Sync,
) -> OperationResult<GraphLayersBuilder> {
    log::debug!("Building GPU graph with max groups count: {}", groups_count);

    let num_vectors = reference_graph.links_layers.len();
    let m = reference_graph.m;
    let m0 = reference_graph.m0;
    let ef = reference_graph.ef_construct;

    let batched_points = Arc::new(BatchedPoints::new(
        |point_id| reference_graph.get_point_level(point_id),
        ids,
        groups_count,
    )?);

    let graph_layers_builder = Arc::new(create_graph_layers_builder(
        &batched_points,
        num_vectors,
        m,
        m0,
        ef,
        entry_points_num,
    )?);

    let gpu_search_context = Arc::new(Mutex::new(GpuSearchContext::new(
        debug_messenger,
        groups_count,
        vector_storage,
        quantized_storage,
        m,
        m0,
        ef,
        num_vectors,
        force_half_precision,
    )?));

    let mut gpu_thread_handle: Option<JoinHandle<OperationResult<()>>> = None;
    let gpu_built_points = Arc::new(AtomicUsize::new(batched_points.points.len()));

    let cpu_stop_condition = |cpu_built_points| {
        assert_ne!(cpu_built_points, batched_points.points.len());

        // if CPU is faster than GPU, wait for GPU to catch up
        while cpu_built_points >= gpu_built_points.load(std::sync::atomic::Ordering::Relaxed) {
            println!(
                "Waiting for GPU to catch up (CPU ready {}, GPU ready {})",
                cpu_built_points,
                gpu_built_points.load(std::sync::atomic::Ordering::Relaxed)
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let is_gpu_ready = gpu_built_points.load(std::sync::atomic::Ordering::Relaxed)
            == batched_points.points.len();
        let min_required_points_reached = cpu_built_points >= min_cpu_linked_points_count;

        // stop id GPU is ready and min required points are linked
        min_required_points_reached && is_gpu_ready
    };

    for level in (0..batched_points.levels_count).rev() {
        // Start building level on CPU
        let gpu_start_index = build_level_on_cpu(
            &pool,
            &graph_layers_builder,
            &batched_points,
            level,
            &cpu_stop_condition,
            &points_scorer_builder,
        )?;

        // CPU is ready, finish prev level GPU thread to continue with this level
        if let Some(handle) = gpu_thread_handle.take() {
            handle.join().unwrap()?;
        }

        // Clear GPU built points counter
        gpu_built_points.store(0, std::sync::atomic::Ordering::Relaxed);

        println!(
            "Starting GPU level {}, skipped points {}",
            level, gpu_start_index
        );

        let gpu_search_context = gpu_search_context.clone();
        let graph_layers_builder = graph_layers_builder.clone();
        let batched_points = batched_points.clone();
        let gpu_built_points = gpu_built_points.clone();
        // Continue level building on GPU in separate thread
        gpu_thread_handle = Some(std::thread::spawn(move || -> OperationResult<()> {
            let mut gpu_search_context = gpu_search_context.lock();
            gpu_search_context.upload_links(level, &graph_layers_builder)?;
            build_level_on_gpu(
                &mut gpu_search_context,
                &batched_points,
                gpu_start_index,
                level,
                |count| {
                    gpu_built_points.store(count, std::sync::atomic::Ordering::Relaxed);
                },
            )?;
            gpu_search_context.download_links(level, &graph_layers_builder)?;
            Ok(())
        }));
    }

    // Wait for the last GPU level
    if let Some(handle) = gpu_thread_handle.take() {
        handle.join().unwrap()?;
    }

    {
        let gpu_search_context = gpu_search_context.lock();
        log::debug!(
            "Gpu graph patches time: {:?}, count {:?}, avg {:?}",
            &gpu_search_context.patches_timer,
            gpu_search_context.patches_count,
            gpu_search_context
                .patches_timer
                .checked_div(gpu_search_context.patches_count as u32)
                .unwrap_or_default(),
        );
        log::debug!(
            "Gpu graph update entries time: {:?}, count {:?}, avg {:?}",
            &gpu_search_context.updates_timer,
            gpu_search_context.updates_count,
            gpu_search_context
                .updates_timer
                .checked_div(gpu_search_context.updates_count as u32)
                .unwrap_or_default(),
        );
    }

    Ok(Arc::into_inner(graph_layers_builder).unwrap())
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use super::*;
    use crate::fixtures::index_fixtures::FakeFilterContext;
    use crate::index::hnsw_index::gpu::tests::{
        check_graph_layers_builders_quality, compare_graph_layers_builders,
        create_gpu_graph_test_data, GpuGraphTestData,
    };

    fn build_gpu_graph(
        test: &GpuGraphTestData,
        groups_count: usize,
        min_cpu_linked_points_count: usize,
    ) -> GraphLayersBuilder {
        let num_vectors = test.graph_layers_builder.links_layers.len();

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(groups_count)
            .build()
            .unwrap();

        let debug_messenger = gpu::PanicIfErrorMessenger {};

        let ids = (0..num_vectors as PointOffsetType).collect();
        build_hnsw_on_gpu(
            &pool,
            &test.graph_layers_builder,
            Some(&debug_messenger),
            groups_count,
            &test.vector_storage.borrow(),
            None,
            1,
            false,
            min_cpu_linked_points_count,
            ids,
            |point_id| {
                let fake_filter_context = FakeFilterContext {};
                let added_vector = test.vector_holder.vectors.get(point_id).to_vec();
                let raw_scorer = test
                    .vector_holder
                    .get_raw_scorer(added_vector.clone())
                    .unwrap();
                Ok((raw_scorer, Some(Box::new(fake_filter_context))))
            },
        )
        .unwrap()
    }

    #[test]
    fn test_gpu_hnsw_equivalency() {
        let num_vectors = 1024;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;
        let min_cpu_linked_points_count = 64;

        let test = create_gpu_graph_test_data(num_vectors, dim, m, m0, ef, 0);
        let graph_layers_builder = build_gpu_graph(&test, 1, min_cpu_linked_points_count);

        compare_graph_layers_builders(&test.graph_layers_builder, &graph_layers_builder);
    }

    #[test]
    fn test_gpu_hnsw_quality() {
        let num_vectors = 1024;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;
        let groups_count = 4;
        let searches_count = 20;
        let top = 10;
        let min_cpu_linked_points_count = 64;

        let test = create_gpu_graph_test_data(num_vectors, dim, m, m0, ef, searches_count);
        let graph_layers_builder =
            build_gpu_graph(&test, groups_count, min_cpu_linked_points_count);

        check_graph_layers_builders_quality(graph_layers_builder, test, top, ef, 0.8)
    }
}
