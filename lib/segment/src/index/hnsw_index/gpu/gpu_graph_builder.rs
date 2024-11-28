use std::sync::atomic::AtomicBool;

use common::types::PointOffsetType;

use super::gpu_vector_storage::GpuVectorStorage;
use crate::common::check_stopped;
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::gpu::batched_points::BatchedPoints;
use crate::index::hnsw_index::gpu::create_graph_layers_builder;
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuInsertContext;
use crate::index::hnsw_index::gpu::gpu_level_builder::build_level_on_gpu;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::payload_storage::FilterContext;
use crate::vector_storage::RawScorer;

/// Maximum count of point IDs per visited flag.
static MAX_VISITED_FLAGS_FACTOR: usize = 32;

/// Build HNSW graph on GPU.
#[allow(clippy::too_many_arguments)]
pub fn build_hnsw_on_gpu<'a>(
    gpu_vector_storage: &GpuVectorStorage,
    // Graph with all settings like m, ef, levels, etc.
    reference_graph: &GraphLayersBuilder,
    // Parallel inserts count.
    groups_count: usize,
    // Number of entry points of hnsw graph.
    entry_points_num: usize,
    // Amount of first points to link on CPU.
    cpu_linked_points: usize,
    // If true, guarantee equality of result with CPU version for both single-threaded case.
    // Required for tests.
    exact: bool,
    // Point IDs to insert.
    // In payload blocks we need to use subset of all points.
    ids: Vec<PointOffsetType>,
    // Scorer builder for CPU build.
    points_scorer_builder: impl Fn(
            PointOffsetType,
        )
            -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
        + Send
        + Sync,
    stopped: &AtomicBool,
) -> OperationResult<GraphLayersBuilder> {
    let num_vectors = reference_graph.links_layers().len();
    let m = reference_graph.m();
    let m0 = reference_graph.m0();
    let ef = std::cmp::max(reference_graph.ef_construct(), m0);

    // Divide points into batches.
    // One batch is one shader invocation.
    let batched_points = BatchedPoints::new(
        |point_id| reference_graph.get_point_level(point_id),
        ids,
        groups_count,
    )?;

    // Create all GPU resources.
    let mut gpu_search_context = GpuInsertContext::new(
        gpu_vector_storage,
        groups_count,
        batched_points.remap(),
        m,
        m0,
        ef,
        exact,
        1..MAX_VISITED_FLAGS_FACTOR,
    )?;

    let graph_layers_builder =
        create_graph_layers_builder(&batched_points, num_vectors, m, m0, ef, entry_points_num);

    // Link first points on CPU.
    let mut cpu_linked_points_count = 0;
    for batch in batched_points.iter_batches(0) {
        for point in batch.points {
            check_stopped(stopped)?;
            let (raw_scorer, filter_context) = points_scorer_builder(point.point_id)?;
            let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());
            graph_layers_builder.link_new_point(point.point_id, points_scorer);
            raw_scorer.take_hardware_counter().discard_results();
            cpu_linked_points_count += 1;
            if cpu_linked_points_count >= cpu_linked_points {
                break;
            }
        }
        if cpu_linked_points_count >= cpu_linked_points {
            break;
        }
    }

    // Build all levels on GPU level by level.
    for level in (0..batched_points.levels_count()).rev() {
        log::debug!("Starting GPU level {}", level,);

        gpu_search_context.upload_links(level, &graph_layers_builder, stopped)?;
        build_level_on_gpu(
            &mut gpu_search_context,
            &batched_points,
            cpu_linked_points,
            level,
            stopped,
        )?;
        gpu_search_context.download_links(level, &graph_layers_builder, stopped)?;
    }

    gpu_search_context.log_measurements();

    Ok(graph_layers_builder)
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
    use crate::vector_storage::chunked_vector_storage::VectorOffsetType;

    fn build_gpu_graph(
        test: &GpuGraphTestData,
        groups_count: usize,
        cpu_linked_points_count: usize,
        exact: bool,
    ) -> GraphLayersBuilder {
        let num_vectors = test.graph_layers_builder.links_layers().len();
        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance = gpu::Instance::new(Some(&debug_messenger), None, false).unwrap();
        let device = gpu::Device::new(instance.clone(), &instance.physical_devices()[0]).unwrap();

        let gpu_vector_storage = GpuVectorStorage::new(
            device.clone(),
            test.vector_storage.borrow(),
            None,
            false,
            &false.into(),
        )
        .unwrap();

        let ids = (0..num_vectors as PointOffsetType).collect();
        build_hnsw_on_gpu(
            &gpu_vector_storage,
            &test.graph_layers_builder,
            groups_count,
            1,
            cpu_linked_points_count,
            exact,
            ids,
            |point_id| {
                let fake_filter_context = FakeFilterContext {};
                let added_vector = test
                    .vector_holder
                    .vectors
                    .get(point_id as VectorOffsetType)
                    .to_vec();
                let raw_scorer = test
                    .vector_holder
                    .get_raw_scorer(added_vector.clone())
                    .unwrap();
                Ok((raw_scorer, Some(Box::new(fake_filter_context))))
            },
            &false.into(),
        )
        .unwrap()
    }

    #[test]
    fn test_gpu_hnsw_equivalency() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let num_vectors = 1024;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;
        let min_cpu_linked_points_count = 64;

        let test = create_gpu_graph_test_data(num_vectors, dim, m, m0, ef, 0);
        let graph_layers_builder = build_gpu_graph(&test, 1, min_cpu_linked_points_count, true);

        compare_graph_layers_builders(&test.graph_layers_builder, &graph_layers_builder);
    }

    #[test]
    fn test_gpu_hnsw_quality_exact() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

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
            build_gpu_graph(&test, groups_count, min_cpu_linked_points_count, true);

        check_graph_layers_builders_quality(graph_layers_builder, test, top, ef, 0.8)
    }

    #[test]
    fn test_gpu_hnsw_quality() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

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
            build_gpu_graph(&test, groups_count, min_cpu_linked_points_count, false);

        check_graph_layers_builders_quality(graph_layers_builder, test, top, ef, 0.8)
    }
}
