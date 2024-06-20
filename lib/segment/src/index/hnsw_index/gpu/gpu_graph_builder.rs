use common::types::PointOffsetType;
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

    let batched_points = BatchedPoints::new(
        |point_id| reference_graph.get_point_level(point_id),
        ids,
        groups_count,
    )?;

    let graph_layers_builder =
        create_graph_layers_builder(&batched_points, num_vectors, m, m0, ef, entry_points_num)?;

    let mut gpu_search_context = GpuSearchContext::new(
        debug_messenger,
        groups_count,
        vector_storage,
        quantized_storage,
        m,
        m0,
        ef,
        num_vectors,
        force_half_precision,
    )?;

    for level in (0..batched_points.levels_count).rev() {
        let gpu_start_index = build_level_on_cpu(
            &pool,
            &graph_layers_builder,
            &batched_points,
            level,
            |i| i >= min_cpu_linked_points_count,
            &points_scorer_builder,
        )?;

        gpu_search_context.upload_links(level, &graph_layers_builder)?;
        build_level_on_gpu(
            &mut gpu_search_context,
            &batched_points,
            gpu_start_index,
            level,
        )?;
        gpu_search_context.download_links(level, &graph_layers_builder)?;
    }

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
