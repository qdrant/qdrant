use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use common::types::PointOffsetType;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPool;

use super::batched_points::{BatchedPoints, PointLinkingData};
use super::gpu_search_context::GpuRequest;
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::graph_layers::GraphLayersBase;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::payload_storage::FilterContext;
use crate::vector_storage::RawScorer;

// Build level on CPU. Returns amount of processed points.
pub fn build_level_on_cpu<'a>(
    pool: &ThreadPool,
    graph_layers_builder: &GraphLayersBuilder,
    batched_points: &BatchedPoints,
    level: usize,
    stop_condition: impl Fn(usize) -> bool + Send + Sync,
    points_scorer_builder: impl Fn(
            PointOffsetType,
        )
            -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
        + Send
        + Sync,
) -> OperationResult<usize> {
    let retry_mutex: Mutex<()> = Default::default();
    let index = AtomicUsize::new(0);

    pool.install(|| {
        (0..graph_layers_builder.links_layers.len())
            .into_par_iter()
            .try_for_each(|_| -> OperationResult<()> {
                if stop_condition(index.load(Ordering::Relaxed)) {
                    return Ok(());
                }

                let i = index.fetch_add(1, Ordering::Relaxed);
                if i >= batched_points.points.len() {
                    return Ok(());
                }
                let linking_point = &batched_points.points[i];

                if graph_layers_builder.get_point_level(linking_point.point_id) < level {
                    update_entry_on_cpu(
                        graph_layers_builder,
                        linking_point,
                        level,
                        &points_scorer_builder,
                    )
                } else {
                    link_point_on_cpu(
                        &retry_mutex,
                        graph_layers_builder,
                        batched_points,
                        linking_point,
                        level,
                        &points_scorer_builder,
                    )
                }
            })
    })?;

    let processed_count = index
        .load(Ordering::Relaxed)
        .min(batched_points.points.len());
    Ok(processed_count)
}

fn update_entry_on_cpu<'a>(
    graph_layers_builder: &GraphLayersBuilder,
    linking_point: &PointLinkingData,
    level: usize,
    points_scorer_builder: impl Fn(
            PointOffsetType,
        )
            -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
        + Send
        + Sync,
) -> OperationResult<()> {
    let (raw_scorer, filter_context) = points_scorer_builder(linking_point.point_id)?;
    let mut points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());
    let new_entry = graph_layers_builder
        .search_entry_on_level(
            linking_point.entry.load(Ordering::Relaxed),
            level,
            &mut points_scorer,
        )
        .idx;
    linking_point.entry.store(new_entry, Ordering::Relaxed);
    Ok(())
}

fn link_point_on_cpu<'a>(
    retry_mutex: &Mutex<()>,
    graph_layers_builder: &GraphLayersBuilder,
    batched_points: &BatchedPoints,
    linking_point: &PointLinkingData,
    level: usize,
    points_scorer_builder: impl Fn(
            PointOffsetType,
        )
            -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
        + Send
        + Sync,
) -> OperationResult<()> {
    let (raw_scorer, filter_context) = points_scorer_builder(linking_point.point_id)?;
    let mut points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());
    let (patches, new_entries) = graph_layers_builder.get_patch(
        GpuRequest {
            id: linking_point.point_id,
            entry: linking_point.entry.load(Ordering::Relaxed),
        },
        level,
        &mut points_scorer,
    );

    let new_entries = if !graph_layers_builder.try_apply_patch(level, patches) {
        let _retry_guard = retry_mutex.lock().unwrap();

        // try again in single-thread mode
        let (patches, new_entries) = graph_layers_builder.get_patch(
            GpuRequest {
                id: linking_point.point_id,
                entry: linking_point.entry.load(Ordering::Relaxed),
            },
            level,
            &mut points_scorer,
        );
        graph_layers_builder.apply_patch(level, patches);
        new_entries
    } else {
        new_entries
    };

    new_entries
        .into_iter()
        .filter(|&new_entry| !batched_points.is_same_batch(linking_point, new_entry))
        .next()
        .map(|new_entry| {
            linking_point.entry.store(new_entry, Ordering::Relaxed);
        });

    Ok(())
}

#[cfg(test)]
mod tests {
    use common::types::PointOffsetType;

    use super::*;
    use crate::fixtures::index_fixtures::FakeFilterContext;
    use crate::index::hnsw_index::gpu::batched_points::BatchedPoints;
    use crate::index::hnsw_index::gpu::create_graph_layers_builder;
    use crate::index::hnsw_index::gpu::tests::{
        check_graph_layers_builders_quality, compare_graph_layers_builders,
        create_gpu_graph_test_data, GpuGraphTestData,
    };

    fn build_cpu_graph(test: &GpuGraphTestData, threads: usize) -> GraphLayersBuilder {
        let num_vectors = test.graph_layers_builder.links_layers.len();
        let m = test.graph_layers_builder.m;
        let m0 = test.graph_layers_builder.m0;
        let ef = test.graph_layers_builder.ef_construct;

        let batched_points = BatchedPoints::new(
            |point_id| test.graph_layers_builder.get_point_level(point_id),
            (0..num_vectors as PointOffsetType).collect(),
            threads,
        )
        .unwrap();

        let graph_layers_builder =
            create_graph_layers_builder(&batched_points, num_vectors, m, m0, ef, 1).unwrap();

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(threads)
            .build()
            .unwrap();

        for level in (0..batched_points.levels_count).rev() {
            build_level_on_cpu(
                &pool,
                &graph_layers_builder,
                &batched_points,
                level,
                |_| false,
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
            .unwrap();
        }

        graph_layers_builder
    }

    #[test]
    fn test_cpu_hnsw_level_equivalency() {
        let num_vectors = 1024;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;

        let test = create_gpu_graph_test_data(num_vectors, dim, m, m0, ef, 0);
        let graph_layers_builder = build_cpu_graph(&test, 1);

        compare_graph_layers_builders(&test.graph_layers_builder, &graph_layers_builder);
    }

    #[test]
    fn test_cpu_hnsw_level_quality() {
        let num_vectors = 1024;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;
        let threads = 4;
        let searches_count = 10;
        let top = 10;

        let test = create_gpu_graph_test_data(num_vectors, dim, m, m0, ef, searches_count);
        let graph_layers_builder = build_cpu_graph(&test, threads);

        check_graph_layers_builders_quality(graph_layers_builder, test, top, ef, 0.9)
    }
}
