use std::sync::atomic::{AtomicBool, Ordering};

use common::types::PointOffsetType;

use super::batched_points::{Batch, BatchedPoints};
use super::gpu_insert_context::GpuInsertContext;
use crate::common::check_stopped;
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuRequest;

// Build level on GPU
pub fn build_level_on_gpu(
    gpu_search_context: &mut GpuInsertContext,
    batched_points: &BatchedPoints,
    skip_count: usize,
    level: usize,
    stopped: &AtomicBool,
) -> OperationResult<()> {
    let mut prev_batch = None;

    for batch in batched_points.iter_batches(skip_count) {
        check_stopped(stopped)?;

        if level > batch.level {
            gpu_batched_update_entries(gpu_search_context, &batch, prev_batch.as_ref())?;
        } else {
            gpu_batched_insert(gpu_search_context, &batch, prev_batch.as_ref())?;
        }

        prev_batch = Some(batch);
    }

    if let Some(prev_batch) = prev_batch {
        let new_entries = gpu_search_context.download_responses(prev_batch.points.len())?;
        gpu_batched_apply_entries(&prev_batch, new_entries);
    }

    Ok(())
}

fn gpu_batched_update_entries(
    gpu_search_context: &mut GpuInsertContext,
    batch: &Batch,
    prev_batch: Option<&Batch>,
) -> OperationResult<()> {
    let mut requests = Vec::with_capacity(batch.points.len());
    for linking_point in batch.points {
        requests.push(GpuRequest {
            id: linking_point.point_id,
            entry: linking_point.entry.load(Ordering::Relaxed),
        })
    }

    let prev_batch_len = prev_batch
        .map(|prev_batch| prev_batch.points.len())
        .unwrap_or(0);

    let new_entries = gpu_search_context.greedy_search(&requests, prev_batch_len)?;

    if let Some(prev_batch) = prev_batch {
        gpu_batched_apply_entries(prev_batch, new_entries);
    }
    Ok(())
}

fn gpu_batched_insert(
    gpu_search_context: &mut GpuInsertContext,
    batch: &Batch,
    prev_batch: Option<&Batch>,
) -> OperationResult<()> {
    let mut requests = Vec::with_capacity(batch.points.len());
    for linking_point in batch.points {
        requests.push(GpuRequest {
            id: linking_point.point_id,
            entry: linking_point.entry.load(Ordering::Relaxed),
        })
    }

    let prev_batch_len = prev_batch
        .map(|prev_batch| prev_batch.points.len())
        .unwrap_or(0);

    let new_entries = gpu_search_context.run_insert_vector(&requests, prev_batch_len)?;

    if let Some(prev_batch) = prev_batch {
        gpu_batched_apply_entries(prev_batch, new_entries);
    }
    Ok(())
}

fn gpu_batched_apply_entries(batch: &Batch, new_entries: Vec<PointOffsetType>) {
    assert_eq!(batch.points.len(), new_entries.len());
    for (linking_point, new_entry) in batch.points.iter().zip(new_entries) {
        linking_point.entry.store(new_entry, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use common::types::PointOffsetType;
    use rstest::rstest;

    use super::*;
    use crate::index::hnsw_index::gpu::batched_points::BatchedPoints;
    use crate::index::hnsw_index::gpu::create_graph_layers_builder;
    use crate::index::hnsw_index::gpu::gpu_vector_storage::GpuVectorStorage;
    use crate::index::hnsw_index::gpu::tests::{
        check_graph_layers_builders_quality, compare_graph_layers_builders,
        create_gpu_graph_test_data, GpuGraphTestData,
    };
    use crate::index::hnsw_index::graph_layers::GraphLayersBase;
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;

    fn build_gpu_graph(
        test: &GpuGraphTestData,
        groups_count: usize,
        visited_flags_factor: usize,
    ) -> GraphLayersBuilder {
        let num_vectors = test.graph_layers_builder.links_layers().len();
        let m = test.graph_layers_builder.m();
        let m0 = test.graph_layers_builder.m0();
        let ef = test.graph_layers_builder.ef_construct();

        let batched_points = BatchedPoints::new(
            |point_id| test.graph_layers_builder.get_point_level(point_id),
            (0..num_vectors as PointOffsetType).collect(),
            groups_count,
        )
        .unwrap();

        let graph_layers_builder =
            create_graph_layers_builder(&batched_points, num_vectors, m, m0, ef, 1);

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

        let mut gpu_search_context = GpuInsertContext::new(
            &gpu_vector_storage,
            groups_count,
            batched_points.remap(),
            m,
            m0,
            ef,
            true,
            visited_flags_factor..32,
        )
        .unwrap();

        for level in (0..batched_points.levels_count()).rev() {
            let level_m = graph_layers_builder.get_m(level);
            gpu_search_context.clear(level_m).unwrap();

            build_level_on_gpu(
                &mut gpu_search_context,
                &batched_points,
                0,
                level,
                &false.into(),
            )
            .unwrap();

            gpu_search_context
                .download_links(level, &graph_layers_builder, &false.into())
                .unwrap();
        }

        graph_layers_builder
    }

    #[test]
    fn test_gpu_hnsw_level_equivalency() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let num_vectors = 1024;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;

        let test = create_gpu_graph_test_data(num_vectors, dim, m, m0, ef, 0);
        let graph_layers_builder = build_gpu_graph(&test, 1, 1);

        compare_graph_layers_builders(&test.graph_layers_builder, &graph_layers_builder);
    }

    #[rstest]
    fn test_gpu_hnsw_level_quality(#[values(1, 2)] visited_flags_factor: usize) {
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

        let test = create_gpu_graph_test_data(num_vectors, dim, m, m0, ef, searches_count);
        let graph_layers_builder = build_gpu_graph(&test, groups_count, visited_flags_factor);

        check_graph_layers_builders_quality(graph_layers_builder, test, top, ef, 0.8)
    }
}
