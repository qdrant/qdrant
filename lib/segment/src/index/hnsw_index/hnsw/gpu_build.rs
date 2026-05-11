use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::cow::BoxCow;
use common::types::PointOffsetType;

use super::SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::hnsw_index::build_condition_checker::BuildConditionChecker;
use crate::index::hnsw_index::gpu::get_gpu_groups_count;
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::gpu::gpu_graph_builder::{
    GPU_MAX_VISITED_FLAGS_FACTOR, build_hnsw_on_gpu,
};
use crate::index::hnsw_index::gpu::gpu_insert_context::GpuInsertContext;
use crate::index::hnsw_index::gpu::gpu_vector_storage::GpuVectorStorage;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::visited_pool::VisitedListHandle;
use crate::payload_storage::FilterContext;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

#[allow(clippy::too_many_arguments)]
pub(super) fn build_main_graph_on_gpu(
    id_tracker: &IdTrackerEnum,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    gpu_vectors: Option<&GpuVectorStorage>,
    graph_layers_builder: &GraphLayersBuilder,
    deleted_bitslice: &BitSlice,
    entry_points_num: usize,
    stopped: &AtomicBool,
) -> OperationResult<Option<GraphLayersBuilder>> {
    let points_scorer_builder = |vector_id| {
        let hardware_counter = HardwareCounterCell::disposable();
        FilteredScorer::new_internal(
            vector_id,
            vector_storage,
            quantized_vectors.as_ref(),
            None,
            id_tracker.deleted_point_bitslice(),
            hardware_counter,
        )
    };

    let mut gpu_insert_context = if let Some(gpu_vectors) = gpu_vectors {
        Some(GpuInsertContext::new(
            gpu_vectors,
            get_gpu_groups_count(),
            graph_layers_builder.hnsw_m(),
            graph_layers_builder.ef_construct(),
            false,
            1..=GPU_MAX_VISITED_FLAGS_FACTOR,
        )?)
    } else {
        None
    };

    build_graph_on_gpu(
        gpu_insert_context.as_mut(),
        graph_layers_builder,
        id_tracker
            .point_mappings()
            .iter_internal_excluding(deleted_bitslice),
        entry_points_num,
        points_scorer_builder,
        stopped,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_filtered_graph_on_gpu(
    id_tracker: &IdTrackerEnum,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    gpu_insert_context: Option<&mut GpuInsertContext<'_>>,
    graph_layers_builder: &GraphLayersBuilder,
    block_filter_list: &VisitedListHandle,
    points_to_index: &[PointOffsetType],
    stopped: &AtomicBool,
) -> OperationResult<Option<GraphLayersBuilder>> {
    build_graph_on_gpu(
        gpu_insert_context,
        graph_layers_builder,
        points_to_index.iter().copied(),
        1,
        |block_point_id| -> OperationResult<_> {
            let hardware_counter = HardwareCounterCell::disposable();
            let block_condition_checker: Box<dyn FilterContext> = Box::new(BuildConditionChecker {
                filter_list: block_filter_list,
                current_point: block_point_id,
            });
            FilteredScorer::new_internal(
                block_point_id,
                vector_storage,
                quantized_vectors.as_ref(),
                Some(BoxCow::Owned(block_condition_checker)),
                id_tracker.deleted_point_bitslice(),
                hardware_counter,
            )
        },
        stopped,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_graph_on_gpu<'a, 'b>(
    gpu_insert_context: Option<&mut GpuInsertContext<'b>>,
    graph_layers_builder: &GraphLayersBuilder,
    points_to_index: impl Iterator<Item = PointOffsetType>,
    entry_points_num: usize,
    points_scorer_builder: impl Fn(PointOffsetType) -> OperationResult<FilteredScorer<'a>> + Send + Sync,
    stopped: &AtomicBool,
) -> OperationResult<Option<GraphLayersBuilder>> {
    if let Some(gpu_insert_context) = gpu_insert_context {
        let gpu_constructed_graph = build_hnsw_on_gpu(
            gpu_insert_context,
            graph_layers_builder,
            get_gpu_groups_count(),
            entry_points_num,
            SINGLE_THREADED_HNSW_BUILD_THRESHOLD,
            points_to_index.collect::<Vec<_>>(),
            points_scorer_builder,
            stopped,
        );

        // GPU construction does not return an error. If it fails, it will fall back to CPU.
        // To cover stopping case, we need to check stopping flag here.
        check_process_stopped(stopped)?;

        match gpu_constructed_graph {
            Ok(gpu_constructed_graph) => Ok(Some(gpu_constructed_graph)),
            Err(gpu_error) => {
                log::warn!("Failed to build HNSW on GPU: {gpu_error}. Falling back to CPU.");
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}

pub(super) fn create_gpu_vectors(
    gpu_device: Option<&LockedGpuDevice>,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    stopped: &AtomicBool,
) -> OperationResult<Option<GpuVectorStorage>> {
    use crate::index::hnsw_index::gpu::get_gpu_force_half_precision;
    if vector_storage.total_vector_count() < SINGLE_THREADED_HNSW_BUILD_THRESHOLD {
        return Ok(None);
    }

    if let Some(gpu_device) = gpu_device {
        let gpu_vectors = GpuVectorStorage::new(
            gpu_device.device(),
            vector_storage,
            quantized_vectors.as_ref(),
            get_gpu_force_half_precision(),
            stopped,
        );

        // GPU construction does not return an error. If it fails, it will fall back to CPU.
        // To cover stopping case, we need to check stopping flag here.
        check_process_stopped(stopped)?;

        match gpu_vectors {
            Ok(gpu_vectors) => Ok(Some(gpu_vectors)),
            Err(err) => {
                log::error!("Failed to create GPU vectors, use CPU instead. Error: {err}.");
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}
