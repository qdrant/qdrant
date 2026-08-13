use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::SINGLE_THREADED_HNSW_BUILD_THRESHOLD;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::condition_checker::ConditionCheckerEnum;
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
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::visited_pool::VisitedListHandle;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

#[allow(clippy::too_many_arguments)]
pub(super) fn build_main_graph_on_gpu(
    id_tracker: &IdTrackerEnum,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    gpu_vectors: Option<&GpuVectorStorage>,
    // Passed through only so a DEVICE_LOST hit during the actual graph dispatch below (as
    // opposed to during create_gpu_vectors' own device acquisition) can also trigger
    // recreate_if_device_lost() — see build_graph_on_gpu's own doc comment for why this call
    // site needed the same fix. Confirmed live 2026-08-11: this exact path (not
    // create_gpu_vectors) produced 3 of 6 real DEVICE_LOST occurrences observed in ~3 hours of
    // production monitoring after the create_gpu_vectors-only fix first shipped — a genuine,
    // roughly-50/50 gap, not a rare edge case.
    mut gpu_device: Option<&mut LockedGpuDevice>,
    graph_layers_builder: &GraphLayersBuilder,
    deleted_bitslice: &BitSlice,
    entry_points_num: usize,
    stopped: &AtomicBool,
) -> OperationResult<(Option<GraphLayersBuilder>, bool)> {
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

    let Some(gpu_vectors) = gpu_vectors else {
        return Ok((None, false));
    };

    // Was a bare `?` before (CodeRabbit review, PR #10213): GpuInsertContext::new() allocates
    // GPU buffers and runs initialization commands, which can hit DEVICE_LOST just as much as
    // the actual graph dispatch in build_graph_on_gpu() below — but `?` propagated that as a
    // hard error all the way up through build_vector_index(), skipping BOTH the graceful
    // CPU-fallback pattern every other GPU failure in this file uses, and device recreation
    // (recreate_if_device_lost() never got a chance to run), leaving the device dead for every
    // future caller too. Same fix shape as build_graph_on_gpu's own match below.
    let mut gpu_insert_context = match GpuInsertContext::new(
        gpu_vectors,
        get_gpu_groups_count(),
        graph_layers_builder.hnsw_m(),
        graph_layers_builder.ef_construct(),
        false,
        1..=GPU_MAX_VISITED_FLAGS_FACTOR,
    ) {
        Ok(gpu_insert_context) => gpu_insert_context,
        Err(err) => {
            log::warn!("Failed to create GPU insert context: {err}. Falling back to CPU.");
            let device_lost = if let Some(gpu_device) = gpu_device.as_deref_mut() {
                gpu_device.recreate_if_device_lost(&err)
            } else {
                false
            };
            return Ok((None, device_lost));
        }
    };

    build_graph_on_gpu(
        gpu_device,
        Some(&mut gpu_insert_context),
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
    // See build_main_graph_on_gpu's identical parameter for why this is here.
    gpu_device: Option<&mut LockedGpuDevice>,
    gpu_insert_context: Option<&mut GpuInsertContext<'_>>,
    graph_layers_builder: &GraphLayersBuilder,
    block_filter_list: &VisitedListHandle,
    points_to_index: &[PointOffsetType],
    stopped: &AtomicBool,
) -> OperationResult<(Option<GraphLayersBuilder>, bool)> {
    build_graph_on_gpu(
        gpu_device,
        gpu_insert_context,
        graph_layers_builder,
        points_to_index.iter().copied(),
        1,
        |block_point_id| -> OperationResult<_> {
            let hardware_counter = HardwareCounterCell::disposable();
            let block_condition_checker =
                OptimizedFilter::from_checker(ConditionCheckerEnum::Build(BuildConditionChecker {
                    filter_list: block_filter_list,
                    current_point: block_point_id,
                }));
            FilteredScorer::new_internal(
                block_point_id,
                vector_storage,
                quantized_vectors.as_ref(),
                Some(block_condition_checker),
                id_tracker.deleted_point_bitslice(),
                hardware_counter,
            )
        },
        stopped,
    )
}

/// Returns `(constructed graph if GPU succeeded, whether a DEVICE_LOST was hit)`. The second
/// field lets callers holding other GPU resources built earlier against the SAME device (e.g.
/// `GpuVectorStorage`/`GpuInsertContext` in `hnsw/build.rs`) know to discard them instead of
/// reusing them against what is now a different, freshly recreated device — see
/// `LockedGpuDevice::recreate_if_device_lost()`'s own doc comment for the production incident
/// this closes.
#[allow(clippy::too_many_arguments)]
fn build_graph_on_gpu<'a, 'b>(
    gpu_device: Option<&mut LockedGpuDevice>,
    gpu_insert_context: Option<&mut GpuInsertContext<'b>>,
    graph_layers_builder: &GraphLayersBuilder,
    points_to_index: impl Iterator<Item = PointOffsetType>,
    entry_points_num: usize,
    points_scorer_builder: impl Fn(PointOffsetType) -> OperationResult<FilteredScorer<'a>> + Send + Sync,
    stopped: &AtomicBool,
) -> OperationResult<(Option<GraphLayersBuilder>, bool)> {
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
            Ok(gpu_constructed_graph) => Ok((Some(gpu_constructed_graph), false)),
            Err(gpu_error) => {
                log::warn!("Failed to build HNSW on GPU: {gpu_error}. Falling back to CPU.");
                // Same DEVICE_LOST recreate-in-place logic as create_gpu_vectors() — confirmed
                // live 2026-08-11 this call site hits DEVICE_LOST just as often (see
                // build_main_graph_on_gpu's doc comment on the gpu_device parameter above).
                let device_lost = if let Some(gpu_device) = gpu_device {
                    gpu_device.recreate_if_device_lost(&gpu_error)
                } else {
                    false
                };
                Ok((None, device_lost))
            }
        }
    } else {
        Ok((None, false))
    }
}

/// Returns `(created storage if GPU succeeded, whether a DEVICE_LOST was hit)` — see
/// `build_graph_on_gpu`'s own doc comment for why the second field matters to callers.
pub(super) fn create_gpu_vectors(
    gpu_device: Option<&mut LockedGpuDevice>,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: &Option<QuantizedVectors>,
    stopped: &AtomicBool,
) -> OperationResult<(Option<GpuVectorStorage>, bool)> {
    use crate::index::hnsw_index::gpu::get_gpu_force_half_precision;
    if vector_storage.total_vector_count() < SINGLE_THREADED_HNSW_BUILD_THRESHOLD {
        return Ok((None, false));
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
            Ok(gpu_vectors) => Ok((Some(gpu_vectors), false)),
            Err(err) => {
                log::error!("Failed to create GPU vectors, use CPU instead. Error: {err}.");
                // If this specific error is DEVICE_LOST, the Vulkan device itself is now
                // permanently dead (per spec) and would otherwise stay dead in the shared pool
                // for the rest of the process's life — see recreate_if_device_lost()'s doc
                // comment. No-op for any other error (timeout, OOM, ...).
                let device_lost = gpu_device.recreate_if_device_lost(&err);
                Ok((None, device_lost))
            }
        }
    } else {
        Ok((None, false))
    }
}
