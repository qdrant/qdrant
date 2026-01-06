use std::sync::Arc;
use std::time::Instant;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::SeqNumberType;
use shard::operations::CollectionUpdateOperations;
use shard::segment_holder::LockedSegmentHolder;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;

use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::operations::generalizer::Generalizer;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::profiling::interface::log_request_to_collector;
use crate::shards::CollectionId;
use crate::shards::local_shard::indexed_only::get_largest_unindexed_segment_vector_size;
use crate::shards::update_tracker::UpdateTracker;
use crate::update_handler::{OperationData, OptimizerSignal, UpdateSignal};
use crate::update_workers::UpdateWorkers;
use crate::wal_delta::LockedWal;

impl UpdateWorkers {
    #[allow(clippy::too_many_arguments)]
    pub async fn update_worker_fn(
        collection_name: CollectionId,
        mut receiver: Receiver<UpdateSignal>,
        optimize_sender: Sender<OptimizerSignal>,
        wal: LockedWal,
        segments: LockedSegmentHolder,
        update_operation_lock: Arc<tokio::sync::RwLock<()>>,
        update_tracker: UpdateTracker,
        prevent_unoptimized_threshold: Option<usize>,
        mut optimization_finished_receiver: watch::Receiver<()>,
    ) {
        while let Some(signal) = receiver.recv().await {
            match signal {
                UpdateSignal::Operation(OperationData {
                    op_num,
                    operation,
                    sender,
                    wait,
                    hw_measurements,
                }) => {
                    let collection_name_clone = collection_name.clone();
                    let wal_clone = wal.clone();
                    let segments_clone = segments.clone();
                    let update_operation_lock_clone = update_operation_lock.clone();
                    let update_tracker_clone = update_tracker.clone();

                    let operation_result = Self::wait_for_optimization(
                        prevent_unoptimized_threshold,
                        &segments_clone,
                        &mut optimization_finished_receiver,
                    )
                    .await;

                    if let Err(err) = operation_result
                        && let Some(feedback) = sender
                    {
                        feedback.send(Err(err)).unwrap_or_else(|_| {
                            log::debug!("Can't report operation {op_num} result. Assume already not required");
                        });
                        continue;
                    };

                    let operation_result = tokio::task::spawn_blocking(move || {
                        Self::update_worker_internal(
                            collection_name_clone,
                            operation,
                            op_num,
                            wait,
                            wal_clone,
                            segments_clone,
                            update_operation_lock_clone,
                            update_tracker_clone,
                            hw_measurements,
                        )
                    })
                    .await;

                    let res = match operation_result {
                        Ok(Ok(update_res)) => optimize_sender
                            .send(OptimizerSignal::Operation(op_num))
                            .await
                            .and(Ok(update_res))
                            .map_err(|send_err| send_err.into()),
                        Ok(Err(err)) => Err(err),
                        Err(err) => Err(CollectionError::from(err)),
                    };

                    if let Some(feedback) = sender {
                        feedback.send(res).unwrap_or_else(|_| {
                            log::debug!("Can't report operation {op_num} result. Assume already not required");
                        });
                    };
                }
                UpdateSignal::Stop => {
                    optimize_sender
                        .send(OptimizerSignal::Stop)
                        .await
                        .unwrap_or_else(|_| log::debug!("Optimizer already stopped"));
                    break;
                }
                UpdateSignal::Nop => optimize_sender
                    .send(OptimizerSignal::Nop)
                    .await
                    .unwrap_or_else(|_| {
                        log::info!(
                            "Can't notify optimizers, assume process is dead. Restart is required"
                        );
                    }),
                UpdateSignal::Plunger(callback_sender) => {
                    callback_sender.send(()).unwrap_or_else(|_| {
                        log::debug!("Can't notify sender, assume nobody is waiting anymore");
                    });
                }
            }
        }
        // Transmitter was destroyed
        optimize_sender
            .send(OptimizerSignal::Stop)
            .await
            .unwrap_or_else(|_| log::debug!("Optimizer already stopped"));
    }

    /// Checks that unoptimized segments are small enough, so that we can effectively
    /// push more updates.
    ///
    /// Returns when all segments are smaller that the optimization_threshold.
    async fn wait_for_optimization(
        // Size of the unoptimized segment to be considered large enough for waiting.
        // If `None`, waiting is disabled.
        optimization_threshold: Option<usize>,
        segments: &LockedSegmentHolder,
        optimization_finished_receiver: &mut watch::Receiver<()>,
    ) -> CollectionResult<()> {
        let Some(optimization_threshold) = optimization_threshold else {
            // Waiting is disabled
            return Ok(());
        };

        loop {
            let locked_segments = segments.clone();
            let can_proceed = tokio::task::spawn_blocking(move || {
                let segments = locked_segments.read();
                let largest_unoptimized_segment_size_opt =
                    get_largest_unindexed_segment_vector_size(&segments);

                let largest_unoptimized_segment_size =
                    largest_unoptimized_segment_size_opt.unwrap_or(0);

                // True, if we can proceed with updates
                largest_unoptimized_segment_size <= optimization_threshold
            })
            .await
            .map_err(CollectionError::from)?;

            if can_proceed {
                return Ok(());
            }

            // ToDo: if there are no optimizations running, it can be a deadlock situation
            // We have to check validate that there are at least some optimizations

            // If unoptimized segments are too large, the only way it can be fixed is optimization
            // So we wait the notification of optimization completion to re-check the sizes
            if let Err(err) = optimization_finished_receiver.changed().await {
                // this can be if optimization is cancelled, we don't need to wait anymore
                log::debug!("Optimization thread terminated with an error: {err}");
                return Ok(());
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn update_worker_internal(
        collection_name: CollectionId,
        operation: CollectionUpdateOperations,
        op_num: SeqNumberType,
        wait: bool,
        wal: LockedWal,
        segments: LockedSegmentHolder,
        update_operation_lock: Arc<tokio::sync::RwLock<()>>,
        update_tracker: UpdateTracker,
        hw_measurements: HwMeasurementAcc,
    ) -> CollectionResult<usize> {
        // If wait flag is set, explicitly flush WAL first
        if wait {
            wal.blocking_lock().flush().map_err(|err| {
                CollectionError::service_error(format!(
                    "Can't flush WAL before operation {op_num} - {err}"
                ))
            })?;
        }

        let start_time = Instant::now();

        // This represents the operation without vectors and payloads for logging purposes
        // Do not use for anything else
        let loggable_operation = operation.remove_details();

        let result = CollectionUpdater::update(
            &segments,
            op_num,
            operation,
            update_operation_lock.clone(),
            update_tracker.clone(),
            &hw_measurements.get_counter_cell(),
        );

        let duration = start_time.elapsed();

        log_request_to_collector(&collection_name, duration, move || loggable_operation);

        result
    }
}
