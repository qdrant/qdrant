use std::sync::Arc;
use std::time::Instant;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::SeqNumberType;
use shard::operations::CollectionUpdateOperations;
use shard::segment_holder::LockedSegmentHolder;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::operations::generalizer::Generalizer;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::profiling::interface::log_request_to_collector;
use crate::shards::CollectionId;
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
