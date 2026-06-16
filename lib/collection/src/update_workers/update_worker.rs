use std::sync::Arc;
use std::time::Instant;

use cancel::CancellationToken;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::SeqNumberType;
use shard::operations::CollectionUpdateOperations;
use shard::segment_holder::locked::LockedSegmentHolder;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, watch};
use tokio_util::task::AbortOnDropHandle;

use crate::collection_manager::collection_updater::CollectionUpdater;
use crate::operations::generalizer::Generalizer;
use crate::operations::types::{CollectionError, CollectionResult, UpdateStatus};
use crate::profiling::interface::log_request_to_collector;
use crate::shards::CollectionId;
use crate::shards::update_tracker::UpdateTracker;
use crate::update_handler::{OperationData, OptimizerSignal, UpdateSignal};
use crate::update_workers::UpdateWorkers;
use crate::update_workers::applied_seq::AppliedSeqHandler;
use crate::update_workers::internal_update_result::InternalUpdateResult;
use crate::wal_delta::LockedWal;

/// Sends the operation result through the feedback channel if present.
/// Logs a debug message if the receiver is no longer waiting.
fn send_feedback(
    sender: Option<oneshot::Sender<CollectionResult<InternalUpdateResult>>>,
    result: CollectionResult<InternalUpdateResult>,
    op_num: SeqNumberType,
) {
    if let Some(feedback) = sender {
        feedback.send(result).unwrap_or_else(|_| {
            log::debug!("Can't report operation {op_num} result. Assume already not required");
        });
    }
}

impl UpdateWorkers {
    /// Main loop of the update worker.
    ///
    /// Returns the receiver when the worker is stopped.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_worker_fn(
        collection_name: CollectionId,
        mut receiver: Receiver<UpdateSignal>,
        optimize_sender: Sender<OptimizerSignal>,
        wal: LockedWal,
        segments: LockedSegmentHolder,
        update_operation_lock: Arc<tokio::sync::RwLock<()>>,
        update_tracker: UpdateTracker,
        prevent_unoptimized: bool,
        optimization_finished_receiver: watch::Receiver<()>,
        applied_seq_handler: Arc<AppliedSeqHandler>,
        cancel: CancellationToken,
    ) -> Receiver<UpdateSignal> {
        let receiver = loop {
            let signal = tokio::select! {
                biased; // biased to check cancellation first
                _ = cancel.cancelled() => {
                    break receiver;
                }
                signal = receiver.recv() => match signal {
                    Some(signal) => signal,
                    None => break receiver,
                }
            };

            match signal {
                UpdateSignal::Operation(OperationData {
                    op_num,
                    operation,
                    sender,
                    wait_for_deferred,
                    hw_measurements,
                }) => {
                    let collection_name_clone = collection_name.clone();
                    let wal_clone = wal.clone();
                    let update_operation_lock_clone = update_operation_lock.clone();
                    let update_tracker_clone = update_tracker.clone();

                    let operation = if let Some(operation) = operation {
                        *operation
                    } else {
                        let wal_clone = wal.clone();
                        let record = match tokio::task::spawn_blocking(move || {
                            wal_clone.blocking_lock().read_raw_record(op_num)
                        })
                        .await
                        {
                            Ok(record) => record,
                            Err(err) => {
                                log::error!("Can't read operation {op_num} from WAL - {err}");
                                send_feedback(sender, Err(CollectionError::from(err)), op_num);
                                continue;
                            }
                        };

                        match record {
                            Some(serialized_record) => match serialized_record.deserialize() {
                                Ok(deserialized) => deserialized.operation,
                                Err(err) => {
                                    log::error!("Can't read operation {op_num} from WAL - {err}");
                                    send_feedback(sender, Err(CollectionError::from(err)), op_num);
                                    continue;
                                }
                            },
                            None => {
                                send_feedback(
                                    sender,
                                    Err(CollectionError::service_error(format!(
                                        "Operation {op_num} not found in WAL"
                                    ))),
                                    op_num,
                                );
                                continue;
                            }
                        }
                    };

                    let wait = sender.is_some();
                    let segments_clone = segments.clone();
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

                    // Early return if operation failed
                    let _res = match res {
                        Ok(res) => res,
                        Err(update_err) => {
                            send_feedback(sender, Err(update_err), op_num);
                            continue;
                        }
                    };

                    if let Err(err) = applied_seq_handler.update(op_num) {
                        log::error!("Can't update last applied_seq {err}")
                    }

                    if wait_for_deferred && prevent_unoptimized {
                        if let Some(mut feedback) = sender {
                            // Detach the deferred-points wait so only the originating
                            // client waits — the update queue keeps draining.
                            let segments = segments.clone();
                            let optimize_sender = optimize_sender.clone();
                            let mut optimization_finished_receiver =
                                optimization_finished_receiver.clone();
                            let cancel = cancel.clone();
                            tokio::spawn(async move {
                                let status = match Self::wait_for_deferred_points_ready(
                                    &segments,
                                    &optimize_sender,
                                    &mut optimization_finished_receiver,
                                    &cancel,
                                    &mut feedback,
                                )
                                .await
                                {
                                    Ok(()) => UpdateStatus::Completed,
                                    Err(err) => {
                                        log::warn!("Failed to await for deferred points: {err}");
                                        UpdateStatus::WaitTimeout
                                    }
                                };
                                send_feedback(
                                    Some(feedback),
                                    Ok(InternalUpdateResult { op_num, status }),
                                    op_num,
                                );
                            });
                        }
                        // No sender: nobody is waiting, skip the deferred wait entirely.
                    } else {
                        send_feedback(
                            sender,
                            Ok(InternalUpdateResult {
                                op_num,
                                status: UpdateStatus::Completed,
                            }),
                            op_num,
                        );
                    }
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
        };

        // Transmitter was destroyed
        optimize_sender
            .send(OptimizerSignal::Stop)
            .await
            .unwrap_or_else(|_| log::debug!("Optimizer already stopped"));

        receiver
    }

    /// Wait until all deferred points are ready for read/search.
    ///
    /// Returns `Ok(())` when all deferred points have been optimized.
    ///
    /// Returns an error if the cancellation token is triggered (e.g. update
    /// handler restarted due to a config change via consensus), or if the
    /// caller is no longer waiting for the result (e.g. client timeout).
    ///
    /// # Cancel safety
    ///
    /// This function is cancel safe.
    async fn wait_for_deferred_points_ready(
        segments: &LockedSegmentHolder,
        optimize_sender: &Sender<OptimizerSignal>,
        optimization_finished_receiver: &mut watch::Receiver<()>,
        cancel: &CancellationToken,
        feedback_sender: &mut oneshot::Sender<CollectionResult<InternalUpdateResult>>,
    ) -> CollectionResult<()> {
        loop {
            let locked_segments = segments.clone();
            let has_deferred_points =
                AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
                    let segments = locked_segments.read();
                    segments.iter().any(|(_, segment)| {
                        let segment_guard = segment.get().read();
                        segment_guard.has_deferred_points()
                    })
                }))
                .await
                .map_err(CollectionError::from)?;

            // No deferred points, nothing to wait for.
            if !has_deferred_points {
                return Ok(());
            }

            // The only way to make deferred points visible is optimization.
            // Send Nop to re-trigger optimizers in case the previous signal was
            // consumed without launching an optimization.
            let _ = optimize_sender.try_send(OptimizerSignal::Nop);

            // Wait for the optimizer to check conditions or complete an optimization.
            // Also wake up if the update handler is restarted (e.g. config change via
            // consensus) or the caller's receiver is dropped (e.g. update_local's
            // outer timeout fired). Without the `closed()` branch, this select would
            // park forever under max_optimization_threads=0 (the optimizer skips
            // without notifying), leaking the detached task.
            log::debug!("waiting for optimization to allow updates");
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    log::debug!("wait_for_deferred_points_ready: update worker cancelled");
                    return Err(CollectionError::cancelled(
                        "Deferred points wait interrupted: update worker restarted"
                    ));
                }
                _ = feedback_sender.closed() => {
                    log::debug!("wait_for_deferred_points_ready: caller no longer waiting");
                    return Err(CollectionError::cancelled(
                        "Deferred points wait interrupted: caller timed out",
                    ));
                }
                result = optimization_finished_receiver.changed() => {
                    if let Err(err) = result {
                        log::warn!("wait_for_deferred_points_ready: optimization notifier closed: {err}");
                        return Err(CollectionError::cancelled(
                            "Deferred points wait interrupted: optimization worker stopped"
                        ));
                    }
                }
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

        let cpu_utilization = hw_measurements.cpu_utilization();

        let result = cpu_utilization.measure(|| {
            CollectionUpdater::update(
                &segments,
                op_num,
                operation,
                update_operation_lock.clone(),
                update_tracker.clone(),
                &hw_measurements.get_counter_cell(),
            )
        });

        let duration = start_time.elapsed();
        let cpu_ratio = cpu_utilization.ratio();
        let cpu_usage_ratio = if cpu_ratio > 0.0 {
            Some(cpu_ratio)
        } else {
            None
        };

        log_request_to_collector(&collection_name, duration, cpu_usage_ratio, move || {
            loggable_operation
        });

        result
    }
}
