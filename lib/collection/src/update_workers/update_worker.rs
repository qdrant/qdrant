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

                    // Apply the operation, retrying inline when it failed because every
                    // appendable segment reached the size cap: wake the optimizer (its wake-up
                    // provisions a fresh appendable segment) and re-apply once capacity is
                    // back. Re-applying is safe: points already applied are skipped by their
                    // version, exactly as in WAL replay. The caller thus sees added latency
                    // instead of a transient failure. Should capacity not reappear in time,
                    // fall through with the error: the operation is queued in
                    // `failed_operation` and asynchronous recovery re-applies it later.
                    //
                    // Retries are bounded by a wall-clock budget rather than a round count:
                    // rounds differ wildly in cost, and this worker handles one operation at a
                    // time, so the budget is exactly what a single operation may hold the
                    // shard's update queue for.
                    const CAPACITY_RETRY_BUDGET: std::time::Duration =
                        std::time::Duration::from_secs(30);
                    const CAPACITY_WAIT_SLICE: std::time::Duration =
                        std::time::Duration::from_millis(100);
                    const CAPACITY_WAIT_PER_ROUND: std::time::Duration =
                        std::time::Duration::from_secs(5);

                    let mut operation = Some(operation);
                    let mut retry_deadline = None;
                    let mut capacity_retries = 0;
                    let operation_result = loop {
                        // The first attempt consumes the operation; the rare retry rounds
                        // re-read it from WAL instead of deep-cloning every operation on the
                        // hot path.
                        let attempt_operation = match operation.take() {
                            Some(operation) => operation,
                            None => {
                                let wal_clone = wal.clone();
                                let reread = tokio::task::spawn_blocking(move || {
                                    wal_clone.blocking_lock().read_raw_record(op_num)
                                })
                                .await
                                .ok()
                                .flatten()
                                .and_then(|record| record.deserialize().ok())
                                .map(|deserialized| deserialized.operation);
                                match reread {
                                    Some(operation) => operation,
                                    None => {
                                        break Ok(Err(CollectionError::service_error(format!(
                                            "Operation {op_num} could not be re-read from WAL \
                                             for a capacity retry"
                                        ))));
                                    }
                                }
                            }
                        };

                        let collection_name_clone = collection_name.clone();
                        let wal_clone = wal.clone();
                        let segments_clone = segments.clone();
                        let update_operation_lock_clone = update_operation_lock.clone();
                        let update_tracker_clone = update_tracker.clone();
                        let hw_measurements_clone = hw_measurements.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            Self::update_worker_internal(
                                collection_name_clone,
                                attempt_operation,
                                op_num,
                                wait,
                                wal_clone,
                                segments_clone,
                                update_operation_lock_clone,
                                update_tracker_clone,
                                hw_measurements_clone,
                            )
                        })
                        .await;

                        let out_of_capacity =
                            matches!(&result, Ok(Err(err)) if err.is_out_of_appendable_capacity());
                        if !out_of_capacity {
                            break result;
                        }
                        // The budget clock starts at the first capacity failure, so it covers
                        // the retries only and not the initial apply.
                        let deadline = *retry_deadline
                            .get_or_insert_with(|| Instant::now() + CAPACITY_RETRY_BUDGET);
                        if Instant::now() >= deadline {
                            break result;
                        }
                        capacity_retries += 1;

                        // Capacity may already be back: the optimizer wake-up triggered by a
                        // previous operation provisions concurrently. Otherwise wake it up and
                        // wait for the fresh segment.
                        let has_capacity = |segments: &LockedSegmentHolder| {
                            let segments_read = segments.read();
                            let cap = segments_read.max_segment_size_bytes();
                            segments_read.has_appendable_segment_with_capacity(cap)
                        };
                        if !has_capacity(&segments) {
                            let _ = optimize_sender.send(OptimizerSignal::Nop).await;

                            // Wake early on the optimizer's `optimization_finished` signal;
                            // the timeout slice doubles as a fallback re-check, since not
                            // every wake-up path fires the signal promptly.
                            let mut optimization_finished = optimization_finished_receiver.clone();
                            let round_deadline =
                                deadline.min(Instant::now() + CAPACITY_WAIT_PER_ROUND);
                            let capacity_appeared = loop {
                                if has_capacity(&segments) {
                                    break true;
                                }
                                let now = Instant::now();
                                if now >= round_deadline {
                                    break false;
                                }
                                let slice = CAPACITY_WAIT_SLICE.min(round_deadline - now);
                                let _ =
                                    tokio::time::timeout(slice, optimization_finished.changed())
                                        .await;
                            };
                            if !capacity_appeared {
                                break result;
                            }
                        } else if capacity_retries > 1 {
                            // Consecutive immediate retries mean this wait predicate and the
                            // apply path keep disagreeing (a segment can measure differently
                            // under momentary lock contention): back off briefly instead of
                            // hot-looping full re-applications.
                            tokio::time::sleep(CAPACITY_WAIT_SLICE).await;
                        }
                    };

                    let res = match operation_result {
                        Ok(Ok(update_res)) => optimize_sender
                            .send(OptimizerSignal::Operation(op_num))
                            .await
                            .and(Ok(update_res))
                            .map_err(|send_err| send_err.into()),
                        Ok(Err(err)) => {
                            // A transient failure (e.g. all appendable segments reached
                            // `max_segment_size`) was queued in `failed_operation`. Wake the
                            // optimizer so its capacity-ensure step can provision a fresh
                            // appendable segment and `try_recover` re-applies the operation.
                            // `Nop` rather than `Operation`: it must run recovery even when
                            // optimization handles are maxed out.
                            if err.is_transient() {
                                let _ = optimize_sender.send(OptimizerSignal::Nop).await;
                            }
                            Err(err)
                        }
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
