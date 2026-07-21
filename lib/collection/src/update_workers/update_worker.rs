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
                    let mut retried_before = false;
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
                        let repeat_retry = retried_before;
                        retried_before = true;

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
                        } else if repeat_retry {
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::DeferredBehavior;
    use segment::data_types::vectors::only_default_vector;
    use segment::entry::entry_point::SegmentEntry as _;
    use segment::payload_json;
    use segment::types::{PayloadContainer, WithPayload};
    use shard::operations::OperationWithClockTag;
    use shard::retrieve::retrieve_blocking::retrieve_blocking;
    use shard::segment_holder::SegmentHolder;
    use shard::wal::{SerdeWal, WalRawRecord};
    use tempfile::Builder;
    use tokio::sync::{Mutex as TokioMutex, mpsc};
    use tokio::task::JoinHandle;
    use wal::WalOptions;

    use super::*;
    use crate::collection_manager::fixtures::{TEST_TIMEOUT, empty_segment};
    use crate::operations::payload_ops::{PayloadOps, SetPayloadOp};

    /// Fixture segments hold dim-4 f32 vectors: 16 bytes per point.
    const TEST_POINT_SIZE_BYTES: usize = 16;

    /// An operation that has to CoW-move `points` out of their non-appendable segment, which is
    /// what needs insert capacity.
    fn set_payload_op(points: &[u64], color: &str) -> CollectionUpdateOperations {
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
            payload: payload_json! {"color": color},
            points: Some(points.iter().map(|id| (*id).into()).collect()),
            filter: None,
            key: None,
        }))
    }

    /// Points 1 and 2 in a non-appendable segment, plus the only appendable segment already at the
    /// cap: any CoW move out of the first has nowhere to land.
    ///
    /// The points are seeded at version 0 so a replayed operation, whose op number is a WAL index,
    /// is not skipped as already applied.
    fn segments_at_capacity(segments_dir: &Path) -> LockedSegmentHolder {
        let hw_counter = HardwareCounterCell::new();
        let vector = only_default_vector(&[1.0, 0.0, 1.0, 1.0]);

        let mut non_appendable = empty_segment(segments_dir);
        for point_id in [1u64, 2] {
            non_appendable
                .upsert_point(0, point_id.into(), vector.clone(), &hw_counter)
                .unwrap();
        }
        non_appendable.appendable_flag = false;

        let mut appendable = empty_segment(segments_dir);
        for point_id in [100u64, 101] {
            appendable
                .upsert_point(0, point_id.into(), vector.clone(), &hw_counter)
                .unwrap();
        }

        let mut holder = SegmentHolder::default();
        holder.add_new(non_appendable);
        holder.add_new(appendable);
        holder.set_max_segment_size_bytes(NonZeroUsize::new(2 * TEST_POINT_SIZE_BYTES));
        LockedSegmentHolder::new(holder)
    }

    /// Start an update worker with a stand-in optimization worker.
    ///
    /// The stand-in provisions one fresh appendable segment on its first `Nop` when
    /// `provision_capacity` is set, mimicking the capacity-ensure step of a real wake-up. It
    /// reports how many it provisioned, which doubles as an assertion that the worker asked.
    fn spawn_worker(
        segments: LockedSegmentHolder,
        wal: SerdeWal<OperationWithClockTag>,
        shard_dir: &Path,
        segments_dir: &Path,
        provision_capacity: bool,
    ) -> (
        mpsc::Sender<UpdateSignal>,
        JoinHandle<Receiver<UpdateSignal>>,
        JoinHandle<usize>,
    ) {
        let (update_sender, update_receiver) = mpsc::channel(8);
        let (optimize_sender, mut optimize_receiver) = mpsc::channel(8);
        let (optimization_finished_sender, optimization_finished_receiver) = watch::channel(());

        let optimizer_segments = segments.clone();
        let optimizer_dir = segments_dir.to_owned();
        let optimizer = tokio::spawn(async move {
            let mut provisioned = 0;
            while let Some(signal) = optimize_receiver.recv().await {
                if matches!(signal, OptimizerSignal::Nop) && provision_capacity && provisioned == 0
                {
                    provisioned += 1;
                    let fresh = empty_segment(&optimizer_dir);
                    optimizer_segments.write().add_new(fresh);
                    let _ = optimization_finished_sender.send(());
                }
            }
            provisioned
        });

        let last_wal_index = wal.first_index() + wal.len(false);
        let worker = tokio::spawn(UpdateWorkers::update_worker_fn(
            "test_collection".to_string(),
            update_receiver,
            optimize_sender,
            Arc::new(TokioMutex::new(wal)),
            segments,
            Arc::new(tokio::sync::RwLock::new(())),
            UpdateTracker::default(),
            false,
            optimization_finished_receiver,
            Arc::new(AppliedSeqHandler::load_or_init(shard_dir, last_wal_index)),
            CancellationToken::new(),
        ));

        (update_sender, worker, optimizer)
    }

    async fn submit(
        update_sender: &mpsc::Sender<UpdateSignal>,
        op_num: SeqNumberType,
        operation: CollectionUpdateOperations,
    ) -> CollectionResult<InternalUpdateResult> {
        let (feedback_sender, feedback_receiver) = oneshot::channel();
        update_sender
            .send(UpdateSignal::Operation(OperationData {
                op_num,
                operation: Some(Box::new(operation)),
                sender: Some(feedback_sender),
                wait_for_deferred: false,
                hw_measurements: HwMeasurementAcc::disposable(),
            }))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(60), feedback_receiver)
            .await
            .expect("the worker must answer within the retry budget")
            .expect("the worker must not drop the feedback channel")
    }

    /// A capacity failure must not surface to the client: the worker signals the optimizer, waits
    /// for the fresh appendable segment and re-applies the operation, so the caller sees added
    /// latency instead of an error. The re-apply reads the operation back from the WAL rather than
    /// keeping a clone, so this also locks that the queued operation is readable at its op number.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_worker_retries_capacity_failure_inline() {
        let segments_dir = Builder::new().prefix("segments").tempdir().unwrap();
        let wal_dir = Builder::new().prefix("wal").tempdir().unwrap();
        let shard_dir = Builder::new().prefix("shard").tempdir().unwrap();

        let segments = segments_at_capacity(segments_dir.path());

        let mut wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(wal_dir.path(), WalOptions::default()).unwrap();
        // Filler so the operation under test lands above the seeded point version of 0.
        wal.write(
            &WalRawRecord::new(&OperationWithClockTag::new(
                set_payload_op(&[], "filler"),
                None,
            ))
            .unwrap(),
        )
        .unwrap();
        let operation = set_payload_op(&[1, 2], "blue");
        let op_num = wal
            .write(
                &WalRawRecord::new(&OperationWithClockTag::new(operation.clone(), None)).unwrap(),
            )
            .unwrap();
        assert!(op_num > 0, "the operation must outrank the seeded points");

        let (update_sender, worker, optimizer) = spawn_worker(
            segments.clone(),
            wal,
            shard_dir.path(),
            segments_dir.path(),
            true,
        );

        submit(&update_sender, op_num, operation)
            .await
            .expect("the inline retry must turn the capacity failure into a success");

        assert!(
            segments.read().failed_operation.is_empty(),
            "the successful re-apply must unpin the WAL acknowledge",
        );

        let is_stopped = AtomicBool::new(false);
        let records = retrieve_blocking(
            segments,
            &[1.into(), 2.into()],
            &WithPayload::from(true),
            &false.into(),
            TEST_TIMEOUT,
            &is_stopped,
            HwMeasurementAcc::new(),
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();
        assert_eq!(records.len(), 2, "both moved points must survive the retry");
        for record in records.values() {
            assert_eq!(
                record.payload.as_ref().and_then(|payload| payload
                    .get_value(&"color".parse().unwrap())
                    .first()
                    .cloned()),
                Some(&serde_json::json!("blue")),
                "the retry must actually apply the operation, not just report success",
            );
        }

        drop(update_sender);
        worker.await.unwrap();
        assert_eq!(
            optimizer.await.unwrap(),
            1,
            "the worker must have signalled the optimizer to provision capacity",
        );
    }

    /// When capacity never comes back, the worker stops waiting and reports the capacity error.
    /// The operation must then be queued in `failed_operation` so asynchronous recovery owns it,
    /// and stay pinned there so the WAL keeps it replayable.
    ///
    /// Takes one `CAPACITY_WAIT_PER_ROUND` to run: the round wait is what expires here, well
    /// before the overall retry budget, which is only a backstop for rounds that keep seeing
    /// capacity appear and still fail.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_worker_hands_over_to_recovery_when_capacity_never_appears() {
        let segments_dir = Builder::new().prefix("segments").tempdir().unwrap();
        let wal_dir = Builder::new().prefix("wal").tempdir().unwrap();
        let shard_dir = Builder::new().prefix("shard").tempdir().unwrap();

        let segments = segments_at_capacity(segments_dir.path());

        let mut wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(wal_dir.path(), WalOptions::default()).unwrap();
        wal.write(
            &WalRawRecord::new(&OperationWithClockTag::new(
                set_payload_op(&[], "filler"),
                None,
            ))
            .unwrap(),
        )
        .unwrap();
        let operation = set_payload_op(&[1, 2], "blue");
        let op_num = wal
            .write(
                &WalRawRecord::new(&OperationWithClockTag::new(operation.clone(), None)).unwrap(),
            )
            .unwrap();

        // The stand-in optimizer never provisions, so capacity never returns.
        let (update_sender, worker, optimizer) = spawn_worker(
            segments.clone(),
            wal,
            shard_dir.path(),
            segments_dir.path(),
            false,
        );

        let err = submit(&update_sender, op_num, operation)
            .await
            .expect_err("without capacity the operation cannot succeed");
        assert!(
            err.is_out_of_appendable_capacity(),
            "expected the capacity error, got: {err}",
        );

        assert_eq!(
            segments
                .read()
                .failed_operation
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![op_num],
            "the operation must be queued for recovery and pin the WAL acknowledge",
        );

        drop(update_sender);
        worker.await.unwrap();
        optimizer.await.unwrap();
    }

    /// The retry re-reads the operation from WAL instead of keeping a clone on the hot path. If it
    /// is not there, the worker must report that rather than silently reporting success or hanging.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_worker_reports_unreadable_wal_record_on_retry() {
        let segments_dir = Builder::new().prefix("segments").tempdir().unwrap();
        let wal_dir = Builder::new().prefix("wal").tempdir().unwrap();
        let shard_dir = Builder::new().prefix("shard").tempdir().unwrap();

        let segments = segments_at_capacity(segments_dir.path());

        let mut wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(wal_dir.path(), WalOptions::default()).unwrap();
        wal.write(
            &WalRawRecord::new(&OperationWithClockTag::new(
                set_payload_op(&[], "filler"),
                None,
            ))
            .unwrap(),
        )
        .unwrap();
        // Past the end of the WAL: the first attempt uses the operation passed with the signal, and
        // only the retry goes looking for it on disk.
        let op_num = wal.first_index() + wal.len(false) + 5;

        let (update_sender, worker, optimizer) = spawn_worker(
            segments.clone(),
            wal,
            shard_dir.path(),
            segments_dir.path(),
            true,
        );

        let err = submit(&update_sender, op_num, set_payload_op(&[1, 2], "blue"))
            .await
            .expect_err("the retry cannot proceed without the operation");
        assert!(
            err.to_string().contains("could not be re-read from WAL"),
            "expected the re-read failure, got: {err}",
        );

        drop(update_sender);
        worker.await.unwrap();
        assert_eq!(
            optimizer.await.unwrap(),
            1,
            "the worker must have reached the retry, which is what re-reads the WAL",
        );
    }
}
