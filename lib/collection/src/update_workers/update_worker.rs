use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// How long a single operation may hold the shard's update queue while retrying a capacity
/// failure. Wall-clock rather than a round count: rounds differ wildly in cost, and this worker
/// handles one operation at a time, so the budget is exactly what a single operation may hold the
/// queue for.
const CAPACITY_RETRY_BUDGET: Duration = Duration::from_secs(30);
/// Poll slice while waiting for the optimizer to provision capacity; also the backoff between
/// consecutive immediate re-applies.
const CAPACITY_WAIT_SLICE: Duration = Duration::from_millis(100);
/// How long one wake-and-wait round may wait for capacity to appear. If it expires, the retry
/// gives up and hands the operation to asynchronous recovery: an optimizer that produced nothing
/// for this long is unlikely to within the budget, which only backstops repeated rounds where
/// capacity keeps appearing and the re-apply still fails (an operation spanning many segments).
const CAPACITY_WAIT_PER_ROUND: Duration = Duration::from_secs(5);

/// Outcome of [`CapacityRetry::wait_for_optimizer`].
enum OptimizerWait {
    Satisfied,
    Cancelled,
    TimedOut,
}

/// Outcome of one [`wait_optimizer_round`].
enum OptimizerRound {
    /// The optimizer reported it finished a cycle (`optimization_finished` fired).
    Progressed,
    /// The wait slice elapsed without a signal (only possible when a slice is given); the caller
    /// re-checks its condition.
    SliceElapsed,
    /// The cancellation token fired.
    Cancelled,
    /// The optimizer's notifier closed, i.e. the optimization worker stopped.
    Stopped,
}

/// Re-signal the optimizer (`Nop`, in case the previous signal was consumed without launching an
/// optimization) and wait on `optimization_finished`: the one wake-and-wait shared by the
/// capacity retry and the deferred-points wait. Returns on progress, an elapsed `slice` (a
/// fallback re-check), cancellation, or a closed notifier; callers own the loop and predicate.
async fn wait_optimizer_round(
    optimize_sender: &Sender<OptimizerSignal>,
    optimization_finished: &mut watch::Receiver<()>,
    cancel: &CancellationToken,
    slice: Option<Duration>,
) -> OptimizerRound {
    let _ = optimize_sender.try_send(OptimizerSignal::Nop);
    let changed = optimization_finished.changed();
    match slice {
        Some(slice) => tokio::select! {
            biased; // biased to check cancellation first
            _ = cancel.cancelled() => OptimizerRound::Cancelled,
            result = tokio::time::timeout(slice, changed) => match result {
                Ok(Ok(())) => OptimizerRound::Progressed,
                Ok(Err(_)) => OptimizerRound::Stopped,
                Err(_elapsed) => OptimizerRound::SliceElapsed,
            },
        },
        None => tokio::select! {
            biased;
            _ = cancel.cancelled() => OptimizerRound::Cancelled,
            result = changed => match result {
                Ok(()) => OptimizerRound::Progressed,
                Err(_) => OptimizerRound::Stopped,
            },
        },
    }
}

/// The shared collaborators the update worker threads into applying one operation with capacity
/// retry.
struct CapacityRetry<'a> {
    collection_name: &'a CollectionId,
    wal: &'a LockedWal,
    segments: &'a LockedSegmentHolder,
    update_operation_lock: &'a Arc<tokio::sync::RwLock<()>>,
    update_tracker: &'a UpdateTracker,
    max_segment_size_bytes: Option<NonZeroUsize>,
    optimize_sender: &'a Sender<OptimizerSignal>,
    optimization_finished: &'a watch::Receiver<()>,
    cancel: &'a CancellationToken,
}

impl CapacityRetry<'_> {
    /// Apply `operation`, retrying inline on capacity failures: wake the optimizer (its wake-up
    /// provisions a fresh appendable segment) and re-apply once capacity is back, with
    /// already-applied points skipped by version as in WAL replay, so the caller sees latency
    /// instead of a transient failure. On cancellation or a spent [`CAPACITY_RETRY_BUDGET`] the
    /// capacity error is returned; the operation stays queued in `failed_operation` for
    /// asynchronous recovery.
    async fn run(
        &self,
        operation: CollectionUpdateOperations,
        op_num: SeqNumberType,
        wait: bool,
        hw_measurements: HwMeasurementAcc,
    ) -> Result<CollectionResult<usize>, tokio::task::JoinError> {
        let has_capacity = |segments: &LockedSegmentHolder| {
            segments
                .read()
                .has_appendable_segment_with_capacity(self.max_segment_size_bytes)
        };

        let mut operation = Some(operation);
        let mut retry_deadline = None;
        let mut retried_before = false;
        loop {
            // The first attempt consumes the operation; the rare retry rounds re-read it from WAL
            // instead of deep-cloning every operation on the hot path.
            let attempt_operation = match operation.take() {
                Some(operation) => operation,
                None => {
                    let wal_clone = self.wal.clone();
                    let read_result = tokio::task::spawn_blocking(move || {
                        wal_clone.blocking_lock().read_raw_record(op_num)
                    })
                    .await;
                    let reread = match read_result {
                        Ok(record) => record
                            .and_then(|record| record.deserialize().ok())
                            .map(|deserialized| deserialized.operation),
                        // A panicked or cancelled blocking task is not a missing record: surface
                        // the real failure as a transient error, so the call site still nudges
                        // the optimizer to recover the queued operation.
                        Err(join_error) => return Ok(Err(CollectionError::from(join_error))),
                    };
                    match reread {
                        Some(operation) => operation,
                        None => {
                            return Ok(Err(CollectionError::service_error(format!(
                                "Operation {op_num} could not be re-read from WAL for a capacity \
                                 retry"
                            ))));
                        }
                    }
                }
            };

            let result = tokio::task::spawn_blocking({
                let collection_name = self.collection_name.clone();
                let wal = self.wal.clone();
                let segments = self.segments.clone();
                let update_operation_lock = self.update_operation_lock.clone();
                let update_tracker = self.update_tracker.clone();
                let max_segment_size_bytes = self.max_segment_size_bytes;
                let hw_measurements = hw_measurements.clone();
                move || {
                    UpdateWorkers::update_worker_internal(
                        collection_name,
                        attempt_operation,
                        op_num,
                        wait,
                        wal,
                        segments,
                        update_operation_lock,
                        update_tracker,
                        max_segment_size_bytes,
                        hw_measurements,
                    )
                }
            })
            .await;

            let out_of_capacity =
                matches!(&result, Ok(Err(err)) if err.is_out_of_appendable_capacity());
            if !out_of_capacity {
                return result;
            }
            // Stop retrying once this worker is asked to stop: `wait_workers_stops` awaits this
            // task, so a retry in flight would hold up shard drops and the worker restart an
            // optimizer config update performs. The operation stays queued in `failed_operation`,
            // where recovery picks it up.
            if self.cancel.is_cancelled() {
                return result;
            }
            // The budget clock starts at the first capacity failure, so it covers the retries only
            // and not the initial apply.
            let deadline =
                *retry_deadline.get_or_insert_with(|| Instant::now() + CAPACITY_RETRY_BUDGET);
            if Instant::now() >= deadline {
                return result;
            }
            let repeat_retry = retried_before;
            retried_before = true;

            // Capacity may already be back: the optimizer wake-up triggered by a previous
            // operation provisions concurrently. Otherwise wake it up and wait for the fresh
            // segment.
            if !has_capacity(self.segments) {
                let round_deadline = deadline.min(Instant::now() + CAPACITY_WAIT_PER_ROUND);
                match self.wait_for_optimizer(round_deadline, has_capacity).await {
                    OptimizerWait::Satisfied => {}
                    OptimizerWait::Cancelled | OptimizerWait::TimedOut => return result,
                }
            } else if repeat_retry {
                // Consecutive immediate retries mean this wait predicate and the apply path keep
                // disagreeing (a segment can measure differently under momentary lock contention):
                // back off briefly instead of hot-looping full re-applications.
                tokio::time::sleep(CAPACITY_WAIT_SLICE).await;
            }
        }
    }

    /// Wait until `ready(segments)` holds, cancellation fires, or `deadline` passes, driving the
    /// optimizer via [`wait_optimizer_round`] each round. The slice timeout is a fallback re-check,
    /// since capacity can also appear from a concurrent operation's optimizer wake-up without an
    /// `optimization_finished` signal.
    async fn wait_for_optimizer(
        &self,
        deadline: Instant,
        ready: impl Fn(&LockedSegmentHolder) -> bool,
    ) -> OptimizerWait {
        let mut optimization_finished = self.optimization_finished.clone();
        loop {
            if ready(self.segments) {
                return OptimizerWait::Satisfied;
            }
            let now = Instant::now();
            if now >= deadline {
                return OptimizerWait::TimedOut;
            }
            let slice = CAPACITY_WAIT_SLICE.min(deadline - now);
            // Progressed / SliceElapsed / Stopped all just re-check the predicate and deadline: a
            // stopped optimizer cannot flip the predicate, so the deadline ends the wait.
            if let OptimizerRound::Cancelled = wait_optimizer_round(
                self.optimize_sender,
                &mut optimization_finished,
                self.cancel,
                Some(slice),
            )
            .await
            {
                return OptimizerWait::Cancelled;
            }
        }
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
        max_segment_size_bytes: Option<NonZeroUsize>,
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

                    // Apply the operation, retrying inline when every appendable segment reached
                    // the size cap so the caller sees added latency instead of a transient
                    // failure. See `CapacityRetry::run`.
                    let operation_result = CapacityRetry {
                        collection_name: &collection_name,
                        wal: &wal,
                        segments: &segments,
                        update_operation_lock: &update_operation_lock,
                        update_tracker: &update_tracker,
                        max_segment_size_bytes,
                        optimize_sender: &optimize_sender,
                        optimization_finished: &optimization_finished_receiver,
                        cancel: &cancel,
                    }
                    .run(operation, op_num, wait, hw_measurements)
                    .await;

                    let res = match operation_result {
                        Ok(Ok(update_res)) => optimize_sender
                            .send(OptimizerSignal::Operation(op_num))
                            .await
                            .and(Ok(update_res))
                            .map_err(|send_err| send_err.into()),
                        Ok(Err(err)) => {
                            // A queued failure (e.g. all appendable segments reached
                            // `max_segment_size`) sits in `failed_operation`. Wake the
                            // optimizer so its capacity-ensure step can provision a fresh
                            // appendable segment and `try_recover` re-applies the operation.
                            // `Nop` rather than `Operation`: it must run recovery even when
                            // optimization handles are maxed out.
                            if err.update_failure_kind().queues_for_recovery() {
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

            // The only way to make deferred points visible is optimization. Poke it and wait for
            // progress through the shared round, racing the caller giving up (its receiver
            // dropped, e.g. update_local's outer timeout fired). Without that race the wait could
            // park forever under max_optimization_threads=0 (the optimizer skips without
            // notifying), leaking the detached task.
            log::debug!("waiting for optimization to allow updates");
            tokio::select! {
                biased;
                _ = feedback_sender.closed() => {
                    log::debug!("wait_for_deferred_points_ready: caller no longer waiting");
                    return Err(CollectionError::cancelled(
                        "Deferred points wait interrupted: caller timed out",
                    ));
                }
                round = wait_optimizer_round(
                    optimize_sender,
                    optimization_finished_receiver,
                    cancel,
                    None,
                ) => match round {
                    OptimizerRound::Cancelled => {
                        log::debug!("wait_for_deferred_points_ready: update worker cancelled");
                        return Err(CollectionError::cancelled(
                            "Deferred points wait interrupted: update worker restarted",
                        ));
                    }
                    OptimizerRound::Stopped => {
                        log::warn!("wait_for_deferred_points_ready: optimization notifier closed");
                        return Err(CollectionError::cancelled(
                            "Deferred points wait interrupted: optimization worker stopped",
                        ));
                    }
                    // No slice is given, so `SliceElapsed` cannot occur; both loop and re-check.
                    OptimizerRound::Progressed | OptimizerRound::SliceElapsed => {}
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
        max_segment_size_bytes: Option<NonZeroUsize>,
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
                max_segment_size_bytes,
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
    use segment::types::{PayloadContainer, WithPayload};
    use shard::operations::OperationWithClockTag;
    use shard::retrieve::retrieve_blocking::retrieve_blocking;
    use shard::segment_holder::SegmentHolder;
    use shard::wal::SerdeWal;
    use tempfile::Builder;
    use tokio::sync::{Mutex as TokioMutex, mpsc};
    use tokio::task::JoinHandle;
    use wal::WalOptions;

    use super::*;
    use crate::collection_manager::fixtures::{
        TEST_TIMEOUT, empty_segment, set_payload_op, write_op,
    };

    /// Fixture segments hold dim-4 f32 vectors: 16 bytes per point.
    const TEST_POINT_SIZE_BYTES: usize = 16;

    /// A WAL holding a filler operation (so the operation under test outranks the seeded point
    /// version of 0) followed by the `set_payload` on points 1 and 2 that needs CoW capacity.
    fn wal_with_move_op(
        wal_dir: &Path,
    ) -> (
        SerdeWal<OperationWithClockTag>,
        CollectionUpdateOperations,
        SeqNumberType,
    ) {
        let mut wal = SerdeWal::new(wal_dir, WalOptions::default()).unwrap();
        write_op(&mut wal, &set_payload_op(&[], "filler"));
        let operation = set_payload_op(&[1, 2], "blue");
        let op_num = write_op(&mut wal, &operation);
        assert!(op_num > 0, "the operation must outrank the seeded points");
        (wal, operation, op_num)
    }

    /// Points 1 and 2 in a non-appendable segment, plus the only appendable segment already at
    /// the cap: a CoW move has nowhere to land. Points are seeded at version 0 so replayed
    /// operations (op number = WAL index) are not skipped as already applied.
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
        LockedSegmentHolder::new(holder)
    }

    /// The cap under which the [`segments_at_capacity`] appendable segment is already full.
    fn test_cap() -> Option<NonZeroUsize> {
        NonZeroUsize::new(2 * TEST_POINT_SIZE_BYTES)
    }

    /// Start an update worker with a stand-in optimization worker, which provisions one fresh
    /// appendable segment on its first `Nop` when `provision_capacity` is set (mimicking the
    /// capacity-ensure step) and reports how many it provisioned, doubling as an assertion that
    /// the worker asked.
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
        CancellationToken,
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
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(UpdateWorkers::update_worker_fn(
            "test_collection".to_string(),
            update_receiver,
            optimize_sender,
            Arc::new(TokioMutex::new(wal)),
            segments,
            Arc::new(tokio::sync::RwLock::new(())),
            UpdateTracker::default(),
            false,
            test_cap(),
            optimization_finished_receiver,
            Arc::new(AppliedSeqHandler::load_or_init(shard_dir, last_wal_index)),
            cancel.clone(),
        ));

        (update_sender, worker, optimizer, cancel)
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
    /// keeping a clone, so this also verifies that the queued operation is readable at its op
    /// number.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_worker_retries_capacity_failure_inline() {
        let segments_dir = Builder::new().prefix("segments").tempdir().unwrap();
        let wal_dir = Builder::new().prefix("wal").tempdir().unwrap();
        let shard_dir = Builder::new().prefix("shard").tempdir().unwrap();

        let segments = segments_at_capacity(segments_dir.path());
        let (wal, operation, op_num) = wal_with_move_op(wal_dir.path());

        let (update_sender, worker, optimizer, _cancel) = spawn_worker(
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
        let (wal, operation, op_num) = wal_with_move_op(wal_dir.path());

        // The stand-in optimizer never provisions, so capacity never returns.
        let (update_sender, worker, optimizer, _cancel) = spawn_worker(
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

    /// A retry in flight must give up as soon as the worker is asked to stop: `wait_workers_stops`
    /// awaits this task, so holding on for the round wait would delay shard drops and the worker
    /// restart an optimizer config update performs. Fails without the cancellation checks, where
    /// the worker keeps waiting for capacity for a full `CAPACITY_WAIT_PER_ROUND`.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_worker_stops_retrying_when_cancelled() {
        let segments_dir = Builder::new().prefix("segments").tempdir().unwrap();
        let wal_dir = Builder::new().prefix("wal").tempdir().unwrap();
        let shard_dir = Builder::new().prefix("shard").tempdir().unwrap();

        let segments = segments_at_capacity(segments_dir.path());
        let (wal, operation, op_num) = wal_with_move_op(wal_dir.path());

        // The stand-in optimizer never provisions, so the worker settles into the retry wait.
        let (update_sender, worker, optimizer, cancel) = spawn_worker(
            segments.clone(),
            wal,
            shard_dir.path(),
            segments_dir.path(),
            false,
        );

        // Cancel once the first apply has failed and queued the operation, not after a fixed
        // delay: from that point the worker is provably past picking the operation up and into
        // the retry logic, so cancelling cannot race the outer receive loop and drop the
        // feedback channel instead.
        let queued = segments.clone();
        let cancel_after = cancel.clone();
        tokio::spawn(async move {
            loop {
                if !queued.read().failed_operation.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            cancel_after.cancel();
        });

        let started = Instant::now();
        let err = submit(&update_sender, op_num, operation)
            .await
            .expect_err("a cancelled worker cannot apply the operation");
        assert!(
            err.is_out_of_appendable_capacity(),
            "expected the capacity error, got: {err}",
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the worker must abandon the retry on cancellation, took {:?}",
            started.elapsed(),
        );

        assert_eq!(
            segments
                .read()
                .failed_operation
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![op_num],
            "the abandoned operation stays queued for recovery",
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
        write_op(&mut wal, &set_payload_op(&[], "filler"));
        // Past the end of the WAL: the first attempt uses the operation passed with the signal, and
        // only the retry goes looking for it on disk.
        let op_num = wal.first_index() + wal.len(false) + 5;

        let (update_sender, worker, optimizer, _cancel) = spawn_worker(
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
