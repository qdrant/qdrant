use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use cancel::CancellationToken;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use segment::types::SeqNumberType;
use shard::operations::CollectionUpdateOperations;
use shard::payload_index_schema::PayloadIndexSchema;
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
use crate::update_handler::{OperationData, Optimizer, OptimizerSignal, UpdateSignal};
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
        optimizers: Arc<Vec<Arc<Optimizer>>>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        optimization_finished_receiver: watch::Receiver<()>,
        applied_seq_handler: Arc<AppliedSeqHandler>,
        cancel: CancellationToken,
    ) -> Receiver<UpdateSignal> {
        // Take thresholds from the first optimizer, like the optimization worker does.
        // Resolved once, a config update restarts the workers.
        let capacity_optimizer = optimizers.first().cloned();
        let max_segment_size_bytes = capacity_optimizer
            .as_ref()
            .and_then(|optimizer| optimizer.threshold_config().max_segment_size_bytes());

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
                    let capacity_optimizer_clone = capacity_optimizer.clone();
                    let payload_index_schema_clone = payload_index_schema.clone();
                    let operation_result = tokio::task::spawn_blocking(move || {
                        // Make sure a destination below the size cap exists before applying.
                        // Best effort, a failure here must not fail the write itself.
                        if let Some(optimizer) = capacity_optimizer_clone
                            && let Err(err) = Self::ensure_appendable_segment_with_capacity(
                                &segments_clone,
                                optimizer.segments_path(),
                                optimizer.segment_optimizer_config(),
                                optimizer.threshold_config(),
                                payload_index_schema_clone,
                            )
                        {
                            log::error!(
                                "Failed to provision appendable capacity, applying anyway: {err}"
                            );
                        }

                        Self::update_worker_internal(
                            collection_name_clone,
                            operation,
                            op_num,
                            wait,
                            wal_clone,
                            segments_clone,
                            update_operation_lock_clone,
                            update_tracker_clone,
                            max_segment_size_bytes,
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

                    // The in-memory bump is a pair of atomics, but the interval-triggered
                    // save fsyncs: keep only the save off the async worker.
                    if applied_seq_handler.update_in_memory(op_num) {
                        let applied_seq_handler = applied_seq_handler.clone();
                        if let Err(err) =
                            tokio::task::spawn_blocking(move || applied_seq_handler.save(op_num))
                                .await
                                .unwrap_or_else(|join_err| {
                                    Err(CollectionError::service_error(format!(
                                        "applied_seq save task panicked: {join_err}"
                                    )))
                                })
                        {
                            log::error!("Can't update last applied_seq {err}")
                        }
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
    use std::sync::Arc;

    use cancel::CancellationToken;
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use common::save_on_disk::SaveOnDisk;
    use segment::common::BYTES_IN_KB;
    use segment::entry::entry_point::SegmentEntry as _;
    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use segment::types::{
        Condition, Distance, FieldCondition, Filter, Match, MatchValue, ValueVariants,
    };
    use shard::fixtures::random_segment;
    use shard::operations::OperationWithClockTag;
    use shard::operations::payload_ops::{PayloadOps, SetPayloadOp};
    use shard::segment_holder::SegmentHolder;
    use shard::wal::SerdeWal;
    use tempfile::Builder;
    use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock, mpsc, oneshot, watch};
    use wal::WalOptions;

    use super::*;
    use crate::config::CollectionConfigInternal;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::optimizers_builder::build_optimizers;
    use crate::tests::fixtures::create_collection_config;

    /// Check that the update worker provisions a segment with capacity before applying, and
    /// applies with the size cap. The optimization worker is not running here, so the new
    /// segment can only come from the update worker itself.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_worker_provisions_capacity_before_applying() {
        let dir = Builder::new().prefix("shard").tempdir().unwrap();
        let hw_counter = common::counter::hardware_counter::HardwareCounterCell::new();

        // With 256-dim vectors a single point exceeds the 1 KB cap configured below.
        // The non-appendable segment holds the point the filtered update matches.
        const DIM: usize = 256;
        let mut holder = SegmentHolder::default();
        let full_id = holder.add_new(random_segment(dir.path(), 100, 3, DIM));
        let mut source = build_simple_segment(dir.path(), DIM, Distance::Dot).unwrap();
        source
            .upsert_point(
                1,
                100.into(),
                segment::data_types::vectors::only_default_vector(&vec![1.0; DIM]),
                &hw_counter,
            )
            .unwrap();
        source
            .set_payload(
                1,
                100.into(),
                &segment::payload_json! {"city": "Berlin".to_owned()},
                &None,
                &hw_counter,
            )
            .unwrap();
        source.appendable_flag = false;
        holder.add_new(source);
        let segments = LockedSegmentHolder::new(holder);
        assert_eq!(segments.read().len(), 2);

        let full_size = segments
            .read()
            .get(full_id)
            .unwrap()
            .get()
            .read()
            .max_available_vectors_size_in_bytes()
            .unwrap();
        assert!(
            full_size > BYTES_IN_KB,
            "Segment should exceed the 1 KB cap"
        );

        let mut config: CollectionConfigInternal = create_collection_config();
        config.params.vectors =
            VectorsConfig::Single(VectorParamsBuilder::new(DIM as u64, Distance::Dot).build());
        config.optimizer_config.max_segment_size = Some(1);
        let config = Arc::new(TokioRwLock::new(config));

        let optimizers = {
            let read = config.read().await;
            build_optimizers(
                dir.path(),
                config.clone(),
                &read.params,
                &read.optimizer_config,
                &read.hnsw_config,
                &Default::default(),
                &read.quantization_config,
            )
        };

        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(dir.path().join("payload.schema")).unwrap());
        let wal: SerdeWal<OperationWithClockTag> =
            SerdeWal::new(dir.path(), WalOptions::default()).unwrap();
        let wal = Arc::new(TokioMutex::new(wal));

        let (update_sender, update_receiver) = mpsc::channel(8);
        let (optimize_sender, _optimize_receiver) = mpsc::channel(8);
        let (_finished_sender, finished_receiver) = watch::channel(());
        let cancel = CancellationToken::new();

        let worker = tokio::spawn(UpdateWorkers::update_worker_fn(
            "test".to_string(),
            update_receiver,
            optimize_sender,
            wal,
            segments.clone(),
            Arc::new(TokioRwLock::new(())),
            UpdateTracker::default(),
            false,
            optimizers,
            payload_index_schema,
            finished_receiver,
            Arc::new(AppliedSeqHandler::load_or_init(dir.path(), 0)),
            cancel.clone(),
        ));

        let (feedback_sender, feedback_receiver) = oneshot::channel();
        update_sender
            .send(UpdateSignal::Operation(OperationData {
                op_num: 10,
                operation: Some(Box::new(CollectionUpdateOperations::PayloadOperation(
                    PayloadOps::SetPayload(SetPayloadOp {
                        payload: segment::payload_json! {"color": "red".to_owned()},
                        points: None,
                        filter: Some(Filter::new_must(Condition::Field(
                            FieldCondition::new_match(
                                "city".parse().unwrap(),
                                Match::Value(MatchValue {
                                    value: ValueVariants::String("Berlin".to_string()),
                                }),
                            ),
                        ))),
                        key: None,
                    }),
                ))),
                sender: Some(feedback_sender),
                wait_for_deferred: false,
                hw_measurements: HwMeasurementAcc::new(),
            }))
            .await
            .unwrap();

        feedback_receiver.await.unwrap().unwrap();

        assert_eq!(
            segments.read().len(),
            3,
            "Worker should provision a new appendable segment before applying",
        );
        assert!(
            !segments
                .read()
                .get(full_id)
                .unwrap()
                .get()
                .read()
                .has_point(100.into(), common::types::DeferredBehavior::WithDeferred),
            "Moved point should not land in the full segment",
        );

        cancel.cancel();
        let _ = worker.await;
    }
}
