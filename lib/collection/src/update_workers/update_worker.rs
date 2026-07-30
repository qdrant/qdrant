use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use cancel::CancellationToken;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use segment::types::SeqNumberType;
use shard::operations::optimization::OptimizerThresholds;
use shard::operations::point_ops::PointOperations;
use shard::operations::vector_ops::VectorOperations;
use shard::operations::{CollectionUpdateOperations, payload_ops};
use shard::optimizers::config::SegmentOptimizerConfig;
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

/// Whether applying `operation` can need somewhere to put a point, and so is worth checking
/// capacity for beforehand.
///
/// Only operations reaching `apply_points_with_conditional_move` (or inserting outright) can be
/// given a destination, so deletes and schema-only operations skip the check entirely rather than
/// measuring every appendable segment for nothing. Deliberately matched exhaustively: a new
/// operation must be classified by whoever adds it, not silently default either way.
fn may_need_write_capacity(operation: &CollectionUpdateOperations) -> bool {
    match operation {
        CollectionUpdateOperations::PointOperation(operation) => match operation {
            PointOperations::UpsertPoints(_)
            | PointOperations::UpsertPointsConditional(_)
            | PointOperations::UpsertPointsRaw(_)
            | PointOperations::SyncPoints(_)
            | PointOperations::SyncPointsRaw(_) => true,
            // Deletes never move a point, they only mark it.
            PointOperations::DeletePoints { .. } | PointOperations::DeletePointsByFilter(_) => {
                false
            }
        },
        // Deleting a named vector from a point in an immutable segment moves the point too.
        CollectionUpdateOperations::VectorOperation(operation) => match operation {
            VectorOperations::UpdateVectors(_)
            | VectorOperations::DeleteVectors(..)
            | VectorOperations::DeleteVectorsByFilter(..) => true,
        },
        // Every payload operation copy-on-write moves the points it touches.
        CollectionUpdateOperations::PayloadOperation(operation) => match operation {
            payload_ops::PayloadOps::SetPayload(_)
            | payload_ops::PayloadOps::DeletePayload(_)
            | payload_ops::PayloadOps::ClearPayload { .. }
            | payload_ops::PayloadOps::ClearPayloadByFilter(_)
            | payload_ops::PayloadOps::OverwritePayload(_) => true,
        },
        // Schema-only, applied to every segment in place.
        CollectionUpdateOperations::FieldIndexOperation(_)
        | CollectionUpdateOperations::VectorNameOperation(_) => false,
        #[cfg(feature = "staging")]
        CollectionUpdateOperations::StagingOperation(_) => false,
    }
}

/// Provision a fresh appendable segment when every existing one reached `max_segment_size`, so the
/// operation about to be applied has a destination below the cap to move points into.
///
/// Without this, a filtered payload or vector operation copy-on-write moves points into whichever
/// appendable segment locks first, growing an already full segment without bound: the optimizer
/// provisions capacity only on its own wake-up, which is too late for the operation being applied
/// now.
///
/// Best effort by design: a provisioning failure is logged and the operation is applied anyway,
/// falling back to the pre-existing behaviour of writing into a full segment. Running out of
/// capacity must never fail a write. The optimization worker deliberately panics on the same
/// failure; here it must not, because a client write is in flight and losing capacity only costs
/// segment size, not correctness.
///
/// Blocking: builds a segment on disk, so this must be called from a blocking context.
fn ensure_capacity_for_update(
    segments: &LockedSegmentHolder,
    segments_path: &Path,
    segment_config: &SegmentOptimizerConfig,
    thresholds_config: &OptimizerThresholds,
    payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
) {
    let result = UpdateWorkers::ensure_appendable_segment_with_capacity(
        segments,
        segments_path,
        segment_config,
        thresholds_config,
        payload_index_schema,
    );

    if let Err(err) = result {
        log::error!(
            "Failed to provision an appendable segment with capacity, applying anyway: {err}"
        );
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
        // Capacity provisioning and the size cap both come from the first optimizer, like the
        // optimization worker does, so the two cannot disagree on what "full" means. Resolved once:
        // the optimizer set is fixed for this worker's lifetime, since a config update restarts the
        // workers. The optimization worker already logs and refuses to run without optimizers, so
        // do not repeat that error for every operation here.
        let capacity_optimizer = optimizers.first().cloned();
        debug_assert!(
            capacity_optimizer.is_some(),
            "No optimizers configured, appendable segment capacity will not be provisioned",
        );
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
                    // Classified before the operation is moved into the blocking task. Deletes and
                    // schema-only operations never need a destination, so they skip measuring
                    // every appendable segment.
                    let operation_capacity_optimizer = if may_need_write_capacity(&operation) {
                        capacity_optimizer.clone()
                    } else {
                        None
                    };
                    let payload_index_schema_clone = payload_index_schema.clone();
                    let operation_result = tokio::task::spawn_blocking(move || {
                        // Give the operation a destination below the size cap before applying it,
                        // rather than letting it grow an already full segment.
                        if let Some(optimizer) = operation_capacity_optimizer {
                            ensure_capacity_for_update(
                                &segments_clone,
                                optimizer.segments_path(),
                                optimizer.segment_optimizer_config(),
                                optimizer.threshold_config(),
                                payload_index_schema_clone,
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
    use common::save_on_disk::SaveOnDisk;
    use segment::types::{Distance, PayloadFieldSchema, PayloadSchemaType};
    use shard::fixtures::random_segment;
    use shard::operations::payload_ops::PayloadOps;
    use shard::operations::point_ops::{PointInsertOperationsInternal, PointOperations};
    use shard::operations::vector_ops::VectorOperations;
    use shard::optimizers::config::SegmentOptimizerConfig;
    use shard::segment_holder::SegmentHolder;
    use tempfile::Builder;

    use super::*;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::operations::{CreateIndex, FieldIndexOperations};
    use crate::optimizers_builder::build_segment_optimizer_config;

    /// Only operations that can be given a destination are worth measuring capacity for. Getting
    /// this wrong is silent either way: too narrow skips provisioning for an operation that needs
    /// it, too broad puts segment measurements back on every delete.
    #[test]
    fn test_may_need_write_capacity_classification() {
        let point_ids = vec![1.into()];

        // Moves or inserts points.
        assert!(may_need_write_capacity(
            &CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(vec![]),
            )),
        ));
        assert!(may_need_write_capacity(
            &CollectionUpdateOperations::PayloadOperation(PayloadOps::ClearPayload {
                points: point_ids.clone(),
            }),
        ));
        // Deleting a named vector moves the point out of an immutable segment too.
        assert!(may_need_write_capacity(
            &CollectionUpdateOperations::VectorOperation(VectorOperations::DeleteVectors(
                point_ids.clone().into(),
                vec!["dense".into()],
            )),
        ));

        // Marks points, never moves them.
        assert!(!may_need_write_capacity(
            &CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
                ids: point_ids,
            }),
        ));
        // Schema-only, applied to every segment in place.
        assert!(!may_need_write_capacity(
            &CollectionUpdateOperations::FieldIndexOperation(FieldIndexOperations::CreateIndex(
                CreateIndex {
                    field_name: "city".parse().unwrap(),
                    field_schema: Some(PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
                },
            )),
        ));
    }

    fn capacity_fixture(
        dir: &std::path::Path,
        max_segment_size_kb: usize,
    ) -> (
        SegmentOptimizerConfig,
        OptimizerThresholds,
        Arc<SaveOnDisk<PayloadIndexSchema>>,
    ) {
        let collection_params = crate::config::CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(256, Distance::Dot).build()),
            ..crate::config::CollectionParams::empty()
        };
        let segment_config = build_segment_optimizer_config(
            &collection_params,
            &Default::default(),
            &Default::default(),
        );
        let thresholds = OptimizerThresholds {
            max_segment_size_kb,
            memmap_threshold_kb: 1_000_000,
            indexing_threshold_kb: 1_000_000,
            deferred_internal_id: None,
        };
        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(dir.join("payload.schema")).unwrap());
        (segment_config, thresholds, payload_index_schema)
    }

    /// The pre-flight is what gives the destination filter something below the cap to pick. Without
    /// it the filter has nothing to prefer until the optimizer wakes up, which is too late for the
    /// operation being applied now.
    #[test]
    fn test_ensure_capacity_for_update_provisions_when_all_segments_are_full() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        // 1 KB is one vector of size 256, so the seeded segments are all over the cap.
        let (segment_config, thresholds, payload_index_schema) = capacity_fixture(dir.path(), 1);

        let mut holder = SegmentHolder::default();
        holder.add_new(random_segment(dir.path(), 100, 3, 256));
        holder.add_new(random_segment(dir.path(), 100, 3, 256));
        let segments = LockedSegmentHolder::new(holder);

        ensure_capacity_for_update(
            &segments,
            dir.path(),
            &segment_config,
            &thresholds,
            payload_index_schema.clone(),
        );
        assert_eq!(
            segments.read().len(),
            3,
            "a fresh appendable segment must be provisioned when every segment is at the cap",
        );

        // The fresh segment is empty, so the next operation finds capacity and provisions nothing.
        ensure_capacity_for_update(
            &segments,
            dir.path(),
            &segment_config,
            &thresholds,
            payload_index_schema,
        );
        assert_eq!(
            segments.read().len(),
            3,
            "a segment below the cap must not trigger another one",
        );
    }

    /// Uncapped means uncapped: no provisioning, whatever the segments measure.
    #[test]
    fn test_ensure_capacity_for_update_is_a_no_op_without_a_cap() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let (segment_config, thresholds, payload_index_schema) = capacity_fixture(dir.path(), 0);

        let mut holder = SegmentHolder::default();
        holder.add_new(random_segment(dir.path(), 100, 3, 256));
        let segments = LockedSegmentHolder::new(holder);

        ensure_capacity_for_update(
            &segments,
            dir.path(),
            &segment_config,
            &thresholds,
            payload_index_schema,
        );

        assert_eq!(segments.read().len(), 1, "no cap, nothing to provision");
    }
}
