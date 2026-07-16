use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use common::save_on_disk::SaveOnDisk;
use common::types::PointOffsetType;
use parking_lot::RwLockUpgradableReadGuard;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::SegmentConfig;

use crate::locked_segment::LockedSegment;
use crate::optimizers::segment_optimizer::Optimizer;
use crate::payload_index_schema::PayloadIndexSchema;
use crate::segment_holder::SegmentHolder;
use crate::segment_holder::locked::LockedSegmentHolder;

/// Backstop against an update that keeps provisioning segments without terminating. Generous on
/// purpose: a single operation legitimately provisions one segment per `max_segment_size` worth
/// of moved data, and the emptiness check in
/// [`LockedSegmentHolder::update_with_segment_provisioning`] already catches non-progress.
const MAX_PROVISIONING_ATTEMPTS_PER_OPERATION: usize = 1024;

/// Everything needed to create a fresh appendable segment on demand while applying an update.
///
/// Sourced from the shard's optimizer configuration (see
/// [`SegmentProvisioning::from_optimizer`]), the same way the optimization worker provisions
/// appendable capacity between operations.
#[derive(Clone)]
pub struct SegmentProvisioning {
    pub segments_path: PathBuf,
    pub segment_config: SegmentConfig,
    pub payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    pub deferred_internal_id: Option<PointOffsetType>,
}

impl SegmentProvisioning {
    pub fn from_optimizer(
        optimizer: &Optimizer,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    ) -> Self {
        Self {
            segments_path: optimizer.segments_path().to_owned(),
            segment_config: optimizer.segment_optimizer_config().plain_segment_config(),
            payload_index_schema,
            deferred_internal_id: optimizer.threshold_config().deferred_internal_id,
        }
    }
}

impl LockedSegmentHolder {
    /// Ensure there is at least one appendable segment with capacity, creating a fresh empty one
    /// when there is none (no appendable segments at all, or all of them measure at or over
    /// `max_segment_size_bytes`).
    ///
    /// Returns the created segment, or `None` when capacity already exists.
    pub fn ensure_appendable_segment_with_capacity(
        &self,
        provisioning: &SegmentProvisioning,
        max_segment_size_bytes: Option<NonZeroUsize>,
    ) -> OperationResult<Option<LockedSegment>> {
        let segments_guard = self.upgradable_read();
        if segments_guard.has_appendable_segment_with_capacity(max_segment_size_bytes) {
            return Ok(None);
        }

        log::debug!("Creating new appendable segment, all existing segments are over capacity");

        // Building the segment yields a `NewSegmentToken` obliging us to register it.
        let (new_segment, token) = segments_guard.build_tmp_segment(
            &provisioning.segments_path,
            Some(provisioning.segment_config.clone()),
            provisioning.payload_index_schema.clone(),
            provisioning.deferred_internal_id,
            true,
        )?;
        segments_guard.sync_segment_manifest(Some(token))?;
        let mut write_guard = RwLockUpgradableReadGuard::upgrade(segments_guard);
        write_guard.add_new_locked(new_segment.clone());
        Ok(Some(new_segment))
    }

    /// Run `update` under a read guard of this holder, provisioning a fresh appendable segment
    /// and re-running it whenever it reports
    /// [`OutOfAppendableCapacity`](segment::common::operation_error::OperationError::OutOfAppendableCapacity).
    ///
    /// Re-running the update is how a single operation moving more data than one segment's
    /// capacity is applied across several fresh segments: updates are idempotent per point
    /// version, so every attempt skips the points already applied by previous attempts and
    /// resumes where the last one stopped — exactly like re-applying an operation from WAL.
    ///
    /// With `provisioning: None` the update runs once and a capacity error propagates to the
    /// caller (holders without a configured size cap never report one).
    pub fn update_with_segment_provisioning<T>(
        &self,
        provisioning: Option<&SegmentProvisioning>,
        mut update: impl FnMut(&SegmentHolder) -> OperationResult<T>,
    ) -> OperationResult<T> {
        let mut last_provisioned: Option<LockedSegment> = None;

        for _attempt in 0..MAX_PROVISIONING_ATTEMPTS_PER_OPERATION {
            let (result, max_segment_size_bytes) = {
                let segments_guard = self.read();
                let result = update(&segments_guard);
                (result, segments_guard.max_segment_size_bytes())
            };

            let err = match result {
                Err(err) if err.is_out_of_appendable_capacity() => err,
                other => return other,
            };
            let Some(provisioning) = provisioning else {
                return Err(err);
            };

            // Progress invariant: on a repeated capacity error, the segment provisioned by the
            // previous attempt must have absorbed data by now (it measured under the cap, so the
            // update preferred it as a destination). A still-empty segment means re-running
            // cannot make progress — e.g. a segment whose size cannot be measured — so fail
            // instead of provisioning segments forever.
            if let Some(segment) = &last_provisioned {
                let absorbed = segment
                    .get()
                    .read()
                    .max_available_vectors_size_in_bytes()
                    .unwrap_or(0)
                    > 0;
                if !absorbed {
                    return Err(err);
                }
            }

            // Re-check under the holder lock: capacity may have been provisioned concurrently
            // (e.g. by the optimization worker), in which case just re-run the update.
            last_provisioned =
                self.ensure_appendable_segment_with_capacity(provisioning, max_segment_size_bytes)?;
        }

        Err(OperationError::service_error(format!(
            "Update did not complete after provisioning appendable segments \
             {MAX_PROVISIONING_ATTEMPTS_PER_OPERATION} times",
        )))
    }
}
