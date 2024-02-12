use thiserror::Error;

use super::clock_map::RecoveryPoint;
use super::{LocalShard, LockedWal};

impl LocalShard {
    pub async fn resolve_wal_delta(
        &self,
        recovery_point: RecoveryPoint,
    ) -> Result<u64, WalDeltaError> {
        let local_last_seen = self.clock_map.lock().await.to_recovery_point();
        resolve_wal_delta(recovery_point, &self.wal, local_last_seen)
    }
}

/// Resolve the WAL delta for the given `recovery_point`
///
/// A `local_wal` and `local_last_seen` are required to resolve the delta. These should be from the
/// node being the source of recovery, likely the current one. The `local_wal` is used to resolve
/// the diff. The `local_last_seen` is used to extend the given recovery point with clocks the
/// failed node does not know about.
///
/// The delta can be sent over to the node which the recovery point is from, to restore its
/// WAL making it consistent with the current shard.
///
/// On success, a WAL record number from which the delta is resolved in the given WAL is returned.
/// If a WAL delta could not be resolved, an error is returned describing the failure.
pub fn resolve_wal_delta(
    mut recovery_point: RecoveryPoint,
    local_wal: &LockedWal,
    local_last_seen: RecoveryPoint,
) -> Result<u64, WalDeltaError> {
    if recovery_point.is_empty() {
        return Err(WalDeltaError::Empty);
    }

    // If our current node has any lower last seen clock than the recovery point specifies,
    // we cannot resolve a WAL delta
    if recovery_point.has_any_higher(&local_last_seen) {
        return Err(WalDeltaError::HigherThanCurrent);
    }

    // Extend clock map with missing clocks this node know about
    // Ensure the recovering node gets records for a clock it might not have seen yet
    recovery_point.extend_with_missing_clocks(&local_last_seen);

    // Remove clocks that are equal to the current last seen
    // We don't have to transfer any record for these
    // TODO: do we want to remove higher clocks too, as the recovery node already has all data?
    recovery_point.remove_equal_clocks(&local_last_seen);

    // TODO: check truncated clock values or each clock we have:
    // TODO: - if truncated is higher, we cannot resolve diff

    // Scroll back over the WAL and find a record that covered all clocks
    // Drain satisfied clocks from the recovery point until we have nothing left
    log::trace!("Resolving WAL delta for: {recovery_point}");
    let delta_from = local_wal
        .lock()
        .read_from_last(true)
        .filter_map(|(op_num, update)| update.clock_tag.map(|clock_tag| (op_num, clock_tag)))
        // Keep scrolling until we have no clocks left
        .find(|(_, clock_tag)| {
            recovery_point.remove_equal_or_lower(*clock_tag);
            recovery_point.is_empty()
        })
        .map(|(op_num, _)| op_num);

    delta_from.ok_or(WalDeltaError::NotFound)
}

#[derive(Error, Debug, Clone)]
#[error("cannot resolve WAL delta: {0}")]
pub enum WalDeltaError {
    #[error("recovery point has no clocks to resolve delta for")]
    Empty,
    #[error("recovery point has higher clocks than current WAL")]
    HigherThanCurrent,
    #[error("some recovery point clocks are truncated in our WAL")]
    Truncated,
    #[error("some recovery point clocks are not found in our WAL")]
    NotFound,
}
