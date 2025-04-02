use std::fmt::Debug;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::operations::{ClockTag, OperationWithClockTag};
use crate::shards::local_shard::clock_map::{ClockMap, RecoveryPoint};
use crate::wal::SerdeWal;

pub(crate) type LockedWal = Arc<Mutex<SerdeWal<OperationWithClockTag>>>;

/// A WAL that is recoverable, with operations having clock tags and a corresponding clock map.
pub struct RecoverableWal {
    pub(super) wal: LockedWal,

    /// Map of all highest seen clocks for each peer and clock ID.
    pub(super) newest_clocks: Arc<Mutex<ClockMap>>,

    /// Map of all clocks and ticks that are cut off.
    ///
    /// Clock ticks equal to those in this map are still recoverable, while clock ticks below those
    /// in this map are not.
    ///
    /// This means two things:
    /// - this WAL has at least all these clock versions
    ///   - (so if we advance these clocks, we have to advance `newest_clocks` as well)
    /// - this WAL cannot resolve any delta below any of these clocks
    pub(super) oldest_clocks: Arc<Mutex<ClockMap>>,
}

impl RecoverableWal {
    pub fn new(
        wal: LockedWal,
        highest_clocks: Arc<Mutex<ClockMap>>,
        cutoff_clocks: Arc<Mutex<ClockMap>>,
    ) -> Self {
        Self {
            wal,
            newest_clocks: highest_clocks,
            oldest_clocks: cutoff_clocks,
        }
    }

    /// Write a record to the WAL, guarantee durability.
    ///
    /// On success, this returns the WAL record number of the written operation along with a WAL
    /// lock guard.
    #[must_use = "returned record number and WAL lock must be used carefully"]
    pub async fn lock_and_write(
        &self,
        operation: &mut OperationWithClockTag,
    ) -> crate::wal::Result<(u64, OwnedMutexGuard<SerdeWal<OperationWithClockTag>>)> {
        // Update last seen clock map and correct clock tag if necessary
        if let Some(clock_tag) = &mut operation.clock_tag {
            let operation_accepted = self
                .newest_clocks
                .lock()
                .await
                .advance_clock_and_correct_tag(clock_tag);

            if !operation_accepted {
                return Err(crate::wal::WalError::ClockRejected);
            }
        }

        // Write operation to WAL
        let mut wal_lock = Mutex::lock_owned(self.wal.clone()).await;
        wal_lock.write(operation).map(|op_num| (op_num, wal_lock))
    }

    /// Update the cutoff clock map based on the given recovery point
    ///
    /// This can only increase clock ticks in the cutoff clock map. If there already are higher
    /// clock ticks, they're kept.
    ///
    /// It updates the highest seen clocks alongside with it.
    pub async fn update_cutoff(&self, cutoff: &RecoveryPoint) {
        // Lock highest and cutoff maps separately to avoid deadlocks

        {
            let mut newest_clocks = self.newest_clocks.lock().await;
            for clock_tag in cutoff.iter_as_clock_tags() {
                newest_clocks.advance_clock(clock_tag);
            }
        }

        {
            let mut oldest_clocks = self.oldest_clocks.lock().await;
            for clock_tag in cutoff.iter_as_clock_tags() {
                oldest_clocks.advance_clock(clock_tag);
            }
        }
    }

    /// Get a recovery point for this WAL.
    pub async fn recovery_point(&self) -> RecoveryPoint {
        self.newest_clocks.lock().await.to_recovery_point()
    }

    pub async fn resolve_wal_delta(
        &self,
        recovery_point: RecoveryPoint,
    ) -> Result<Option<u64>, WalDeltaError> {
        let newest_clocks = self.recovery_point().await;

        let oldest_clocks = self.oldest_clocks.lock().await.to_recovery_point();

        resolve_wal_delta(
            self.wal
                .lock()
                .await
                .read_all(true)
                .map(|(op_num, op)| (op_num, op.clock_tag)),
            recovery_point,
            newest_clocks,
            oldest_clocks,
        )
    }

    pub async fn wal_version(&self) -> Result<Option<u64>, WalDeltaError> {
        let wal = self.wal.lock().await;
        if wal.is_empty() {
            Ok(None)
        } else {
            Ok(Some(wal.last_index()))
        }
    }

    /// Append records to this WAL from `other`, starting at operation `append_from` in `other`.
    #[cfg(test)]
    pub async fn append_from(&self, other: &Self, append_from: u64) -> crate::wal::Result<()> {
        let mut operations = other
            .wal
            .lock()
            .await
            .read(append_from)
            .map(|(_, op)| op)
            .collect::<Vec<_>>();
        for update in operations.iter_mut() {
            let (_, _) = self.lock_and_write(update).await?;
        }
        Ok(())
    }
}

/// Resolve the WAL delta for the given `recovery_point`
///
/// A `local_wal`, `newest_clocks` and `oldest_clocks` are required to resolve the
/// delta. These should be from the node being the source of recovery, likely the current one. The
/// `local_wal` is used to resolve the diff. The `newest_clocks` is used to extend the given
/// recovery point with clocks the failed node does not know about. The `oldest_clocks` is
/// used as lower bound for WAL delta resolution.
///
/// The delta can be sent over to the node which the recovery point is from, to restore its
/// WAL making it consistent with the current shard.
///
/// On success, an option holding a WAL record number is returned.
/// If `Some` - the remote WAL can be recovered by sending the local WAL from that record number.
/// If `None` - the remote WAL is already equal, and we don't have to send any records.
/// If `Err` - no delta can be resolved.
fn resolve_wal_delta(
    operations: impl DoubleEndedIterator<Item = (u64, Option<ClockTag>)>,
    mut recovery_point: RecoveryPoint,
    mut newest_clocks: RecoveryPoint,
    mut oldest_clocks: RecoveryPoint,
) -> Result<Option<u64>, WalDeltaError> {
    // If recovery point is empty, we cannot do a diff transfer
    if recovery_point.is_empty() {
        return Err(WalDeltaError::Empty);
    }

    // If the recovery point has clocks our current node does not know about
    // we're missing essential records and cannot resolve a WAL delta
    if recovery_point.has_clocks_not_in(&newest_clocks) {
        return Err(WalDeltaError::UnknownClocks);
    }

    // If our current node has any lower clock than the recovery point specifies,
    // we're missing essential records and cannot resolve a WAL delta
    if recovery_point.has_any_newer_clocks_than(&newest_clocks) {
        return Err(WalDeltaError::HigherThanCurrent);
    }

    // From this point, increase all clocks by one
    // We must do that so we can specify clock tick 0 as needing everything from that clock
    recovery_point.increase_all_clocks_by(1);
    newest_clocks.increase_all_clocks_by(1);
    oldest_clocks.increase_all_clocks_by(1);

    // Extend clock map with missing clocks this node know about
    // Ensure the recovering node gets records for a clock it might not have seen yet
    recovery_point.initialize_clocks_missing_from(&newest_clocks);

    // Remove clocks that are equal to this node, we don't have to transfer records for them
    // TODO: do we want to remove higher clocks too, as the recovery node already has all data?
    recovery_point.remove_clocks_equal_to(&newest_clocks);

    // Recovery point may not be below our cutoff point
    if recovery_point.has_any_older_clocks_than(&oldest_clocks) {
        return Err(WalDeltaError::Cutoff);
    }

    // If there are no points left, WALs match op so we do not recovery anything
    if recovery_point.is_empty() {
        return Ok(None);
    }

    // Scroll back over the WAL and find a record that covered all clocks, allowing delta resolution
    // Drain satisfied clocks from the recovery point until we have nothing left
    log::trace!("Resolving WAL delta for: {recovery_point}");

    let mut last_op_num = None;

    for (op_num, clock_tag) in operations.rev() {
        // We cannot resolve a delta if we have untagged records
        let Some(clock_tag) = clock_tag else {
            return Err(WalDeltaError::UntaggedRecords);
        };

        // Keep scrolling until we have no clocks left
        let removed_equal = recovery_point.remove_clock_if_newer_or_equal_to_tag(clock_tag);

        if recovery_point.is_empty() {
            // If we only removed newer clocks, delta-ing from the previous record is good enough

            let recover_from = if removed_equal {
                Some(op_num)
            } else {
                last_op_num
            };

            return Ok(recover_from);
        }

        last_op_num.replace(op_num);
    }

    Err(WalDeltaError::NotFound)
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("cannot resolve WAL delta: {0}")]
pub enum WalDeltaError {
    #[error("recovery point has no clocks to resolve delta for")]
    Empty,
    #[error("recovery point requests clocks this WAL does not know about")]
    UnknownClocks,
    #[error("recovery point requests higher clocks this WAL has")]
    HigherThanCurrent,
    #[error("some recovery point clocks are below the cutoff point in our WAL")]
    Cutoff,
    #[error("WAL delta cannot include records without clock tags")]
    UntaggedRecords,
    #[error("cannot find slice of WAL records that satisfies the recovery point")]
    NotFound,
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::ops::Range;
    use std::sync::Arc;

    use rand::prelude::SliceRandom;
    use rand::rngs::StdRng;
    use rand::seq::IndexedRandom;
    use rand::{Rng, SeedableRng};
    use rstest::rstest;
    use segment::data_types::vectors::VectorStructInternal;
    use tempfile::{Builder, TempDir};
    use wal::WalOptions;

    use super::*;
    use crate::operations::point_ops::{
        PointInsertOperationsInternal, PointOperations, PointStructPersisted,
    };
    use crate::operations::{ClockTag, CollectionUpdateOperations, OperationWithClockTag};
    use crate::shards::local_shard::clock_map::{ClockMap, RecoveryPoint};
    use crate::shards::replica_set::clock_set::ClockSet;
    use crate::wal::SerdeWal;

    fn fixture_empty_wal() -> (RecoverableWal, TempDir) {
        let dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let options = WalOptions {
            segment_capacity: 1024 * 1024,
            segment_queue_len: 0,
        };
        let wal = SerdeWal::new(dir.path().to_str().unwrap(), options).unwrap();
        (
            RecoverableWal::new(
                Arc::new(Mutex::new(wal)),
                Arc::new(Mutex::new(ClockMap::default())),
                Arc::new(Mutex::new(ClockMap::default())),
            ),
            dir,
        )
    }

    fn mock_operation(id: u64) -> CollectionUpdateOperations {
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
                id: id.into(),
                vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0]).into(),
                payload: None,
            }]),
        ))
    }

    /// Test WAL delta resolution with just one missed operation on node C.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[tokio::test]
    async fn test_resolve_wal_delta_one_operation() {
        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock set for peer A, start first clock from 1
        let mut a_clock_set = ClockSet::new();
        a_clock_set.get_clock().advance_to(0);

        // Create operation on peer A
        let mut a_clock_0 = a_clock_set.get_clock();
        let clock_tick = a_clock_0.tick_once();
        let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
        let bare_operation = mock_operation(1);
        let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

        // Write operation to peer A, B and C, and advance clocks
        let mut a_operation = operation.clone();
        let mut b_operation = operation.clone();
        let mut c_operation = operation.clone();
        let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
        let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
        let (_, _) = c_wal.lock_and_write(&mut c_operation).await.unwrap();
        a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        drop(a_clock_0);

        // Create operation on peer A
        let mut a_clock_0 = a_clock_set.get_clock();
        let clock_tick = a_clock_0.tick_once();
        let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
        let bare_operation = mock_operation(2);
        let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

        // Write operation to peer A and B, not C, and advance clocks
        let mut a_operation = operation.clone();
        let mut b_operation = operation.clone();
        let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
        let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
        a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        drop(a_clock_0);

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, 1);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, 1);

        // Diff should have 1 operation, as C missed just one
        assert_eq!(b_wal.wal.lock().await.read(delta_from).count(), 1);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WALs should match up perfectly now
        a_wal
            .wal
            .lock()
            .await
            .read(0)
            .zip(b_wal.wal.lock().await.read(0))
            .zip(c_wal.wal.lock().await.read(0))
            .for_each(|((a, b), c)| {
                assert_eq!(a, b);
                assert_eq!(b, c);
            });

        assert_wal_ordering_property(&a_wal, false).await;
        assert_wal_ordering_property(&b_wal, false).await;
        assert_wal_ordering_property(&c_wal, false).await;
    }

    /// Test WAL delta resolution when there is gaps in the WAL on all machines.
    ///
    /// We normally do not expect this situation. But it's good to support it if it happens
    /// unexpectedly.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[rstest]
    #[tokio::test]
    async fn test_resolve_wal_delta_with_gaps(#[values(true, false)] with_gap: bool) {
        const N: usize = 5;
        const GAP_SIZE: usize = 10;

        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock set for peer A, start first clock from 1
        let mut a_clock_set = ClockSet::new();
        a_clock_set.get_clock().advance_to(0);

        // Create N operations on peer A
        for n in 0..N {
            let mut a_clock_0 = a_clock_set.get_clock();
            let clock_tick = a_clock_0.tick_once();
            let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
            let bare_operation = mock_operation((1 + n) as u64);
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operation to peer A, B and C and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            let mut c_operation = operation.clone();
            let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
            let (_, _) = c_wal.lock_and_write(&mut c_operation).await.unwrap();
            a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        }

        // Introduce a gap in the clocks on A
        if with_gap {
            for _ in 0..GAP_SIZE {
                let mut a_clock_0 = a_clock_set.get_clock();
                let clock_tick = a_clock_0.tick_once();
                a_clock_0.advance_to(clock_tick);
            }
        }

        // Create N operations on peer A, which are missed on node C
        for n in 0..N {
            let mut a_clock_0 = a_clock_set.get_clock();
            let clock_tick = a_clock_0.tick_once();
            let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
            let bare_operation = mock_operation((1 + N + n) as u64);
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operation to peer A and B and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
            a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        }

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, N as u64);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, N as u64);

        // Diff should have N operation, as C missed just N of them
        assert_eq!(b_wal.wal.lock().await.read(delta_from).count(), N);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WALs should match up perfectly now
        a_wal
            .wal
            .lock()
            .await
            .read(0)
            .zip(b_wal.wal.lock().await.read(0))
            .zip(c_wal.wal.lock().await.read(0))
            .for_each(|((a, b), c)| {
                assert_eq!(a, b);
                assert_eq!(b, c);
            });

        assert_wal_ordering_property(&a_wal, true).await;
        assert_wal_ordering_property(&b_wal, true).await;
        assert_wal_ordering_property(&c_wal, true).await;
    }

    /// Test WAL delta resolution with a many missed operations on node C.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[tokio::test]
    async fn test_resolve_wal_delta_many_operations() {
        const N: usize = 5;
        const M: usize = 25;

        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock set for peer A, start first clock from 1
        let mut a_clock_set = ClockSet::new();
        a_clock_set.get_clock().advance_to(0);

        // Create N operations on peer A
        for i in 0..N {
            let mut a_clock_0 = a_clock_set.get_clock();
            let clock_tick = a_clock_0.tick_once();
            let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
            let bare_operation = mock_operation(i as u64);
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operations to peer A, B and C, and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            let mut c_operation = operation.clone();
            let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
            let (_, _) = c_wal.lock_and_write(&mut c_operation).await.unwrap();
            a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        }

        // Create M operations on peer A, which are missed on node C
        for i in N..N + M {
            let mut a_clock_0 = a_clock_set.get_clock();
            let clock_tick = a_clock_0.tick_once();
            let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
            let bare_operation = mock_operation(i as u64);
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operations to peer A and B, not C, and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
            a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        }

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, N as u64);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal
            .resolve_wal_delta(c_recovery_point)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, N as u64);

        // Diff should have M operations, as node C missed M operations
        assert_eq!(b_wal.wal.lock().await.read(delta_from).count(), M);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WALs should match up perfectly now
        a_wal
            .wal
            .lock()
            .await
            .read(0)
            .zip(b_wal.wal.lock().await.read(0))
            .zip(c_wal.wal.lock().await.read(0))
            .for_each(|((a, b), c)| {
                assert_eq!(a, b);
                assert_eq!(b, c);
            });

        assert_wal_ordering_property(&a_wal, false).await;
        assert_wal_ordering_property(&b_wal, false).await;
        assert_wal_ordering_property(&c_wal, false).await;
    }

    /// Test WAL delta resolution with many intermixed operations on node C. Intermixed as in,
    /// from multiple nodes.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[tokio::test]
    async fn test_resolve_wal_delta_many_intermixed_operations() {
        const N: usize = 3;
        const M: usize = 50;

        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock sets for peer A and B
        let mut a_clock_set = ClockSet::new();
        let mut b_clock_set = ClockSet::new();

        // Create N operations on peer A
        for i in 0..N {
            let mut a_clock_0 = a_clock_set.get_clock();
            a_clock_0.advance_to(0);
            let clock_tick = a_clock_0.tick_once();
            let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
            let bare_operation = mock_operation(i as u64);
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operations to peer A, B and C, and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            let mut c_operation = operation.clone();
            let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
            let (_, _) = c_wal.lock_and_write(&mut c_operation).await.unwrap();
            a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
            a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        }

        // Create M operations on peers A and B, which are missed on node C
        for i in N..N + M {
            let is_node_a = i % 3 == 0;
            let peer_id = if is_node_a { 1 } else { 2 };

            let mut clock = if is_node_a {
                a_clock_set.get_clock()
            } else {
                b_clock_set.get_clock()
            };
            clock.advance_to(0);
            let clock_tick = clock.tick_once();
            let clock_tag = ClockTag::new(peer_id, clock.id(), clock_tick);
            let bare_operation = mock_operation(i as u64);
            let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

            // Write operations to peer A and B, not C, and advance clocks
            let mut a_operation = operation.clone();
            let mut b_operation = operation.clone();
            let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
            clock.advance_to(a_operation.clock_tag.unwrap().clock_tick);
            clock.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        }

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, N as u64);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal
            .resolve_wal_delta(c_recovery_point)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, N as u64);

        // Diff should have M operations, as node C missed M operations
        assert_eq!(b_wal.wal.lock().await.read(delta_from).count(), M);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WALs should match up perfectly now
        a_wal
            .wal
            .lock()
            .await
            .read(0)
            .zip(b_wal.wal.lock().await.read(0))
            .zip(c_wal.wal.lock().await.read(0))
            .for_each(|((a, b), c)| {
                assert_eq!(a, b);
                assert_eq!(b, c);
            });

        assert_wal_ordering_property(&a_wal, false).await;
        assert_wal_ordering_property(&b_wal, false).await;
        assert_wal_ordering_property(&c_wal, false).await;
    }

    /// Test WAL delta resolution with operations in a different order on node A and B.
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[tokio::test]
    async fn test_resolve_wal_delta_unordered_operations() {
        // Create WALs for peer A, B and C
        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();
        let (c_wal, _c_wal_dir) = fixture_empty_wal();

        // Create clock sets for peer A and B, start first clocks from 1
        let mut a_clock_set = ClockSet::new();
        let mut b_clock_set = ClockSet::new();
        a_clock_set.get_clock().advance_to(0);
        b_clock_set.get_clock().advance_to(0);

        // Create operation on peer A
        let mut a_clock_0 = a_clock_set.get_clock();
        let clock_tick = a_clock_0.tick_once();
        let clock_tag = ClockTag::new(1, a_clock_0.id(), clock_tick);
        let bare_operation = mock_operation(1);
        let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

        // Write operation to peer A, B and C, and advance clocks
        let mut a_operation = operation.clone();
        let mut b_operation = operation.clone();
        let mut c_operation = operation.clone();
        let (_, _) = a_wal.lock_and_write(&mut a_operation).await.unwrap();
        let (_, _) = b_wal.lock_and_write(&mut b_operation).await.unwrap();
        let (_, _) = c_wal.lock_and_write(&mut c_operation).await.unwrap();
        a_clock_0.advance_to(a_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(b_operation.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(c_operation.clock_tag.unwrap().clock_tick);
        drop(a_clock_0);

        // Create operations on nodes A and B
        let mut a_clock_0 = a_clock_set.get_clock();
        let mut b_clock_0 = b_clock_set.get_clock();
        let a_clock_tick = a_clock_0.tick_once();
        let b_clock_tick = b_clock_0.tick_once();
        let a_clock_tag = ClockTag::new(1, a_clock_0.id(), a_clock_tick);
        let b_clock_tag = ClockTag::new(2, a_clock_0.id(), b_clock_tick);
        let bare_operation_1 = mock_operation(2);
        let bare_operation_2 = mock_operation(3);
        let operation_1 = OperationWithClockTag::new(bare_operation_1, Some(a_clock_tag));
        let operation_2 = OperationWithClockTag::new(bare_operation_2, Some(b_clock_tag));

        // Write operations to nodes A and B in different order, but not to node C
        let mut a_operation_1 = operation_1.clone();
        let mut a_operation_2 = operation_2.clone();
        let mut b_operation_1 = operation_1.clone();
        let mut b_operation_2 = operation_2.clone();
        let (_, _) = a_wal.lock_and_write(&mut a_operation_1).await.unwrap();
        let (_, _) = a_wal.lock_and_write(&mut a_operation_2).await.unwrap();
        let (_, _) = b_wal.lock_and_write(&mut b_operation_2).await.unwrap();
        let (_, _) = b_wal.lock_and_write(&mut b_operation_1).await.unwrap();
        a_clock_0.advance_to(a_operation_1.clock_tag.unwrap().clock_tick);
        a_clock_0.advance_to(a_operation_2.clock_tag.unwrap().clock_tick);
        b_clock_0.advance_to(b_operation_2.clock_tag.unwrap().clock_tick);
        b_clock_0.advance_to(b_operation_1.clock_tag.unwrap().clock_tick);
        drop(a_clock_0);
        drop(b_clock_0);

        let c_recovery_point = c_wal.recovery_point().await;

        // Resolve delta on node A for node C, assert correctness
        let delta_from = a_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, 1);

        // Resolve delta on node B for node C, assert correctness
        let delta_from = b_wal
            .resolve_wal_delta(c_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta_from, 1);

        // Diff should have 2 operations on both nodes
        assert_eq!(a_wal.wal.lock().await.read(delta_from).count(), 2);
        assert_eq!(b_wal.wal.lock().await.read(delta_from).count(), 2);

        // Recover WAL on node C by writing delta from node B to it
        c_wal.append_from(&b_wal, delta_from).await.unwrap();

        // WAL on node B and C will match, A is in different order
        assert!(
            !a_wal
                .wal
                .lock()
                .await
                .read(0)
                .zip(c_wal.wal.lock().await.read(0))
                .all(|(a, c)| a == c),
        );
        assert!(
            b_wal
                .wal
                .lock()
                .await
                .read(0)
                .zip(c_wal.wal.lock().await.read(0))
                .all(|(b, c)| b == c),
        );

        // All WALs should have 3 operations
        assert_eq!(a_wal.wal.lock().await.read(0).count(), 3);
        assert_eq!(b_wal.wal.lock().await.read(0).count(), 3);
        assert_eq!(c_wal.wal.lock().await.read(0).count(), 3);

        // All WALs must have operations for point 1, 2 and 3
        let get_point = |op| match op {
            OperationWithClockTag {
                operation:
                    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                        PointInsertOperationsInternal::PointsList(points),
                    )),
                ..
            } => points[0].clone(),
            _ => unreachable!(),
        };
        let a_wal_point_ids = a_wal
            .wal
            .lock()
            .await
            .read(0)
            .map(|(_, op)| get_point(op).id)
            .collect::<HashSet<_>>();
        let b_wal_point_ids = b_wal
            .wal
            .lock()
            .await
            .read(0)
            .map(|(_, op)| get_point(op).id)
            .collect::<HashSet<_>>();
        let c_wal_point_ids = c_wal
            .wal
            .lock()
            .await
            .read(0)
            .map(|(_, op)| get_point(op).id)
            .collect::<HashSet<_>>();
        (1..=3).for_each(|i| {
            assert!(a_wal_point_ids.contains(&i.into()));
            assert!(b_wal_point_ids.contains(&i.into()));
            assert!(c_wal_point_ids.contains(&i.into()));
        });

        assert_wal_ordering_property(&a_wal, false).await;
        assert_wal_ordering_property(&b_wal, false).await;
        assert_wal_ordering_property(&c_wal, false).await;
    }

    #[tokio::test]
    async fn test_recover_from_previously_recoverred_with_forward_proxy() {
        // Consider a situation

        // Steps:
        //
        // 1. We initialize 2 operations on A and B, that are successfully written to both from C
        // 2. Operation 3 is written to A, but not B
        // 2.1. Operation 30  is written to A, but not B (Second channel)
        // 3. Operation 4,5 from D is written to both A and B
        // 4. Now B is reported as failed and we need to recover it from A
        // 5. During recovery, we send untagged operations from A to B (full transfer) + node C sends an Update
        // 6. After recovered (need to check consistency of A and B), node D sends an Update to A and B
        // 7. Now we want to recover newly created node E from B, it expectedly fails, because of cutoff point
        // 8. Try to recover A from E (expect no diff, as both have same data)
        // 9. Insert new operation to B but not A to make sure diff resolution starts working after full recovery

        //          Recover
        //     ┌───┬───────►┌───┐
        //     │ A │        │ B │
        //     └─▲─┴───────►└─▲─┘
        //       │  Forward   │
        //       │            │
        //       │            │
        //       │   ┌───┐    │
        //       └───┤ C ├────┘
        //  Update   └───┘  Failed Update
        //
        //
        //
        //           ┌───┐
        //       ┌───┤ D ├────┐
        // Update│   └───┘    │Update
        //       │            │
        //       │            │
        //     ┌─▼─┐        ┌─▼─┐
        //     │ A │        │ B │
        //     └───┘        └───┘
        //
        //
        //                   (Almost Empty)
        //     ┌───┐Recover ┌───┐
        //     │ B ├───────►│ E │
        //     └───┘        └───┘
        //
        //
        //                   (Identical)
        //     ┌───┐Recover ┌───┐
        //     │ E ├───────►│ A │
        //     └───┘        └───┘

        let (a_wal, _a_wal_dir) = fixture_empty_wal();
        let (b_wal, _b_wal_dir) = fixture_empty_wal();

        let (e_wal, _e_wal_dir) = fixture_empty_wal();

        let mut c_clock_set = ClockSet::new();
        let mut d_clock_set = ClockSet::new();

        let node_c_peer_id = 1;
        let node_d_peer_id = 2;

        let op1: CollectionUpdateOperations = mock_operation(1);

        // Initial normal operation, written to both A and B  + additionally Em but we will need it later
        {
            // Node C is sending updates to A and B
            let mut c_clock_0 = c_clock_set.get_clock();
            c_clock_0.advance_to(0);
            let clock_tick = c_clock_0.tick_once();
            let clock_tag = ClockTag::new(node_c_peer_id, c_clock_0.id(), clock_tick);

            let operation_with_clock = OperationWithClockTag::new(op1, Some(clock_tag));

            let mut operation_a = operation_with_clock.clone();
            let mut operation_b = operation_with_clock.clone();
            let mut operation_e = operation_with_clock.clone();

            let (_, _) = a_wal.lock_and_write(&mut operation_a).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut operation_b).await.unwrap();
            let (_, _) = e_wal.lock_and_write(&mut operation_e).await.unwrap();

            c_clock_0.advance_to(operation_a.clock_tag.unwrap().clock_tick);
            c_clock_0.advance_to(operation_b.clock_tag.unwrap().clock_tick);
            c_clock_0.advance_to(operation_e.clock_tag.unwrap().clock_tick);
        }

        let op2: CollectionUpdateOperations = mock_operation(2);

        // Initial normal operation, written to both
        {
            // Node C is sending updates to A and B
            let mut c_clock_0 = c_clock_set.get_clock();
            c_clock_0.advance_to(0);
            let clock_tick = c_clock_0.tick_once();
            let clock_tag = ClockTag::new(node_c_peer_id, c_clock_0.id(), clock_tick);

            let operation_with_clock = OperationWithClockTag::new(op2, Some(clock_tag));

            let mut operation_a = operation_with_clock.clone();
            let mut operation_b = operation_with_clock.clone();

            let (_, _) = a_wal.lock_and_write(&mut operation_a).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut operation_b).await.unwrap();

            c_clock_0.advance_to(operation_a.clock_tag.unwrap().clock_tick);
            c_clock_0.advance_to(operation_b.clock_tag.unwrap().clock_tick);
        }

        let op3: CollectionUpdateOperations = mock_operation(3);
        let op30: CollectionUpdateOperations = mock_operation(30);

        // Next operation gets written to A, but not B
        {
            // Node C is sending updates to A and B
            let mut c_clock_0 = c_clock_set.get_clock();
            let mut c_clock_1 = c_clock_set.get_clock();
            c_clock_0.advance_to(0);
            c_clock_1.advance_to(0);

            {
                // First parallel operation
                let clock_tick = c_clock_0.tick_once();
                let clock_tag = ClockTag::new(node_c_peer_id, c_clock_0.id(), clock_tick);

                let operation_with_clock = OperationWithClockTag::new(op3, Some(clock_tag));

                let mut operation_a = operation_with_clock.clone();

                let (_, _) = a_wal.lock_and_write(&mut operation_a).await.unwrap();

                c_clock_0.advance_to(operation_a.clock_tag.unwrap().clock_tick);
            }

            {
                // Second parallel operation
                let clock_tick = c_clock_1.tick_once();
                let clock_tag = ClockTag::new(node_c_peer_id, c_clock_1.id(), clock_tick);

                let operation_with_clock = OperationWithClockTag::new(op30, Some(clock_tag));

                let mut operation_a = operation_with_clock.clone();

                let (_, _) = a_wal.lock_and_write(&mut operation_a).await.unwrap();

                c_clock_1.advance_to(operation_a.clock_tag.unwrap().clock_tick);
            }
        }

        let op4: CollectionUpdateOperations = mock_operation(4);

        // Node D sends an update to both A and B, both successfully written
        {
            let mut d_clock_0 = d_clock_set.get_clock();
            d_clock_0.advance_to(0);
            let clock_tick = d_clock_0.tick_once();
            let clock_tag = ClockTag::new(node_d_peer_id, d_clock_0.id(), clock_tick);

            let operation_with_clock = OperationWithClockTag::new(op4, Some(clock_tag));

            let mut operation_a = operation_with_clock.clone();
            let mut operation_b = operation_with_clock.clone();

            let (_, _) = a_wal.lock_and_write(&mut operation_a).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut operation_b).await.unwrap();

            d_clock_0.advance_to(operation_a.clock_tag.unwrap().clock_tick);
            d_clock_0.advance_to(operation_b.clock_tag.unwrap().clock_tick);
        }

        let op5: CollectionUpdateOperations = mock_operation(5);

        // Node D sends an update to both A and B, both successfully written
        {
            let mut d_clock_0 = d_clock_set.get_clock();
            d_clock_0.advance_to(0);
            let clock_tick = d_clock_0.tick_once();
            let clock_tag = ClockTag::new(node_d_peer_id, d_clock_0.id(), clock_tick);

            let operation_with_clock = OperationWithClockTag::new(op5, Some(clock_tag));

            let mut operation_a = operation_with_clock.clone();
            let mut operation_b = operation_with_clock.clone();

            let (_, _) = a_wal.lock_and_write(&mut operation_a).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut operation_b).await.unwrap();

            d_clock_0.advance_to(operation_a.clock_tag.unwrap().clock_tick);
            d_clock_0.advance_to(operation_b.clock_tag.unwrap().clock_tick);
        }

        // Now B is reported as failed and we need to recover it from A

        let b_recovery_point = b_wal.recovery_point().await;

        let delta_from = a_wal
            .resolve_wal_delta(b_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();

        // Operation 0 and 1 are written to both and do not need to be recovered
        // All further operations have to be written to B
        assert_eq!(delta_from, 2);

        // But instead of recovering from WAL, we will check full streaming transfer
        {
            let op1 = mock_operation(1);
            let op1_with_clock = OperationWithClockTag::new(op1, None);
            let (_, _) = b_wal
                .lock_and_write(&mut op1_with_clock.clone())
                .await
                .unwrap();

            let op2 = mock_operation(2);
            let op2_with_clock = OperationWithClockTag::new(op2, None);
            let (_, _) = b_wal
                .lock_and_write(&mut op2_with_clock.clone())
                .await
                .unwrap();
        }

        let op6: CollectionUpdateOperations = mock_operation(6);

        // In between the recovery, we have a new update from C
        // It is written to both A and B, plus forwarded to B with forward proxy
        {
            let mut c_clock_0 = c_clock_set.get_clock();
            c_clock_0.advance_to(0);
            let clock_tick = c_clock_0.tick_once();
            let clock_tag = ClockTag::new(node_c_peer_id, c_clock_0.id(), clock_tick);

            let operation_with_clock = OperationWithClockTag::new(op6, Some(clock_tag));

            let mut operation_a = operation_with_clock.clone();
            let mut operation_b = operation_with_clock.clone();
            let mut operation_b_forward = operation_with_clock.clone();

            let (_, _) = a_wal.lock_and_write(&mut operation_a).await.unwrap();
            let (_, _) = b_wal.lock_and_write(&mut operation_b).await.unwrap();
            let (_, _) = b_wal
                .lock_and_write(&mut operation_b_forward)
                .await
                .unwrap();

            c_clock_0.advance_to(operation_a.clock_tag.unwrap().clock_tick);
            c_clock_0.advance_to(operation_b.clock_tag.unwrap().clock_tick);
        }

        // Continue recovery
        {
            let op3 = mock_operation(3);
            let op3_with_clock = OperationWithClockTag::new(op3, None);
            let (_, _) = b_wal
                .lock_and_write(&mut op3_with_clock.clone())
                .await
                .unwrap();

            let op30 = mock_operation(30);
            let op30_with_clock = OperationWithClockTag::new(op30, None);
            let (_, _) = b_wal
                .lock_and_write(&mut op30_with_clock.clone())
                .await
                .unwrap();

            let op4 = mock_operation(4);
            let op4_with_clock = OperationWithClockTag::new(op4, None);
            let (_, _) = b_wal
                .lock_and_write(&mut op4_with_clock.clone())
                .await
                .unwrap();

            let op5 = mock_operation(5);
            let op5_with_clock = OperationWithClockTag::new(op5, None);
            let (_, _) = b_wal
                .lock_and_write(&mut op5_with_clock.clone())
                .await
                .unwrap();

            // Once full transfer is done, we update cutoff point on B to the last seen of A
            b_wal.update_cutoff(&a_wal.recovery_point().await).await;
        }

        // Try to recover E from B
        let e_recovery_point = e_wal.recovery_point().await;

        // Cannot recover E from B, because B has a high cutoff point due to the full transfer
        let delta_from = b_wal.resolve_wal_delta(e_recovery_point.clone()).await;
        assert_eq!(delta_from.unwrap_err(), WalDeltaError::Cutoff);

        // Try to recover A from B

        // Which should also fail, because B has a high cutoff point due to the full transfer

        let a_recovery_point = a_wal.recovery_point().await;

        /*
        a_recovery_point = RecoveryPoint {
            clocks: {
                C_1: 1,
                C: 4,
                D: 2,
            },
        }
        cutoff_point_b = RecoveryPoint {
            clocks: {
                C_1: 1,
                C: 4,
                D: 2,
            },
        }
        */

        let delta_from = b_wal.resolve_wal_delta(a_recovery_point.clone()).await;

        // No diff expected
        assert_eq!(delta_from, Ok(None));

        let op7 = mock_operation(7);
        // Add operation to B but not A
        {
            // Node D is sending updates to B
            let mut d_clock = d_clock_set.get_clock();
            d_clock.advance_to(0);

            // First parallel operation
            let clock_tick = d_clock.tick_once();
            let clock_tag = ClockTag::new(node_d_peer_id, d_clock.id(), clock_tick);

            let operation_with_clock = OperationWithClockTag::new(op7, Some(clock_tag));

            let mut operation_b = operation_with_clock.clone();

            let (_, _) = b_wal.lock_and_write(&mut operation_b).await.unwrap();

            d_clock.advance_to(operation_b.clock_tag.unwrap().clock_tick);
        }

        let a_recovery_point = a_wal.recovery_point().await;
        let delta_from = b_wal
            .resolve_wal_delta(a_recovery_point.clone())
            .await
            .unwrap()
            .unwrap();

        // Diff expected
        assert_eq!(b_wal.wal.lock().await.read(delta_from).count(), 1);

        assert_wal_ordering_property(&a_wal, false).await;
        assert_wal_ordering_property(&b_wal, false).await;
        assert_wal_ordering_property(&e_wal, false).await;
    }

    /// A randomized and more extensive test for resolving a WAL delta.
    ///
    /// This tests configurations from 2 up to 10 nodes.
    ///
    /// This randomizes:
    /// - The number of operations
    /// - What node is used as entry point
    /// - What node dies
    ///
    /// This test does the following 25 times:
    /// - insert random number of operations on all nodes
    /// - randomly kill a number of nodes (or rather, mark as killed)
    /// - write random number of operations some operations to all other nodes
    /// - recover the killed nodes
    /// - assert correctness
    ///
    /// See: <https://www.notion.so/qdrant/Testing-suite-4e28a978ec05476080ff26ed07757def?pvs=4>
    #[rstest]
    #[case::two_nodes(2, 0..2)]
    #[case::three_nodes(3, 0..3)]
    #[case::four_nodes(4, 1..4)]
    #[case::five_nodes(5, 1..5)]
    #[case::six_nodes(6, 1..6)]
    #[case::seven_nodes(7, 1..7)]
    #[case::eight_nodes(8, 1..8)]
    #[case::nine_nodes(9, 1..9)]
    #[case::ten_nodes(10, 1..10)]
    #[tokio::test]
    async fn test_resolve_wal_delta_randomized(
        #[case] node_count: usize,
        #[case] dead_nodes_range: Range<usize>,
    ) {
        let mut rng = StdRng::seed_from_u64(42);
        let mut point_id_source = 1..;

        // Create WALs, clock sets and clock maps for each node
        let mut wals = std::iter::repeat_with(fixture_empty_wal)
            .take(node_count)
            .collect::<Vec<_>>();
        let mut clock_sets = std::iter::repeat_with(ClockSet::new)
            .take(node_count)
            .collect::<Vec<_>>();

        // A list of clocks we don't release for some iterations
        let mut kept_clocks = vec![];

        // 25 times:
        // - insert random number of operations on all nodes
        // - randomly kill a node (or rather, mark as killed)
        // - write random number of operations some operations to all other nodes
        // - recover the killed node
        // - assert correctness
        for _ in 0..25 {
            // Insert random number of operations on all nodes
            for _ in 0..rng.random_range(0..10) {
                let entrypoint = rng.random_range(0..node_count);

                let mut clock = clock_sets[entrypoint].get_clock();
                clock.advance_to(0);
                let clock_tick = clock.tick_once();
                let clock_tag = ClockTag::new(entrypoint as u64, clock.id(), clock_tick);

                let bare_operation =
                    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                        PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
                            id: point_id_source.next().unwrap().into(),
                            vector: VectorStructInternal::from(
                                std::iter::repeat_with(|| rng.random::<f32>())
                                    .take(3)
                                    .collect::<Vec<_>>(),
                            )
                            .into(),
                            payload: None,
                        }]),
                    ));
                let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

                // Write operations to all WALs
                for (wal, _wal_dir) in wals.iter_mut() {
                    let mut operation = operation.clone();
                    let (_, _) = wal.lock_and_write(&mut operation).await.unwrap();
                    clock.advance_to(operation.clock_tag.unwrap().clock_tick);
                }

                // Maybe keep the clock for some iterations
                let keep_clock_for = rng.random_range(0..3);
                if keep_clock_for > 0 {
                    kept_clocks.push((keep_clock_for, clock));
                }
            }

            // Make a random list of alive and dead nodes
            let mut alive_nodes = (0..node_count).collect::<Vec<_>>();
            alive_nodes.shuffle(&mut rng);
            let dead_nodes = alive_nodes
                .drain(0..rng.random_range(dead_nodes_range.clone()))
                .collect::<HashSet<_>>();

            // Insert random number of operations into all alive nodes
            let operation_count = rng.random_range(0..100);
            for _ in 0..operation_count {
                let entrypoint = *alive_nodes.choose(&mut rng).unwrap();

                let mut clock = clock_sets[entrypoint].get_clock();
                clock.advance_to(0);
                let clock_tick = clock.tick_once();
                let clock_tag = ClockTag::new(entrypoint as u64, clock.id(), clock_tick);

                let bare_operation =
                    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                        PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
                            id: point_id_source.next().unwrap().into(),
                            vector: VectorStructInternal::from(
                                std::iter::repeat_with(|| rng.random::<f32>())
                                    .take(3)
                                    .collect::<Vec<_>>(),
                            )
                            .into(),
                            payload: None,
                        }]),
                    ));
                let operation = OperationWithClockTag::new(bare_operation, Some(clock_tag));

                // Write operations to all WALs and clock maps on alive node
                for &alive_node in &alive_nodes {
                    let mut operation = operation.clone();
                    let (_, _) = wals[alive_node]
                        .0
                        .lock_and_write(&mut operation)
                        .await
                        .unwrap();
                    clock.advance_to(operation.clock_tag.unwrap().clock_tick);
                }

                // Maybe keep the clock for some iterations
                let keep_clock_for = rng.random_range(0..10);
                if keep_clock_for > 0 {
                    kept_clocks.push((keep_clock_for, clock));
                }
            }

            // Recover dead nodes
            for dead_node in dead_nodes {
                // Resolve WAL on every alive node, to recover the dead node
                let recovery_point = wals[dead_node].0.recovery_point().await;
                let mut from_deltas = HashSet::new();
                for &alive_node in &alive_nodes {
                    let delta_from = wals[alive_node]
                        .0
                        .resolve_wal_delta(recovery_point.clone())
                        .await
                        .expect("failed to resolve WAL delta on alive node");
                    from_deltas.insert(delta_from);
                }
                assert_eq!(
                    from_deltas.len(),
                    1,
                    "found different delta starting points in different WALs, while all should be the same",
                );
                let delta_from = from_deltas.into_iter().next().unwrap();
                assert_eq!(
                    delta_from.is_some(),
                    operation_count > 0,
                    "if we had operations to some node, we must find a delta, otherwise not",
                );

                // Recover WAL on the dead node from a random alive node
                if let Some(delta_from) = delta_from {
                    let alive_node = *alive_nodes.choose(&mut rng).unwrap();
                    wals[dead_node]
                        .0
                        .append_from(&wals[alive_node].0, delta_from)
                        .await
                        .unwrap();
                }
            }

            // All WALs must be equal, having exactly the same entries
            let mut opened_wals = Vec::new();
            for wal in &wals {
                opened_wals.push(wal.0.wal.lock().await);
            }
            opened_wals.windows(2).for_each(|wals| {
                assert!(
                    wals[0].read(0).eq(wals[1].read(0)),
                    "all WALs must have the same entries",
                );
            });

            // Release some kept clocks
            kept_clocks.retain(|(keep_for, _)| *keep_for > 1);
        }

        for (wal, _) in wals {
            assert_wal_ordering_property(&wal, false).await;
        }
    }

    /// We cannot resolve a WAL delta if the slice contains records without a clock tag.
    #[tokio::test]
    async fn test_cannot_resolve_delta_over_untagged_record() {
        let (wal, _wal_dir) = fixture_empty_wal();

        // Insert 3 operations with clocks
        let (_, _) = wal
            .lock_and_write(&mut OperationWithClockTag::new(
                mock_operation(1),
                Some(ClockTag::new(1, 0, 1)),
            ))
            .await
            .unwrap();
        let (_, _) = wal
            .lock_and_write(&mut OperationWithClockTag::new(
                mock_operation(2),
                Some(ClockTag::new(1, 0, 2)),
            ))
            .await
            .unwrap();
        let (_, _) = wal
            .lock_and_write(&mut OperationWithClockTag::new(
                mock_operation(3),
                Some(ClockTag::new(1, 0, 3)),
            ))
            .await
            .unwrap();

        // Can resolve a delta for the last clock
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert(1, 0, 2);
        let resolve_result = wal.resolve_wal_delta(recovery_point).await.unwrap();
        assert_eq!(resolve_result, Some(2));

        // Insert operation 4 and 5, where operation 4 does not have a clock tag
        let (_, _) = wal
            .lock_and_write(&mut OperationWithClockTag::new(mock_operation(4), None))
            .await
            .unwrap();
        let (_, _) = wal
            .lock_and_write(&mut OperationWithClockTag::new(
                mock_operation(5),
                Some(ClockTag::new(1, 0, 4)),
            ))
            .await
            .unwrap();

        // Can still resolve a delta for the last clock
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert(1, 0, 4);
        let resolve_result = wal.resolve_wal_delta(recovery_point).await.unwrap();
        assert_eq!(resolve_result, None);

        // Cannot resolve a delta for our previous clock, it now has an untagged record after it
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert(1, 0, 2);
        let resolve_result = wal.resolve_wal_delta(recovery_point).await;
        assert_eq!(resolve_result.unwrap_err(), WalDeltaError::UntaggedRecords);
    }

    /// Empty recovery point should not resolve any diff.
    #[test]
    fn test_empty_recovery_point() {
        let (wal, _wal_dir) = fixture_empty_wal();

        // Empty recovery points, should not resolve any diff
        let recovery_point = RecoveryPoint::default();
        let newest_clocks = RecoveryPoint::default();

        let resolve_result = resolve_wal_delta(
            wal.wal
                .blocking_lock()
                .read_all(true)
                .map(|(op_num, op)| (op_num, op.clock_tag)),
            recovery_point,
            newest_clocks,
            RecoveryPoint::default(),
        );
        assert_eq!(resolve_result.unwrap_err(), WalDeltaError::Empty);
    }

    /// Recovery point with a clock our source does not know about cannot resolve a diff.
    #[test]
    fn test_recover_point_has_unknown_clock() {
        let (wal, _wal_dir) = fixture_empty_wal();

        let mut recovery_point = RecoveryPoint::default();
        let mut newest_clocks = RecoveryPoint::default();

        // Recovery point has a clock our source does not know about
        recovery_point.insert(1, 0, 15);
        recovery_point.insert(1, 1, 8);
        recovery_point.insert(2, 1, 5);
        newest_clocks.insert(1, 0, 20);
        newest_clocks.insert(1, 1, 8);

        let resolve_result = resolve_wal_delta(
            wal.wal
                .blocking_lock()
                .read_all(true)
                .map(|(op_num, op)| (op_num, op.clock_tag)),
            recovery_point,
            newest_clocks,
            RecoveryPoint::default(),
        );
        assert_eq!(resolve_result.unwrap_err(), WalDeltaError::UnknownClocks);
    }

    /// Recovery point with higher clocks than the source cannot resolve a diff.
    #[test]
    fn test_recover_point_higher_than_source() {
        let (wal, _wal_dir) = fixture_empty_wal();

        let mut recovery_point = RecoveryPoint::default();
        let mut newest_clocks = RecoveryPoint::default();

        // Recovery point asks tick 10, but source only has tick 8
        recovery_point.insert(1, 0, 15);
        recovery_point.insert(1, 1, 10);
        newest_clocks.insert(1, 0, 20);
        newest_clocks.insert(1, 1, 8);

        let resolve_result = resolve_wal_delta(
            wal.wal
                .blocking_lock()
                .read_all(true)
                .map(|(op_num, op)| (op_num, op.clock_tag)),
            recovery_point,
            newest_clocks,
            RecoveryPoint::default(),
        );
        assert_eq!(
            resolve_result.unwrap_err(),
            WalDeltaError::HigherThanCurrent
        );
    }

    /// Recovery point requests clocks that are already truncated
    #[test]
    fn test_recover_point_cutoff() {
        let (wal, _wal_dir) = fixture_empty_wal();

        let mut recovery_point = RecoveryPoint::default();
        let mut newest_clocks = RecoveryPoint::default();
        let mut oldest_clocks = RecoveryPoint::default();

        // Recovery point asks clock tick that has been truncated already
        recovery_point.insert(1, 0, 15);
        recovery_point.insert(1, 1, 10);
        newest_clocks.insert(1, 0, 20);
        newest_clocks.insert(1, 1, 12);
        oldest_clocks.insert(1, 0, 16);

        let resolve_result = resolve_wal_delta(
            wal.wal
                .blocking_lock()
                .read_all(true)
                .map(|(op_num, op)| (op_num, op.clock_tag)),
            recovery_point,
            newest_clocks,
            oldest_clocks,
        );
        assert_eq!(resolve_result.unwrap_err(), WalDeltaError::Cutoff);
    }

    /// Recovery point operations are not in our WAL.
    #[test]
    fn test_recover_point_not_in_wal() {
        let (wal, _wal_dir) = fixture_empty_wal();

        let mut recovery_point = RecoveryPoint::default();
        let mut newest_clocks = RecoveryPoint::default();

        // Recovery point asks tick 10, but source only has tick 8
        recovery_point.insert(1, 0, 15);
        recovery_point.insert(1, 1, 10);
        newest_clocks.insert(1, 0, 20);
        newest_clocks.insert(1, 1, 12);

        let resolve_result = resolve_wal_delta(
            wal.wal
                .blocking_lock()
                .read_all(true)
                .map(|(op_num, op)| (op_num, op.clock_tag)),
            recovery_point,
            newest_clocks,
            RecoveryPoint::default(),
        );
        assert_eq!(resolve_result.unwrap_err(), WalDeltaError::NotFound);
    }

    /// Assert that we `check_clock_tag_ordering_property` on the WAL.
    async fn assert_wal_ordering_property(wal: &RecoverableWal, allow_gaps: bool) {
        // Grab list of clock tags from WAL records, skip non-existent or below cutoff tags
        let clock_tags = {
            let cutoff = wal.oldest_clocks.lock().await;
            wal.wal
                .lock()
                .await
                .read(0)
                // Only take records with clock tags
                .filter_map(|(_, operation)| operation.clock_tag)
                // Clock tags must be equal or higher to cutoff point
                .filter(|clock_tag| {
                    cutoff
                        .current_tick(clock_tag.peer_id, clock_tag.clock_id)
                        .is_none_or(|cutoff_tick| clock_tag.clock_tick >= cutoff_tick)
                })
                .collect::<Vec<_>>()
        };

        check_clock_tag_ordering_property(&clock_tags, allow_gaps)
            .expect("WAL ordering property violated");
    }

    /// Test that we satisfy the clock ordering property, allowing WAL recovery resolution.
    ///
    /// Property:
    /// For each operation with peer+clock tick X, all following operations having the same
    /// peer+clock must cover ticks X+1, X+2, ..., X+n in order up to the highest tick value of
    /// that peer+clock in the WAL.
    ///
    /// More specifically, this tests the property again on every clock tag. The result of this
    /// check is that the sequence always ends in order at the end, going up to the highest clock
    /// clock tick.
    ///
    /// `allow_gaps` specifies whether gaps in the clock tick sequences are allowed.
    ///
    /// This logic is validated with examples in `validate_clock_tag_ordering_property`.
    ///
    /// This property may not be valid if a diff transfer has not been resolved correctly or
    /// completely, or if the WAL got malformed in another way.
    fn check_clock_tag_ordering_property(
        clock_tags: &[ClockTag],
        allow_gaps: bool,
    ) -> Result<(), String> {
        // Get the highest clock value for each clock+peer
        let mut highest_clocks = HashMap::new();
        for clock_tag in clock_tags {
            highest_clocks
                .entry((clock_tag.peer_id, clock_tag.clock_id))
                .and_modify(|highest| *highest = clock_tag.clock_tick.max(*highest))
                .or_insert(clock_tag.clock_tick);
        }

        // Test each clock tag for the ordering property
        for (i, clock_tag) in clock_tags.iter().enumerate() {
            let key = (clock_tag.peer_id, clock_tag.clock_id);
            let highest = highest_clocks[&key];

            // An ordered list of ticks we must see for this peer+clock
            let mut must_see_ticks =
                ((clock_tag.clock_tick + 1)..=highest).collect::<VecDeque<_>>();

            // For all the following clock tags of the same peer+clock, remove their tick value
            for newer in clock_tags.iter().skip(i + 1) {
                // Skip other peer and clock pairs
                if (newer.peer_id, newer.clock_id) != key {
                    continue;
                }

                // Keep removing ticks we must see from the beginning of the list
                // If we don't allow gaps, we only remove this exact tick from the beginning
                // If we do allow gaps, we remove this tick and all lower ones from the beginning
                while {
                    must_see_ticks.front().is_some_and(|&tick| {
                        if allow_gaps {
                            tick <= newer.clock_tick
                        } else {
                            tick == newer.clock_tick
                        }
                    })
                } {
                    must_see_ticks.pop_front().unwrap();
                }
            }

            // If list is not empty, we have not seen all numbers
            if !must_see_ticks.is_empty() {
                return Err(format!(
                    "following clock tags did not cover ticks [{}] in order (peer_id: {}, clock_id: {}, max_tick: {highest})",
                    must_see_ticks
                        .into_iter()
                        .map(|tick| tick.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    clock_tag.peer_id,
                    clock_tag.clock_id,
                ));
            }
        }

        Ok(())
    }

    /// Validate that `check_clock_tag_ordering_property` works as expected.
    ///
    /// Yes, this is a test for a test for a test. (⌐■_■)
    #[rstest]
    fn validate_clock_tag_ordering_property(#[values(false, true)] allow_gaps: bool) {
        // Empty is fine
        check_clock_tag_ordering_property(&[], allow_gaps).unwrap();

        // Any one clock tag is fine
        check_clock_tag_ordering_property(&[ClockTag::new(1, 2, 3)], allow_gaps).unwrap();

        // Clock tags in order are allowed
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 3),
            ],
            allow_gaps,
        )
        .unwrap();

        // Clock tags in order with gaps are only allowed if specified
        let result = check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                // Misses 1:0:3-9
                ClockTag::new(1, 0, 10),
                ClockTag::new(1, 0, 11),
            ],
            allow_gaps,
        );
        assert_eq!(result.is_ok(), allow_gaps);

        // Not starting at zero (truncated) is allowed
        check_clock_tag_ordering_property(
            &[
                // Truncated
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 3),
                ClockTag::new(1, 0, 4),
            ],
            allow_gaps,
        )
        .unwrap();

        // Repeated clock tags are allowed
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 2),
            ],
            allow_gaps,
        )
        .unwrap();

        // Repeating clock tag sequence is allowed
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                // Repeats 1:0:0-2 two more times
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
            ],
            allow_gaps,
        )
        .unwrap();

        // Repeating part of clock tag sequence is allowed
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 3),
                // Repeats 1:0:2-3 two more times
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 3),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 3),
            ],
            allow_gaps,
        )
        .unwrap();

        // Repeating clock tag sequence with new ones at the end is allowed
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                // Repeats 1:0:0-2 one more time
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                // Adds 1:0:3-6 on top of it
                ClockTag::new(1, 0, 3),
                ClockTag::new(1, 0, 4),
                ClockTag::new(1, 0, 5),
                ClockTag::new(1, 0, 6),
            ],
            allow_gaps,
        )
        .unwrap();

        // Repeating clock tags in random order is allowed, as long as the end is in order
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                // Repeats 1:0:0-2 a few more times in random order
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                // Adds 1:0:3 on top of it
                ClockTag::new(1, 0, 3),
            ],
            allow_gaps,
        )
        .unwrap();

        // Repeating clock tag sequence must not miss clock tags at the end
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                // Misses 1:0:2
            ],
            allow_gaps,
        )
        .unwrap_err();

        // Repeating clock tag sequence must only miss clock tags in the middle if specified
        let result = check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 0, 0),
                // Misses 1:0:1
                ClockTag::new(1, 0, 2),
            ],
            allow_gaps,
        );
        assert_eq!(result.is_ok(), allow_gaps);

        // Skipping a clock ID is allowed
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                // Skipped clock ID 1
                ClockTag::new(1, 2, 10),
                ClockTag::new(1, 2, 11),
                ClockTag::new(1, 2, 12),
            ],
            allow_gaps,
        )
        .unwrap();

        // Intermixed repeating clock tag sequence is allowed
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 1, 0),
                ClockTag::new(2, 0, 0),
                ClockTag::new(2, 0, 1),
                ClockTag::new(2, 0, 2),
                ClockTag::new(1, 1, 1),
                ClockTag::new(2, 0, 0),
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 1, 2),
                ClockTag::new(1, 1, 3),
                ClockTag::new(2, 0, 1),
                ClockTag::new(1, 0, 1),
                ClockTag::new(2, 0, 2),
                ClockTag::new(1, 1, 4),
                ClockTag::new(1, 0, 2),
                ClockTag::new(1, 1, 5),
            ],
            allow_gaps,
        )
        .unwrap();

        // Intermixed clock tag sequence where one tick for peer 2 is missing is only allowed if specified
        let result = check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(2, 0, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(2, 0, 1),
                ClockTag::new(1, 0, 2),
                // Misses 2:0:2
                ClockTag::new(1, 0, 3),
                ClockTag::new(2, 0, 3),
            ],
            allow_gaps,
        );
        assert_eq!(result.is_ok(), allow_gaps);

        // Intermixed clock tag sequence where one tick for clock ID 1 is missing is only allowed if specified
        let result = check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(1, 1, 0),
                ClockTag::new(1, 0, 1),
                ClockTag::new(1, 1, 1),
                ClockTag::new(1, 0, 2),
                // Misses 1:1:2
                ClockTag::new(1, 0, 3),
                ClockTag::new(1, 1, 3),
            ],
            allow_gaps,
        );
        assert_eq!(result.is_ok(), allow_gaps);

        // Intermixed clock tag sequence where one tick is missing is not allowed
        check_clock_tag_ordering_property(
            &[
                ClockTag::new(1, 0, 0),
                ClockTag::new(2, 0, 0),
                ClockTag::new(3, 0, 0),
                ClockTag::new(3, 0, 1),
                ClockTag::new(1, 0, 1),
                ClockTag::new(2, 0, 1),
                ClockTag::new(3, 0, 2),
                ClockTag::new(2, 0, 2),
                ClockTag::new(1, 0, 2),
                // Peer 2 only partially recovering here, missing 2:0:2
                ClockTag::new(2, 0, 0),
                ClockTag::new(2, 0, 1),
                // Peer 1 and 3 continue
                ClockTag::new(1, 0, 3),
                ClockTag::new(1, 0, 4),
                ClockTag::new(3, 0, 3),
                ClockTag::new(3, 0, 4),
            ],
            allow_gaps,
        )
        .unwrap_err();
    }
}
