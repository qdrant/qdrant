//! Find the points that orphan operations in a WAL changed.
//!
//! An orphan operation exists only on this replica. It happens when the update leader writes an
//! operation to its local WAL and dies before it sends the operation to the other replicas. On a
//! WAL delta transfer to this replica, the source sees this replica ahead on the leader's clock,
//! and cannot resolve a delta. Instead, the source can overwrite the points the orphan operations
//! changed with its own versions of them.

use std::collections::{HashMap, HashSet};

use segment::types::PointIdType;
use shard::operations::affected_points::AffectedPoints;
use thiserror::Error;

use crate::operations::{ClockTag, OperationWithClockTag};
use crate::shards::local_shard::clock_map::RecoveryPoint;
use crate::shards::shard::PeerId;

/// Maximum number of points that orphan operations may have changed.
///
/// Bounds the size of the orphan points response and the number of points a transfer has to
/// overwrite. Above it, a full shard transfer is used instead.
pub const MAX_ORPHAN_POINTS: usize = 10_000;

/// Points that orphan operations changed, found by [`find_orphan_points`].
#[derive(Clone, Debug, Default)]
pub struct OrphanPoints {
    /// Points the orphan operations can have changed
    pub point_ids: HashSet<PointIdType>,
    /// For each clock, the tag of the last record at or below the source's tick, if any
    pub clocks: Vec<ClockTag>,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum OrphanPointsError {
    #[error("WAL may be missing records of clock {peer_id}({clock_id}) above tick {tick}")]
    Incomplete {
        peer_id: PeerId,
        clock_id: u32,
        tick: u64,
    },
    #[error("WAL record {op_num} can change points it doesn't list")]
    UnknownPoints { op_num: u64 },
    #[error("orphan operations changed more than {max_points} points")]
    TooManyPoints { max_points: usize },
    #[error("failed to read WAL: {0}")]
    WalReadError(String),
}

/// Find the points changed by orphan operations in WAL `records`.
///
/// `source_clocks` holds the transfer source's tick for each clock this replica is ahead on, or
/// tick 0 for a clock the source doesn't know. Records of these clocks with a higher tick are
/// orphan operations.
///
/// Fails if:
/// - the WAL may be missing orphan records of a clock, because it is truncated
/// - an orphan operation can change points it doesn't list
/// - orphan operations changed more than `max_points` points
pub fn find_orphan_points(
    records: impl Iterator<Item = shard::wal::Result<(u64, OperationWithClockTag)>>,
    source_clocks: &RecoveryPoint,
    max_points: usize,
) -> Result<OrphanPoints, OrphanPointsError> {
    let source_ticks: HashMap<_, _> = source_clocks
        .iter_as_clock_tags()
        .map(|tag| ((tag.peer_id, tag.clock_id), tag.clock_tick))
        .collect();

    let mut earliest_ticks = HashMap::new();
    let mut clocks = HashMap::new();
    let mut point_ids = HashSet::new();

    for record in records {
        let (op_num, operation) =
            record.map_err(|err| OrphanPointsError::WalReadError(err.to_string()))?;

        let Some(clock_tag) = operation.clock_tag else {
            continue;
        };

        let key = (clock_tag.peer_id, clock_tag.clock_id);

        let Some(&source_tick) = source_ticks.get(&key) else {
            continue;
        };

        earliest_ticks.entry(key).or_insert(clock_tag.clock_tick);

        if clock_tag.clock_tick <= source_tick {
            clocks.insert(key, clock_tag);
            continue;
        }

        match operation.operation.affected_points() {
            AffectedPoints::Ids(ids) => point_ids.extend(ids),
            AffectedPoints::Nothing => (),
            AffectedPoints::Unknown => return Err(OrphanPointsError::UnknownPoints { op_num }),
        }

        if point_ids.len() > max_points {
            return Err(OrphanPointsError::TooManyPoints { max_points });
        }
    }

    // Truncation removes records from the start of the WAL. If the earliest record of a clock is
    // at most one tick above the source's, no orphan record of that clock was truncated.
    for (&(peer_id, clock_id), &source_tick) in &source_ticks {
        let is_complete = earliest_ticks
            .get(&(peer_id, clock_id))
            .is_some_and(|&tick| tick <= source_tick + 1);

        if !is_complete {
            return Err(OrphanPointsError::Incomplete {
                peer_id,
                clock_id,
                tick: source_tick,
            });
        }
    }

    Ok(OrphanPoints {
        point_ids,
        clocks: clocks.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::VectorStructInternal;
    use segment::types::Filter;

    use super::*;
    use crate::operations::CollectionUpdateOperations;
    use crate::operations::point_ops::{
        PointInsertOperationsInternal, PointOperations, PointStructPersisted,
    };

    const PEER_ID: PeerId = 1;
    const OTHER_PEER_ID: PeerId = 2;

    fn upsert(id: u64) -> CollectionUpdateOperations {
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
                id: id.into(),
                vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0]).into(),
                payload: None,
            }]),
        ))
    }

    fn delete(id: u64) -> CollectionUpdateOperations {
        CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
            ids: vec![id.into()],
        })
    }

    fn delete_by_filter() -> CollectionUpdateOperations {
        CollectionUpdateOperations::PointOperation(PointOperations::DeletePointsByFilter(
            Filter::default(),
        ))
    }

    /// WAL records with the given operations and clock tags, numbered from 0
    fn records(
        operations: Vec<(CollectionUpdateOperations, Option<ClockTag>)>,
    ) -> impl Iterator<Item = shard::wal::Result<(u64, OperationWithClockTag)>> {
        operations
            .into_iter()
            .enumerate()
            .map(|(op_num, (operation, clock_tag))| {
                Ok((
                    op_num as u64,
                    OperationWithClockTag::new(operation, clock_tag),
                ))
            })
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "records take an optional clock tag"
    )]
    fn tag(peer_id: PeerId, tick: u64) -> Option<ClockTag> {
        Some(ClockTag::new_with_token(peer_id, 0, tick, tick))
    }

    fn source_clocks(ticks: &[(PeerId, u64)]) -> RecoveryPoint {
        let mut source_clocks = RecoveryPoint::default();
        for &(peer_id, tick) in ticks {
            source_clocks.insert_with_token(peer_id, 0, tick, 0);
        }
        source_clocks
    }

    fn ids(ids: &[u64]) -> HashSet<PointIdType> {
        ids.iter().map(|&id| PointIdType::from(id)).collect()
    }

    #[test]
    fn test_find_orphan_points() {
        let records = records(vec![
            (upsert(1), tag(PEER_ID, 1)),
            (upsert(2), tag(PEER_ID, 2)),
            (upsert(10), tag(OTHER_PEER_ID, 5)),
            (upsert(3), tag(PEER_ID, 3)),
            (upsert(11), None),
            (delete(4), tag(PEER_ID, 4)),
        ]);

        let orphan_points = find_orphan_points(records, &source_clocks(&[(PEER_ID, 2)]), 100);

        let OrphanPoints { point_ids, clocks } = orphan_points.unwrap();
        assert_eq!(point_ids, ids(&[3, 4]));
        assert_eq!(clocks, vec![tag(PEER_ID, 2).unwrap()]);
    }

    #[test]
    fn test_find_orphan_points_unknown_clock() {
        let records = records(vec![
            (upsert(1), tag(PEER_ID, 1)),
            (upsert(2), tag(PEER_ID, 2)),
        ]);

        let orphan_points = find_orphan_points(records, &source_clocks(&[(PEER_ID, 0)]), 100);

        let OrphanPoints { point_ids, clocks } = orphan_points.unwrap();
        assert_eq!(point_ids, ids(&[1, 2]));
        assert!(clocks.is_empty());
    }

    #[test]
    fn test_find_orphan_points_truncated() {
        // The first orphan tick is in the WAL
        let records_from_first_orphan = records(vec![
            (upsert(3), tag(PEER_ID, 3)),
            (upsert(4), tag(PEER_ID, 4)),
        ]);

        let orphan_points = find_orphan_points(
            records_from_first_orphan,
            &source_clocks(&[(PEER_ID, 2)]),
            100,
        );

        let OrphanPoints { point_ids, clocks } = orphan_points.unwrap();
        assert_eq!(point_ids, ids(&[3, 4]));
        assert!(clocks.is_empty());

        // Tick 3 may be truncated
        let records_from_second_orphan = records(vec![(upsert(4), tag(PEER_ID, 4))]);

        let orphan_points = find_orphan_points(
            records_from_second_orphan,
            &source_clocks(&[(PEER_ID, 2)]),
            100,
        );

        assert_eq!(
            orphan_points.unwrap_err(),
            OrphanPointsError::Incomplete {
                peer_id: PEER_ID,
                clock_id: 0,
                tick: 2,
            },
        );

        // No records of the clock at all
        let records_of_other_clock = records(vec![(upsert(10), tag(OTHER_PEER_ID, 5))]);

        let orphan_points =
            find_orphan_points(records_of_other_clock, &source_clocks(&[(PEER_ID, 2)]), 100);

        assert!(matches!(
            orphan_points,
            Err(OrphanPointsError::Incomplete { .. }),
        ));
    }

    #[test]
    fn test_find_orphan_points_unknown_points() {
        // Below the source's tick, the operation is not an orphan
        let records_with_known_filter = records(vec![
            (delete_by_filter(), tag(PEER_ID, 1)),
            (upsert(2), tag(PEER_ID, 2)),
        ]);

        let orphan_points = find_orphan_points(
            records_with_known_filter,
            &source_clocks(&[(PEER_ID, 1)]),
            100,
        );

        assert_eq!(orphan_points.unwrap().point_ids, ids(&[2]));

        // Above the source's tick, we can't tell which points it changed
        let records_with_orphan_filter = records(vec![
            (upsert(1), tag(PEER_ID, 1)),
            (delete_by_filter(), tag(PEER_ID, 2)),
        ]);

        let orphan_points = find_orphan_points(
            records_with_orphan_filter,
            &source_clocks(&[(PEER_ID, 1)]),
            100,
        );

        assert_eq!(
            orphan_points.unwrap_err(),
            OrphanPointsError::UnknownPoints { op_num: 1 },
        );
    }

    #[test]
    fn test_find_orphan_points_too_many_points() {
        let records = records(vec![
            (upsert(1), tag(PEER_ID, 1)),
            (upsert(2), tag(PEER_ID, 2)),
            (upsert(3), tag(PEER_ID, 3)),
        ]);

        let orphan_points = find_orphan_points(records, &source_clocks(&[(PEER_ID, 1)]), 1);

        assert_eq!(
            orphan_points.unwrap_err(),
            OrphanPointsError::TooManyPoints { max_points: 1 },
        );
    }
}
