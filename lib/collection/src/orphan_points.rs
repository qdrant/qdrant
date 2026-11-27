//! Find the points that orphan operations in a WAL changed.
//!
//! An orphan operation exists only on this replica. It happens when the update leader writes an
//! operation to its local WAL and dies before it sends the operation to the other replicas. On a
//! WAL delta transfer to this replica, the source sees this replica ahead on the leader's clock,
//! and cannot resolve a delta. Instead, the source can overwrite the points the orphan operations
//! changed with its own versions of them.

use std::collections::{HashMap, HashSet};

use api::grpc::qdrant::RecoveryPointClockTag;
use segment::types::PointIdType;
use shard::operations::affected_points::AffectedPoints;
use thiserror::Error;
use tonic::Status;

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
    /// For each clock the source knows, the tag of the last non-forced record at or below the
    /// source's tick
    pub clocks: Vec<ClockTag>,
}

impl From<OrphanPoints> for api::grpc::qdrant::OrphanPoints {
    fn from(orphan_points: OrphanPoints) -> Self {
        let OrphanPoints { point_ids, clocks } = orphan_points;

        let point_ids = point_ids
            .into_iter()
            .map(api::grpc::qdrant::PointId::from)
            .collect();

        let clocks = clocks
            .into_iter()
            .map(|clock_tag| {
                let ClockTag {
                    peer_id,
                    clock_id,
                    clock_tick,
                    token,
                    force: _,
                } = clock_tag;

                RecoveryPointClockTag {
                    peer_id,
                    clock_id,
                    clock_tick,
                    token,
                }
            })
            .collect();

        Self { point_ids, clocks }
    }
}

impl TryFrom<api::grpc::qdrant::OrphanPoints> for OrphanPoints {
    type Error = Status;

    fn try_from(orphan_points: api::grpc::qdrant::OrphanPoints) -> Result<Self, Self::Error> {
        let api::grpc::qdrant::OrphanPoints { point_ids, clocks } = orphan_points;

        let point_ids = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<Result<_, _>>()?;

        let clocks = clocks
            .into_iter()
            .map(|clock_tag| {
                let RecoveryPointClockTag {
                    peer_id,
                    clock_id,
                    clock_tick,
                    token,
                } = clock_tag;

                ClockTag::new_with_token(peer_id, clock_id, clock_tick, token)
            })
            .collect();

        Ok(Self { point_ids, clocks })
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum OrphanPointsError {
    #[error("WAL may be missing records of clock {peer_id}({clock_id}) above tick {tick}")]
    Incomplete {
        peer_id: PeerId,
        clock_id: u32,
        tick: u64,
    },
    #[error("WAL has no non-forced record of clock {peer_id}({clock_id}) at or below tick {tick}")]
    MissingRecord {
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
/// Forced records, written by shard transfers, can have a lower tick than records before them, so
/// they don't count as a record at or below the source's tick, or as the first record of a clock.
/// Forced records above the source's tick still count as orphan operations.
///
/// Fails if:
/// - the WAL has no non-forced record at or below the source's tick of a clock the source knows,
///   so the source can't verify that both replicas have the same operation there
/// - the WAL may be missing orphan records of a clock the source doesn't know, because it is
///   truncated
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

    let mut earliest_own_ticks = HashMap::new();
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

        // A forced record can have a lower tick than records before it, so it tells nothing about
        // which records were truncated. Only records that passed the clock check do: their ticks
        // never go down through the WAL.
        if !clock_tag.force {
            earliest_own_ticks
                .entry(key)
                .or_insert(clock_tag.clock_tick);
        }

        if clock_tag.clock_tick <= source_tick {
            if !clock_tag.force {
                clocks.insert(key, clock_tag);
            }

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

    // Truncation removes records from the start of the WAL. Orphan records passed the clock check,
    // because this replica wrote them as update leader, so they come after every non-forced record
    // of the same clock with a lower tick. If a non-forced record at or below the source's tick is
    // still in the WAL, no orphan record of that clock was truncated. The source also verifies that
    // record against its own WAL, so it is required. A clock the source doesn't know has no such
    // record, so its first record, at tick 1, must still be in the WAL instead.
    for (&(peer_id, clock_id), &source_tick) in &source_ticks {
        let key = (peer_id, clock_id);

        if source_tick == 0 {
            let has_first_record = earliest_own_ticks.get(&key).is_some_and(|&tick| tick <= 1);

            if !has_first_record {
                return Err(OrphanPointsError::Incomplete {
                    peer_id,
                    clock_id,
                    tick: source_tick,
                });
            }
        } else if !clocks.contains_key(&key) {
            return Err(OrphanPointsError::MissingRecord {
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

    /// Tag of a forced record, which a shard transfer writes
    #[expect(
        clippy::unnecessary_wraps,
        reason = "records take an optional clock tag"
    )]
    fn forced_tag(peer_id: PeerId, tick: u64) -> Option<ClockTag> {
        Some(ClockTag::new_with_token(peer_id, 0, tick, tick).force(true))
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
    fn test_find_orphan_points_missing_record() {
        let missing_record = OrphanPointsError::MissingRecord {
            peer_id: PEER_ID,
            clock_id: 0,
            tick: 2,
        };

        // All orphan records are in the WAL, but not the record at the source's tick
        let records_from_first_orphan = records(vec![
            (upsert(3), tag(PEER_ID, 3)),
            (upsert(4), tag(PEER_ID, 4)),
        ]);

        let orphan_points = find_orphan_points(
            records_from_first_orphan,
            &source_clocks(&[(PEER_ID, 2)]),
            100,
        );

        assert_eq!(orphan_points.unwrap_err(), missing_record);

        // No records of the clock at all
        let records_of_other_clock = records(vec![(upsert(10), tag(OTHER_PEER_ID, 5))]);

        let orphan_points =
            find_orphan_points(records_of_other_clock, &source_clocks(&[(PEER_ID, 2)]), 100);

        assert_eq!(orphan_points.unwrap_err(), missing_record);
    }

    #[test]
    fn test_find_orphan_points_truncated() {
        let incomplete = OrphanPointsError::Incomplete {
            peer_id: PEER_ID,
            clock_id: 0,
            tick: 0,
        };

        // Tick 1 of a clock the source doesn't know may be truncated
        let records_from_second_tick = records(vec![
            (upsert(2), tag(PEER_ID, 2)),
            (upsert(3), tag(PEER_ID, 3)),
        ]);

        let orphan_points = find_orphan_points(
            records_from_second_tick,
            &source_clocks(&[(PEER_ID, 0)]),
            100,
        );

        assert_eq!(orphan_points.unwrap_err(), incomplete);

        // No records of the clock at all
        let records_of_other_clock = records(vec![(upsert(10), tag(OTHER_PEER_ID, 5))]);

        let orphan_points =
            find_orphan_points(records_of_other_clock, &source_clocks(&[(PEER_ID, 0)]), 100);

        assert_eq!(orphan_points.unwrap_err(), incomplete);
    }

    #[test]
    fn test_find_orphan_points_forced_records() {
        let missing_record = OrphanPointsError::MissingRecord {
            peer_id: PEER_ID,
            clock_id: 0,
            tick: 2,
        };

        // The orphan record at tick 3 was truncated, before a forced copy of tick 2 was written
        let records_from_forced_copy = records(vec![
            (upsert(2), forced_tag(PEER_ID, 2)),
            (upsert(4), tag(PEER_ID, 4)),
        ]);

        let orphan_points = find_orphan_points(
            records_from_forced_copy,
            &source_clocks(&[(PEER_ID, 2)]),
            100,
        );

        assert_eq!(orphan_points.unwrap_err(), missing_record);

        // Same WAL without truncation. The boundary is the non-forced record at tick 2, and a forced
        // record above the source's tick is an orphan too
        let records_not_truncated = records(vec![
            (upsert(1), tag(PEER_ID, 1)),
            (upsert(2), tag(PEER_ID, 2)),
            (upsert(3), tag(PEER_ID, 3)),
            (upsert(2), forced_tag(PEER_ID, 2)),
            (upsert(4), tag(PEER_ID, 4)),
            (upsert(5), forced_tag(PEER_ID, 5)),
        ]);

        let orphan_points =
            find_orphan_points(records_not_truncated, &source_clocks(&[(PEER_ID, 2)]), 100);

        let OrphanPoints { point_ids, clocks } = orphan_points.unwrap();
        assert_eq!(point_ids, ids(&[3, 4, 5]));
        assert_eq!(clocks, vec![tag(PEER_ID, 2).unwrap()]);

        // A forced first record of a clock the source doesn't know doesn't count either
        let records_from_forced_first = records(vec![
            (upsert(1), forced_tag(PEER_ID, 1)),
            (upsert(2), tag(PEER_ID, 2)),
        ]);

        let orphan_points = find_orphan_points(
            records_from_forced_first,
            &source_clocks(&[(PEER_ID, 0)]),
            100,
        );

        assert_eq!(
            orphan_points.unwrap_err(),
            OrphanPointsError::Incomplete {
                peer_id: PEER_ID,
                clock_id: 0,
                tick: 0,
            },
        );
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
    fn test_orphan_points_grpc_round_trip() {
        let orphan_points = OrphanPoints {
            point_ids: ids(&[1, 2, 3]),
            clocks: vec![
                ClockTag::new_with_token(PEER_ID, 0, 5, 50),
                ClockTag::new_with_token(PEER_ID, 1, 7, 70),
            ],
        };

        let grpc = api::grpc::qdrant::OrphanPoints::from(orphan_points.clone());
        let OrphanPoints { point_ids, clocks } = OrphanPoints::try_from(grpc).unwrap();

        assert_eq!(point_ids, orphan_points.point_ids);
        assert_eq!(clocks, orphan_points.clocks);
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
