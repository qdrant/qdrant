use std::cmp::Reverse;

use super::ahead_clocks::AheadClocks;
use super::{WalDeltaBounds, WalDeltaError, check_clocks};
use crate::shards::local_shard::clock_map::RecoveryPoint;
use crate::shards::shard::PeerId;

/// How likely a source is to resolve a WAL delta for a recovering peer
///
/// The rank comes from the source's clocks and WAL delta bounds, without reading its WAL. It
/// runs the clock checks of [`super::resolve_wal_delta`] exactly, and estimates whether the WAL
/// scan reaches far enough back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRank {
    /// The source has every clock of the recovery point, at its tick or above
    Covers {
        /// How many more records the WAL delta scan could read than the delta likely has
        spare_records: u64,
    },
    /// The recovering peer is ahead of the source only on its own clocks
    ///
    /// The WAL delta transfer repairs the recovering peer's orphan operations.
    NeedsRepair {
        /// How many more records the WAL delta scan could read than the delta likely has
        spare_records: u64,
    },
    /// The source didn't report its WAL delta bounds, on an older version
    Unknown,
    /// The source likely can't resolve the WAL delta
    Fails(SourceRankFailure),
}

/// Why a source likely can't resolve a WAL delta, see [`SourceRank::Fails`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRankFailure {
    /// The recovering peer is ahead on clocks of other peers
    AheadOnClocksOfOtherPeers(RecoveryPoint),
    /// A clock check fails
    Clocks(WalDeltaError),
    /// The delta likely has more records than the WAL delta scan can read
    TaggedRecords { records: u64, tagged_records: u64 },
}

impl SourceRank {
    /// Rank a source for the `recovery_point` of `recovering_peer`
    ///
    /// `newest_clocks` is the source's recovery point, and `bounds` are its WAL delta bounds.
    pub fn new(
        recovery_point: &RecoveryPoint,
        recovering_peer: PeerId,
        newest_clocks: &RecoveryPoint,
        bounds: Option<&WalDeltaBounds>,
    ) -> Self {
        let Some(bounds) = bounds else {
            return Self::Unknown;
        };

        let ahead_clocks = AheadClocks::new(recovery_point, newest_clocks, recovering_peer);

        let mut recovery_point = recovery_point.clone();

        let needs_repair = match ahead_clocks {
            AheadClocks::None => false,

            AheadClocks::OtherPeers(ahead_clocks) => {
                let failure = SourceRankFailure::AheadOnClocksOfOtherPeers(ahead_clocks);
                return Self::Fails(failure);
            }

            AheadClocks::Own(ahead_clocks) => {
                // The WAL delta transfer drops the clocks we don't know, and lowers the others to
                // the recovering peer's last record at or below our tick. Finding that record
                // needs a scan of the recovering peer's WAL, so assume it's at our exact tick.
                for clock_tag in ahead_clocks
                    .same_clocks_in(newest_clocks)
                    .iter_as_clock_tags()
                {
                    if clock_tag.clock_tick == 0 {
                        recovery_point.remove_clock(clock_tag);
                    } else {
                        recovery_point.set_clock(clock_tag);
                    }
                }

                true
            }
        };

        let clock_check = check_clocks(
            recovery_point.clone(),
            newest_clocks.clone(),
            bounds.cutoff.clone(),
        );

        if let Err(err) = clock_check {
            return Self::Fails(SourceRankFailure::Clocks(err));
        }

        // Each update advances one clock by one tick, so the ticks we're ahead approximate the
        // records of the delta
        let records = newest_clocks.ticks_ahead_of(&recovery_point);

        let Some(spare_records) = bounds.tagged_records.checked_sub(records) else {
            let failure = SourceRankFailure::TaggedRecords {
                records,
                tagged_records: bounds.tagged_records,
            };

            return Self::Fails(failure);
        };

        if needs_repair {
            Self::NeedsRepair { spare_records }
        } else {
            Self::Covers { spare_records }
        }
    }

    /// Key to sort sources by, best first
    ///
    /// Sorts by rank, then by the most spare records.
    pub fn sort_key(&self) -> (u8, Reverse<u64>) {
        match self {
            Self::Covers { spare_records } => (0, Reverse(*spare_records)),
            Self::NeedsRepair { spare_records } => (1, Reverse(*spare_records)),
            Self::Unknown => (2, Reverse(0)),
            Self::Fails(_) => (3, Reverse(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECOVERING_PEER: PeerId = 1;
    const OTHER_PEER: PeerId = 2;

    fn bounds(tagged_records: u64) -> WalDeltaBounds {
        WalDeltaBounds {
            cutoff: RecoveryPoint::default(),
            tagged_records,
        }
    }

    #[test]
    fn test_unknown_without_bounds() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(OTHER_PEER, 0, 5, 5);

        let rank = SourceRank::new(&recovery_point, RECOVERING_PEER, &recovery_point, None);

        assert_eq!(rank, SourceRank::Unknown);
    }

    #[test]
    fn test_covers() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(OTHER_PEER, 0, 5, 5);

        // 3 records on a clock the recovering peer knows, 2 on a clock it doesn't
        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert_with_token(OTHER_PEER, 0, 8, 8);
        newest_clocks.insert_with_token(OTHER_PEER, 1, 2, 2);

        let rank = SourceRank::new(
            &recovery_point,
            RECOVERING_PEER,
            &newest_clocks,
            Some(&bounds(10)),
        );

        assert_eq!(rank, SourceRank::Covers { spare_records: 5 });
    }

    #[test]
    fn test_covers_equal() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(OTHER_PEER, 0, 5, 5);

        let rank = SourceRank::new(
            &recovery_point,
            RECOVERING_PEER,
            &recovery_point,
            Some(&bounds(0)),
        );

        assert_eq!(rank, SourceRank::Covers { spare_records: 0 });
    }

    /// The recovering peer is one tick ahead on its own clock, and has an own clock the source
    /// doesn't know. Neither counts as a record of the delta.
    #[test]
    fn test_needs_repair() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(RECOVERING_PEER, 0, 6, 60);
        recovery_point.insert_with_token(RECOVERING_PEER, 1, 1, 10);
        recovery_point.insert_with_token(OTHER_PEER, 0, 5, 5);

        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert_with_token(RECOVERING_PEER, 0, 5, 5);
        newest_clocks.insert_with_token(OTHER_PEER, 0, 7, 7);

        let rank = SourceRank::new(
            &recovery_point,
            RECOVERING_PEER,
            &newest_clocks,
            Some(&bounds(4)),
        );

        assert_eq!(rank, SourceRank::NeedsRepair { spare_records: 2 });
    }

    #[test]
    fn test_fails_ahead_on_clocks_of_other_peers() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(OTHER_PEER, 0, 6, 6);

        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert_with_token(OTHER_PEER, 0, 5, 5);

        let rank = SourceRank::new(
            &recovery_point,
            RECOVERING_PEER,
            &newest_clocks,
            Some(&bounds(10)),
        );

        assert!(matches!(
            rank,
            SourceRank::Fails(SourceRankFailure::AheadOnClocksOfOtherPeers(_)),
        ));
    }

    #[test]
    fn test_fails_cutoff() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(OTHER_PEER, 0, 5, 5);

        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert_with_token(OTHER_PEER, 0, 8, 8);

        let mut cutoff = RecoveryPoint::default();
        cutoff.insert_with_token(OTHER_PEER, 0, 6, 6);

        let bounds = WalDeltaBounds {
            cutoff,
            tagged_records: 10,
        };

        let rank = SourceRank::new(
            &recovery_point,
            RECOVERING_PEER,
            &newest_clocks,
            Some(&bounds),
        );

        assert_eq!(
            rank,
            SourceRank::Fails(SourceRankFailure::Clocks(WalDeltaError::Cutoff)),
        );
    }

    /// The source didn't see the recovering peer's own clock at all. The repair drops it, and the
    /// recovery point is empty.
    #[test]
    fn test_fails_repair_leaves_empty_recovery_point() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(RECOVERING_PEER, 0, 1, 10);

        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert_with_token(OTHER_PEER, 0, 3, 3);

        let rank = SourceRank::new(
            &recovery_point,
            RECOVERING_PEER,
            &newest_clocks,
            Some(&bounds(10)),
        );

        assert_eq!(
            rank,
            SourceRank::Fails(SourceRankFailure::Clocks(WalDeltaError::Empty)),
        );
    }

    #[test]
    fn test_fails_tagged_records() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(OTHER_PEER, 0, 5, 5);

        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert_with_token(OTHER_PEER, 0, 8, 8);

        let rank = SourceRank::new(
            &recovery_point,
            RECOVERING_PEER,
            &newest_clocks,
            Some(&bounds(2)),
        );

        let failure = SourceRankFailure::TaggedRecords {
            records: 3,
            tagged_records: 2,
        };

        assert_eq!(rank, SourceRank::Fails(failure));
    }

    #[test]
    fn test_sort_key() {
        let failure = SourceRankFailure::Clocks(WalDeltaError::Cutoff);

        let mut ranks = vec![
            SourceRank::Fails(failure.clone()),
            SourceRank::Unknown,
            SourceRank::NeedsRepair { spare_records: 9 },
            SourceRank::Covers { spare_records: 1 },
            SourceRank::Covers { spare_records: 5 },
        ];

        ranks.sort_by_key(SourceRank::sort_key);

        let expected = vec![
            SourceRank::Covers { spare_records: 5 },
            SourceRank::Covers { spare_records: 1 },
            SourceRank::NeedsRepair { spare_records: 9 },
            SourceRank::Unknown,
            SourceRank::Fails(failure),
        ];

        assert_eq!(ranks, expected);
    }
}
