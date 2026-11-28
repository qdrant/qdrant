use crate::shards::local_shard::clock_map::RecoveryPoint;
use crate::shards::shard::PeerId;

/// Clocks on which a recovering peer is ahead of the source of a WAL delta
///
/// A WAL delta can't send records the source doesn't have. The recovering peer can be ahead on
/// its own clocks when it wrote orphan operations: it wrote an update to its local WAL as update
/// leader, and died before sending it to the other replicas. A WAL delta transfer repairs these by
/// overwriting the points they changed. If the recovering peer is ahead on clocks of other peers,
/// we can't tell why, and the WAL delta fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AheadClocks {
    /// The recovering peer is not ahead on any clock
    None,
    /// The recovering peer is ahead only on its own clocks, a WAL delta can repair them
    Own(RecoveryPoint),
    /// The recovering peer is ahead on clocks of other peers, a WAL delta can't repair them
    OtherPeers(RecoveryPoint),
}

impl AheadClocks {
    /// Compare the `recovery_point` of `recovering_peer` with the `newest_clocks` of the source
    ///
    /// Clocks the source doesn't know count as ahead.
    pub fn new(
        recovery_point: &RecoveryPoint,
        newest_clocks: &RecoveryPoint,
        recovering_peer: PeerId,
    ) -> Self {
        let ahead_clocks = recovery_point.clocks_newer_than(newest_clocks);

        if ahead_clocks.is_empty() {
            Self::None
        } else if ahead_clocks.has_clocks_of_other_peers(recovering_peer) {
            Self::OtherPeers(ahead_clocks)
        } else {
            Self::Own(ahead_clocks)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECOVERING_PEER: PeerId = 1;
    const OTHER_PEER: PeerId = 2;

    #[test]
    fn test_not_ahead() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert(RECOVERING_PEER, 0, 5);
        recovery_point.insert(OTHER_PEER, 0, 3);

        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert(RECOVERING_PEER, 0, 5);
        newest_clocks.insert(OTHER_PEER, 0, 4);
        newest_clocks.insert(OTHER_PEER, 1, 1);

        let ahead_clocks = AheadClocks::new(&recovery_point, &newest_clocks, RECOVERING_PEER);

        assert_eq!(ahead_clocks, AheadClocks::None);
    }

    /// A higher tick and an unknown clock of the recovering peer are both its own ahead clocks
    #[test]
    fn test_ahead_on_own_clocks() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(RECOVERING_PEER, 0, 6, 60);
        recovery_point.insert_with_token(RECOVERING_PEER, 1, 1, 10);
        recovery_point.insert(OTHER_PEER, 0, 3);

        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert(RECOVERING_PEER, 0, 5);
        newest_clocks.insert(OTHER_PEER, 0, 4);

        let ahead_clocks = AheadClocks::new(&recovery_point, &newest_clocks, RECOVERING_PEER);

        let mut expected = RecoveryPoint::default();
        expected.insert_with_token(RECOVERING_PEER, 0, 6, 60);
        expected.insert_with_token(RECOVERING_PEER, 1, 1, 10);

        assert_eq!(ahead_clocks, AheadClocks::Own(expected));
    }

    #[test]
    fn test_ahead_on_clocks_of_other_peers() {
        let mut recovery_point = RecoveryPoint::default();
        recovery_point.insert_with_token(RECOVERING_PEER, 0, 6, 60);
        recovery_point.insert_with_token(OTHER_PEER, 0, 5, 50);

        let mut newest_clocks = RecoveryPoint::default();
        newest_clocks.insert(RECOVERING_PEER, 0, 5);
        newest_clocks.insert(OTHER_PEER, 0, 4);

        let ahead_clocks = AheadClocks::new(&recovery_point, &newest_clocks, RECOVERING_PEER);

        let mut expected = RecoveryPoint::default();
        expected.insert_with_token(RECOVERING_PEER, 0, 6, 60);
        expected.insert_with_token(OTHER_PEER, 0, 5, 50);

        assert_eq!(ahead_clocks, AheadClocks::OtherPeers(expected));
    }
}
