//! Check whether other replicas have the operations a WAL delta transfer treats as orphans.
//!
//! When the remote shard of a WAL delta transfer is ahead of us on its own clocks, we assume no
//! other replica has its operations above our ticks, and overwrite them on the remote shard. If
//! another active replica has them, they are not orphan operations: we missed them, and after the
//! transfer the remote shard and we differ from that replica. The transfer can't fix that, but we
//! can report it.

use std::collections::HashMap;
use std::time::Duration;

use futures::future;

use crate::operations::ClockTag;
use crate::shards::CollectionId;
use crate::shards::local_shard::clock_map::RecoveryPoint;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardId};

/// Timeout for asking another replica for its recovery point.
const RECOVERY_POINT_TIMEOUT: Duration = Duration::from_secs(10);

/// What another replica has on a clock the remote shard is ahead of us on.
#[derive(Clone, Debug, PartialEq, Eq)]
enum OtherReplicaClock {
    /// The other replica has the remote shard's newest operation of the clock, which we don't
    HasNewestOperation { clock_tag: ClockTag },
    /// The other replica is ahead of us on the clock, possibly only with updates still in flight
    /// to us
    Ahead { clock_tag: ClockTag, our_tick: u64 },
}

/// Compare another replica's recovery point with the clocks the remote shard is ahead of us on.
///
/// `ahead_clocks` are the remote shard's clocks it is ahead of us on, and `newest_clocks` are ours.
fn compare_other_replica_clocks(
    ahead_clocks: &RecoveryPoint,
    newest_clocks: &RecoveryPoint,
    other_recovery_point: &RecoveryPoint,
) -> Vec<OtherReplicaClock> {
    let remote_tags: HashMap<_, _> = ahead_clocks
        .iter_as_clock_tags()
        .map(|tag| ((tag.peer_id, tag.clock_id), tag))
        .collect();

    let our_ticks: HashMap<_, _> = newest_clocks
        .iter_as_clock_tags()
        .map(|tag| ((tag.peer_id, tag.clock_id), tag.clock_tick))
        .collect();

    let mut findings = Vec::new();

    for clock_tag in other_recovery_point.iter_as_clock_tags() {
        let key = (clock_tag.peer_id, clock_tag.clock_id);

        let Some(remote_tag) = remote_tags.get(&key) else {
            continue;
        };

        let our_tick = our_ticks.get(&key).copied().unwrap_or(0);

        // The same tick and token is the same operation
        let has_newest_operation =
            clock_tag.clock_tick == remote_tag.clock_tick && clock_tag.token == remote_tag.token;

        if has_newest_operation {
            findings.push(OtherReplicaClock::HasNewestOperation { clock_tag });
        } else if clock_tag.clock_tick > our_tick {
            findings.push(OtherReplicaClock::Ahead {
                clock_tag,
                our_tick,
            });
        }
    }

    findings
}

/// Log whether other active replicas have the operations we treat as orphans on the remote shard.
///
/// Best effort: replicas that fail to answer or don't answer in time are skipped. Doesn't change
/// the transfer.
pub(super) async fn log_other_replica_clocks(
    replica_set: &ShardReplicaSet,
    remote_shard: &RemoteShard,
    collection_id: &CollectionId,
    shard_id: ShardId,
    ahead_clocks: &RecoveryPoint,
    newest_clocks: &RecoveryPoint,
) {
    let other_replicas: Vec<_> = replica_set
        .active_remote_shards()
        .await
        .into_iter()
        .filter(|other| other.peer_id != remote_shard.peer_id)
        .collect();

    let recovery_points = future::join_all(other_replicas.iter().map(|other| async move {
        let recovery_point = tokio::time::timeout(
            RECOVERY_POINT_TIMEOUT,
            other.shard_recovery_point(collection_id, shard_id),
        )
        .await;

        (other.peer_id, recovery_point)
    }))
    .await;

    for (peer_id, recovery_point) in recovery_points {
        let recovery_point = match recovery_point {
            Ok(Ok(recovery_point)) => recovery_point,

            Ok(Err(err)) => {
                log::debug!(
                    "Can't check orphan operations of peer {} on peer {peer_id}: {err}",
                    remote_shard.peer_id,
                );
                continue;
            }

            Err(_) => {
                log::debug!(
                    "Can't check orphan operations of peer {} on peer {peer_id}: \
                     no recovery point within {RECOVERY_POINT_TIMEOUT:?}",
                    remote_shard.peer_id,
                );
                continue;
            }
        };

        let findings = compare_other_replica_clocks(ahead_clocks, newest_clocks, &recovery_point);

        for finding in findings {
            log_finding(peer_id, remote_shard.peer_id, &finding);
        }
    }
}

fn log_finding(peer_id: PeerId, remote_peer_id: PeerId, finding: &OtherReplicaClock) {
    match finding {
        OtherReplicaClock::HasNewestOperation { clock_tag } => log::warn!(
            "Peer {peer_id} has the newest operation of clock {}({}) at tick {}, which peer \
             {remote_peer_id} has and we don't. It is not an orphan operation: we missed it, and \
             overwriting it on peer {remote_peer_id} leaves peer {peer_id} different from us",
            clock_tag.peer_id,
            clock_tag.clock_id,
            clock_tag.clock_tick,
        ),

        OtherReplicaClock::Ahead {
            clock_tag,
            our_tick,
        } => log::info!(
            "Peer {peer_id} is ahead of us on clock {}({}) of peer {remote_peer_id}, at tick {} \
             while we are at tick {our_tick}. It may have operations we missed, or updates still \
             in flight to us",
            clock_tag.peer_id,
            clock_tag.clock_id,
            clock_tag.clock_tick,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REMOTE_PEER_ID: PeerId = 1;

    fn recovery_point(clocks: &[(u32, u64, u64)]) -> RecoveryPoint {
        let mut recovery_point = RecoveryPoint::default();
        for &(clock_id, tick, token) in clocks {
            recovery_point.insert_with_token(REMOTE_PEER_ID, clock_id, tick, token);
        }
        recovery_point
    }

    #[test]
    fn test_compare_other_replica_clocks() {
        // The remote shard is ahead of us on clocks 0, 1 and 2
        let ahead_clocks = recovery_point(&[(0, 5, 50), (1, 5, 51), (2, 5, 52)]);
        let newest_clocks = recovery_point(&[(0, 4, 40), (1, 4, 41), (2, 4, 42), (3, 9, 90)]);

        let other_recovery_point = recovery_point(&[
            // Same tick and token as the remote shard
            (0, 5, 50),
            // Same tick, but another token: ahead of us
            (1, 5, 99),
            // Same tick as ours: nothing to report
            (2, 4, 42),
            // A clock the remote shard isn't ahead on: nothing to report
            (3, 10, 100),
        ]);

        let mut findings =
            compare_other_replica_clocks(&ahead_clocks, &newest_clocks, &other_recovery_point);
        findings.sort_by_key(|finding| match finding {
            OtherReplicaClock::HasNewestOperation { clock_tag } => clock_tag.clock_id,
            OtherReplicaClock::Ahead { clock_tag, .. } => clock_tag.clock_id,
        });

        assert_eq!(
            findings,
            vec![
                OtherReplicaClock::HasNewestOperation {
                    clock_tag: ClockTag::new_with_token(REMOTE_PEER_ID, 0, 5, 50),
                },
                OtherReplicaClock::Ahead {
                    clock_tag: ClockTag::new_with_token(REMOTE_PEER_ID, 1, 5, 99),
                    our_tick: 4,
                },
            ],
        );
    }

    #[test]
    fn test_compare_other_replica_clocks_unknown_to_us() {
        // The remote shard has a clock we don't know
        let ahead_clocks = recovery_point(&[(0, 1, 10)]);
        let newest_clocks = RecoveryPoint::default();

        let other_recovery_point = recovery_point(&[(0, 1, 10)]);

        let findings =
            compare_other_replica_clocks(&ahead_clocks, &newest_clocks, &other_recovery_point);

        assert_eq!(
            findings,
            vec![OtherReplicaClock::HasNewestOperation {
                clock_tag: ClockTag::new_with_token(REMOTE_PEER_ID, 0, 1, 10),
            }],
        );
    }
}
