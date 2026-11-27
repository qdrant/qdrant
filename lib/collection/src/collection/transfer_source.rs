use std::time::Duration;

use futures::future;

use crate::shards::remote_shard::RemoteRecoveryPoint;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::PeerId;
use crate::shards::transfer::ShardTransferMethod;
use crate::wal_delta::source_rank::SourceRank;

/// How long to wait for a candidate source's recovery point
///
/// `sync_local_state` runs on the consensus thread, so waiting delays consensus.
const SOURCE_RECOVERY_POINT_TIMEOUT: Duration = Duration::from_secs(1);

/// Candidate source of an automatic shard transfer to this peer's replica
#[derive(Debug)]
pub(super) struct TransferSource {
    pub peer_id: PeerId,

    /// Rank as the source of a WAL delta, `None` for other transfer methods
    pub rank: Option<SourceRank>,

    /// Whether the source answered our recovery point request
    ///
    /// An answer shows that the source's replica is available.
    pub answered: bool,
}

impl TransferSource {
    fn unranked(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            rank: None,
            answered: false,
        }
    }
}

/// Choose the method of an automatic transfer to this peer's replica, and order the candidate
/// sources, best first
///
/// For a WAL delta, ask the candidates for their recovery points, and sort them by
/// [`SourceRank`]. Ties keep the order of `candidates`. If this peer's recovery point is empty,
/// every WAL delta fails, so choose `default_method` instead. Other methods keep the order of
/// `candidates`.
pub(super) async fn order_transfer_sources(
    collection_name: &str,
    replica_set: &ShardReplicaSet,
    method: ShardTransferMethod,
    default_method: ShardTransferMethod,
    candidates: Vec<PeerId>,
) -> (ShardTransferMethod, Vec<TransferSource>) {
    match method {
        ShardTransferMethod::WalDelta => (),

        ShardTransferMethod::StreamRecords
        | ShardTransferMethod::Snapshot
        | ShardTransferMethod::ReshardingStreamRecords => {
            let sources = candidates
                .into_iter()
                .map(TransferSource::unranked)
                .collect();

            return (method, sources);
        }
    }

    let recovery_point = match replica_set.shard_recovery_point().await {
        Ok(recovery_point) => recovery_point,

        Err(err) => {
            log::debug!(
                "Can't rank WAL delta sources for shard {}:{}, \
                 failed to get the local recovery point: {err}",
                collection_name,
                replica_set.shard_id,
            );

            let sources = candidates
                .into_iter()
                .map(TransferSource::unranked)
                .collect();

            return (method, sources);
        }
    };

    if recovery_point.is_empty() {
        let sources = candidates
            .into_iter()
            .map(TransferSource::unranked)
            .collect();

        return (default_method, sources);
    }

    let this_peer_id = replica_set.this_peer_id();

    let responses = future::join_all(candidates.into_iter().map(|peer_id| async move {
        let response = tokio::time::timeout(
            SOURCE_RECOVERY_POINT_TIMEOUT,
            replica_set.remote_shard_recovery_point(peer_id),
        )
        .await;

        (peer_id, response)
    }))
    .await;

    let mut sources: Vec<_> = responses
        .into_iter()
        .map(|(peer_id, response)| {
            let remote_recovery_point = match response {
                Ok(Ok(remote_recovery_point)) => Some(remote_recovery_point),

                Ok(Err(err)) => {
                    log::trace!("Failed to get recovery point from peer {peer_id}: {err}");
                    None
                }

                Err(_elapsed) => {
                    log::trace!("Timed out getting recovery point from peer {peer_id}");
                    None
                }
            };

            let answered = remote_recovery_point.is_some();

            let rank = match remote_recovery_point {
                Some(RemoteRecoveryPoint {
                    recovery_point: newest_clocks,
                    wal_delta_bounds,
                }) => SourceRank::new(
                    &recovery_point,
                    this_peer_id,
                    &newest_clocks,
                    wal_delta_bounds.as_ref(),
                ),

                None => SourceRank::Unknown,
            };

            (peer_id, rank, answered)
        })
        .collect();

    // Stable sort, so ties keep the order of the candidates
    sources.sort_by_key(|(_peer_id, rank, _answered)| rank.sort_key());

    log::debug!(
        "Ranked WAL delta sources for shard {}:{} on peer {this_peer_id}: {sources:?}",
        collection_name,
        replica_set.shard_id,
    );

    let sources = sources
        .into_iter()
        .map(|(peer_id, rank, answered)| TransferSource {
            peer_id,
            rank: Some(rank),
            answered,
        })
        .collect();

    (method, sources)
}
