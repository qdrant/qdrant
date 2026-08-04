//! Cross-peer view of how far behind each peer is at applying consensus entries.
//!
//! Every peer keeps a small ring of the entries it has applied, stamped with its own wall clock.
//! Collecting those rings from the whole cluster and lining them up on the entry indices that
//! appear in more than one of them gives the delay between peers for the same entry.
//!
//! Each entry is measured from whichever peer applied it first, so a lag is how long the others
//! took to catch up on that entry and is never negative. Which peer is first can differ from entry
//! to entry, so the baseline is per entry rather than a single chosen peer.
//!
//! Timestamps come from different machines, so the delays include whatever clock skew exists
//! between them. A peer whose clock runs behind is first on every entry and reports no lag at all,
//! pushing that skew into everyone else's numbers.

use std::collections::HashMap;
use std::time::Duration;

use api::grpc;
use collection::shards::channel_service::ChannelService;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::Serialize;
use shard::PeerId;
use storage::content_manager::consensus_manager::{AppliedEntry, AppliedLog, ConsensusStateRef};
use storage::content_manager::errors::StorageError;

/// Range of entry indices a peer's log covers.
#[derive(Debug, Serialize)]
pub struct LogWindow {
    pub first_index: u64,
    pub last_index: u64,
}

/// How long a peer trailed the first peer to apply each entry, over the entries it shares.
#[derive(Debug, Serialize)]
pub struct LagStats {
    pub avg_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
    /// Shared entry indices these numbers are drawn from.
    pub samples: usize,
}

/// How one peer compares against the rest of the cluster.
#[derive(Debug, Serialize)]
pub struct PeerLag {
    pub peer_id: PeerId,
    /// Highest entry index this peer reported applying, absent when its log is empty.
    pub last_applied_index: Option<u64>,
    /// Term of that entry.
    pub last_applied_term: Option<u64>,
    /// Entries the most advanced peer has applied and this one has not.
    pub behind_entries: Option<u64>,
    /// Entries committed but not yet applied here.
    pub pending_operations: u64,
    /// Age of this peer's newest applied entry, measured against that peer's own clock, so it is
    /// free of skew. Grows without bound while a peer is stuck part-way through applying an entry.
    pub newest_applied_age_ms: Option<u64>,
    /// How long this peer trailed the first peer to apply each shared entry.
    /// Absent when no other peer still remembers any entry this one applied.
    pub lag_ms: Option<LagStats>,
    /// No entry this peer applied is still remembered by any other peer, so only the entry gap is
    /// known, not the delay. Raise `APPLIED_LOG_SIZE` to measure lags this large.
    pub beyond_window: bool,
    /// Longest single entry apply within this peer's log.
    pub slowest_apply_ms: Option<u64>,
}

/// A peer that did not answer.
#[derive(Debug, Serialize)]
pub struct UnreachablePeer {
    pub peer_id: PeerId,
    pub error: String,
}

/// Consensus apply lag across the cluster.
#[derive(Debug, Serialize)]
pub struct ConsensusLagReport {
    /// Peer that has applied the furthest, which `behind_entries` counts back from.
    pub most_advanced_peer: PeerId,
    /// Index range that peer's log covers, absent when it has applied nothing yet.
    pub applied_window: Option<LogWindow>,
    /// Peer with the lowest average lag. Absent when no entry is held by more than one peer.
    pub fastest_peer: Option<PeerId>,
    /// One entry per peer that answered, including this one, ordered by peer id.
    pub peers: Vec<PeerLag>,
    /// Peers that failed to answer within the timeout.
    pub unreachable_peers: Vec<UnreachablePeer>,
}

/// Collect the applied-entry log from every peer and compare them.
///
/// Peers that fail or time out are reported in `unreachable_peers` rather than failing the whole
/// request; a partial answer is more useful than none when the point is to find a stuck peer.
pub async fn collect_consensus_lag(
    consensus_state: &ConsensusStateRef,
    channel_service: &ChannelService,
    this_peer_id: PeerId,
    timeout: Duration,
) -> Result<ConsensusLagReport, StorageError> {
    let mut logs = HashMap::from([(this_peer_id, consensus_state.applied_log())]);
    let mut unreachable_peers = Vec::new();

    let mut requests = channel_service
        .other_peers(this_peer_id)
        .into_iter()
        .map(|peer_id| async move {
            let result = tokio::time::timeout(
                timeout,
                channel_service.with_qdrant_client(peer_id, |mut client| async move {
                    client
                        .get_consensus_applied_log(grpc::GetConsensusAppliedLogRequest {})
                        .await
                }),
            )
            .await;
            (peer_id, result)
        })
        .collect::<FuturesUnordered<_>>();

    while let Some((peer_id, result)) = requests.next().await {
        match result {
            Ok(Ok(response)) => {
                logs.insert(peer_id, applied_log_from_grpc(response.into_inner()));
            }
            Ok(Err(err)) => unreachable_peers.push(UnreachablePeer {
                peer_id,
                error: err.to_string(),
            }),
            Err(_elapsed) => unreachable_peers.push(UnreachablePeer {
                peer_id,
                error: format!("timed out after {timeout:?}"),
            }),
        }
    }

    unreachable_peers.sort_by_key(|peer| peer.peer_id);

    Ok(build_report(this_peer_id, &logs, unreachable_peers))
}

fn applied_log_from_grpc(response: grpc::GetConsensusAppliedLogResponse) -> AppliedLog {
    let grpc::GetConsensusAppliedLogResponse {
        entries,
        now_ms,
        pending_operations,
    } = response;

    let entries = entries
        .into_iter()
        .map(|entry| {
            let grpc::AppliedConsensusEntry {
                index,
                term,
                applied_at_ms,
                took_ms,
            } = entry;

            AppliedEntry {
                index,
                term,
                applied_at_ms,
                took_ms,
            }
        })
        .collect();

    AppliedLog {
        entries,
        now_ms,
        pending_operations: pending_operations as usize,
    }
}

/// Measure every peer against the first peer to apply each entry.
fn build_report(
    this_peer_id: PeerId,
    logs: &HashMap<PeerId, AppliedLog>,
    unreachable_peers: Vec<UnreachablePeer>,
) -> ConsensusLagReport {
    // Ties go to the lowest peer id, so repeated calls against an idle cluster agree with
    // each other instead of alternating between equally-advanced peers.
    let most_advanced_peer = logs
        .iter()
        .filter_map(|(&peer_id, log)| Some((log.entries.last()?.index, peer_id)))
        .max_by_key(|&(last_index, peer_id)| (last_index, std::cmp::Reverse(peer_id)))
        .map_or(this_peer_id, |(_last_index, peer_id)| peer_id);

    let applied_window = logs.get(&most_advanced_peer).and_then(|log| {
        Some(LogWindow {
            first_index: log.entries.first()?.index,
            last_index: log.entries.last()?.index,
        })
    });

    let baseline = Baseline::of(logs);

    let mut peers: Vec<_> = logs
        .iter()
        .map(|(&peer_id, log)| peer_lag(peer_id, log, &baseline, applied_window.as_ref()))
        .collect();
    peers.sort_by_key(|peer| peer.peer_id);

    // Ties go to the lowest peer id for the same reason as above.
    let fastest_peer = peers
        .iter()
        .filter_map(|peer| Some((peer.lag_ms.as_ref()?.avg_ms, peer.peer_id)))
        .min()
        .map(|(_avg_ms, peer_id)| peer_id);

    ConsensusLagReport {
        most_advanced_peer,
        applied_window,
        fastest_peer,
        peers,
        unreachable_peers,
    }
}

/// Earliest apply time per entry index, over the entries at least two peers still remember.
///
/// Entries only one peer has are left out: measuring a peer against itself would score a lag of
/// zero and drag every average toward it.
struct Baseline(HashMap<u64, u64>);

impl Baseline {
    fn of(logs: &HashMap<PeerId, AppliedLog>) -> Self {
        let mut earliest_by_index: HashMap<u64, (u64, usize)> = HashMap::new();

        for log in logs.values() {
            for entry in &log.entries {
                earliest_by_index
                    .entry(entry.index)
                    .and_modify(|(earliest_ms, holders)| {
                        *earliest_ms = (*earliest_ms).min(entry.applied_at_ms);
                        *holders += 1;
                    })
                    .or_insert((entry.applied_at_ms, 1));
            }
        }

        earliest_by_index.retain(|_index, (_earliest_ms, holders)| *holders > 1);

        Self(
            earliest_by_index
                .into_iter()
                .map(|(index, (earliest_ms, _holders))| (index, earliest_ms))
                .collect(),
        )
    }

    /// How long after the first peer this one applied the entry, if it is shared at all.
    fn lag_of(&self, entry: &AppliedEntry) -> Option<u64> {
        Some(
            entry
                .applied_at_ms
                .saturating_sub(*self.0.get(&entry.index)?),
        )
    }
}

fn peer_lag(
    peer_id: PeerId,
    log: &AppliedLog,
    baseline: &Baseline,
    applied_window: Option<&LogWindow>,
) -> PeerLag {
    let newest = log.entries.last();

    let lags: Vec<u64> = log
        .entries
        .iter()
        .filter_map(|entry| baseline.lag_of(entry))
        .collect();

    let lag_ms = (!lags.is_empty()).then(|| LagStats {
        avg_ms: lags.iter().sum::<u64>() / lags.len() as u64,
        min_ms: lags.iter().copied().min().unwrap_or_default(),
        max_ms: lags.iter().copied().max().unwrap_or_default(),
        samples: lags.len(),
    });

    PeerLag {
        peer_id,
        last_applied_index: newest.map(|entry| entry.index),
        last_applied_term: newest.map(|entry| entry.term),
        behind_entries: newest
            .zip(applied_window)
            .map(|(entry, window)| window.last_index.saturating_sub(entry.index)),
        pending_operations: log.pending_operations as u64,
        newest_applied_age_ms: newest.map(|entry| log.now_ms.saturating_sub(entry.applied_at_ms)),
        lag_ms,
        beyond_window: newest.is_some() && lags.is_empty(),
        slowest_apply_ms: log.entries.iter().map(|entry| entry.took_ms).max(),
    }
}
