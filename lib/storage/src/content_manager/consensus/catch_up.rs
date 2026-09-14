//! Catching up with the consensus commit of the cluster after a (re)start.

use std::collections::HashSet;
use std::future::{self, Future};
use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::qdrant_internal_client::QdrantInternalClient;
use api::grpc::qdrant::{GetConsensusCommitRequest, GetConsensusCommitResponse};
use api::grpc::transport_channel_pool::{self, TransportChannelPool};
use common::defaults;
use futures::stream::FuturesUnordered;
use futures::{StreamExt as _, TryStreamExt as _};
use itertools::Itertools;
use tokio::{runtime, time};

use crate::content_manager::consensus_manager::ConsensusStateRef;
use crate::types::PeerAddressById;

/// How often to check whether this peer reached the cluster commit while catching up.
const CATCH_UP_POLL_INTERVAL: Duration = Duration::from_millis(100);

const GET_CONSENSUS_COMMITS_RETRIES: usize = 2;

impl ConsensusStateRef {
    /// Spawn a task that marks `is_consensus_caught_up` once this peer has applied every consensus
    /// entry the cluster had committed when this peer (re)started.
    ///
    /// Committed but unapplied entries are replayed before this peer joins consensus, and the
    /// entries committed while it was down only arrive once it has joined. Anything acting on the
    /// state of consensus at startup, such as `/readyz` or a shard transfer driver, waits for this
    /// flag rather than for a leader.
    ///
    /// With `wait_for_bootstrap`, the check holds until this peer has joined the cluster for the
    /// first time, which a new deployment started with `--bootstrap` needs.
    pub fn spawn_consensus_catch_up(
        &self,
        runtime: &runtime::Handle,
        transport_channel_pool: Arc<TransportChannelPool>,
        wait_for_bootstrap: bool,
    ) {
        let state = self.clone();
        runtime.spawn(async move {
            state
                .catch_up(&transport_channel_pool, wait_for_bootstrap)
                .await;
            log::debug!("Caught up with the consensus commit of the cluster");
            state.is_consensus_caught_up.make_ready();
        });
    }

    async fn catch_up(
        &self,
        transport_channel_pool: &TransportChannelPool,
        wait_for_bootstrap: bool,
    ) {
        // Wait until node joins cluster for the first time
        if wait_for_bootstrap {
            while self.peer_count() <= 1 {
                time::sleep(CATCH_UP_POLL_INTERVAL).await;
            }
        }

        // Get estimate of current cluster commit so we can wait for it
        let Some(mut cluster_commit_index) = self
            .cluster_commit_index(transport_channel_pool, true)
            .await
        else {
            return;
        };

        // Wait until local peer has reached cluster commit
        loop {
            while self.applied_index() < cluster_commit_index {
                // Ensure we're not the only peer left
                if self.member_peer_addresses().len() <= 1 {
                    return;
                }

                time::sleep(CATCH_UP_POLL_INTERVAL).await;
            }

            match self
                .cluster_commit_index(transport_channel_pool, false)
                .await
            {
                // If cluster commit is still the same, we caught up and we're done
                Some(new_index) if cluster_commit_index == new_index => return,
                // Cluster commit is newer, update it and wait again
                Some(new_index) => cluster_commit_index = new_index,
                // Failed to get cluster commit, assume we're done
                None => return,
            }
        }
    }

    fn applied_index(&self) -> u64 {
        self.persistent.read().last_applied_entry().unwrap_or(0)
    }

    /// Addresses of the peers that are members of the current consensus configuration.
    ///
    /// `peer_address_by_id` can also hold peers that are no longer part of the cluster. Most
    /// notably, after `--reinit` of a previously removed peer, it still lists the peers of the
    /// old cluster, while `conf_state` is reset to this peer only. Waiting to reach the commit
    /// index of such peers means waiting for the commit index of a foreign consensus, which this
    /// peer may never reach.
    ///
    /// The `conf_state` filter only applies if this peer is a member itself. A bootstrapping
    /// peer seeds `conf_state` with just the first voter of the cluster until it applies the
    /// configuration change entries of the log. Filtering by that interim `conf_state` would
    /// leave a single member and misclassify this peer as a single-node cluster, reporting it
    /// ready before it caught up with the cluster commit index. The same reasoning covers an
    /// empty `conf_state`: this peer doesn't know the cluster configuration yet, so all known
    /// peer addresses are considered members.
    fn member_peer_addresses(&self) -> PeerAddressById {
        let persistent = self.persistent.read();
        let conf_state = &persistent.state().conf_state;

        let members: HashSet<_> = conf_state
            .voters
            .iter()
            .chain(&conf_state.voters_outgoing)
            .chain(&conf_state.learners)
            .chain(&conf_state.learners_next)
            .copied()
            .collect();

        let mut peer_address_by_id = persistent.peer_address_by_id();

        if members.contains(&persistent.this_peer_id()) {
            peer_address_by_id.retain(|peer_id, _| members.contains(peer_id));
        }

        peer_address_by_id
    }

    /// Get the highest consensus commit across cluster peers
    ///
    /// If `one_peer` is true the first fetched commit is returned. It may not necessarily be the
    /// latest commit.
    async fn cluster_commit_index(
        &self,
        transport_channel_pool: &TransportChannelPool,
        one_peer: bool,
    ) -> Option<u64> {
        let peer_address_by_id = self.member_peer_addresses();

        // Check if there is only 1 node in the cluster
        if peer_address_by_id.len() <= 1 {
            return None;
        }

        // Get *cluster* commit index
        let this_peer_uri = peer_address_by_id.get(&self.this_peer_id());

        let mut requests = peer_address_by_id
            .values()
            // Do not get the current commit from ourselves
            .filter(|&uri| Some(uri) != this_peer_uri)
            // Historic peers might use the same URLs as our current peers, request each URI once
            .unique()
            .map(|uri| get_consensus_commit(transport_channel_pool, uri))
            .collect::<FuturesUnordered<_>>()
            .inspect_err(|err| log::error!("GetConsensusCommit request failed: {err}"))
            .filter_map(|res| future::ready(res.ok()));

        // Raft commits consensus operation, after majority of nodes persisted it.
        //
        // This means, if we check the majority of nodes (e.g., `total nodes / 2 + 1`), at least one
        // of these nodes will *always* have an up-to-date commit index. And so, the highest commit
        // index among majority of nodes *is* the cluster commit index.
        //
        // Our current node *is* one of the cluster nodes, so it's enough to query `total nodes / 2`
        // *additional* nodes, to get cluster commit index.
        //
        // The check goes like this:
        // - Either at least one of the "additional" nodes return a *higher* commit index, which
        //   means our node is *not* up-to-date, and we have to wait to reach this commit index
        // - Or *all* of them return *lower* commit index, which means current node is *already*
        //   up-to-date, and the check will pass to the next step
        //
        // Example:
        //
        // Total nodes: 2
        // Required: 2 / 2 = 1
        //
        // Total nodes: 3
        // Required: 3 / 2 = 1
        //
        // Total nodes: 4
        // Required: 4 / 2 = 2
        //
        // Total nodes: 5
        // Required: 5 / 2 = 2
        let sufficient_commit_indices_count = if !one_peer {
            peer_address_by_id.len() / 2
        } else {
            1
        };

        // *Wait* for `total nodex / 2` successful responses...
        let mut commit_indices: Vec<_> = (&mut requests)
            .take(sufficient_commit_indices_count)
            .collect()
            .await;

        // ...and also collect any additional responses, that we might have *already* received
        while let Ok(Some(resp)) = time::timeout(Duration::ZERO, requests.next()).await {
            commit_indices.push(resp);
        }

        // Find the maximum commit index among all responses.
        //
        // Note, that we progress even if most (or even *all*) requests failed (e.g., because all
        // other nodes are unavailable or they don't support `GetConsensusCommit` gRPC API).
        //
        // So this check is not 100% reliable and can give a false-positive result!
        let cluster_commit_index = commit_indices
            .into_iter()
            .map(|resp| resp.into_inner().commit)
            .max()
            .unwrap_or(0);

        Some(cluster_commit_index as _)
    }
}

fn get_consensus_commit<'a>(
    transport_channel_pool: &'a TransportChannelPool,
    uri: &'a tonic::transport::Uri,
) -> impl Future<Output = GetConsensusCommitResult> + 'a {
    transport_channel_pool.with_channel_timeout(
        uri,
        |channel| async {
            let mut client = QdrantInternalClient::new(channel);
            let mut request = tonic::Request::new(GetConsensusCommitRequest {});
            request.set_timeout(defaults::CONSENSUS_META_OP_WAIT);
            client.get_consensus_commit(request).await
        },
        Some(defaults::CONSENSUS_META_OP_WAIT),
        GET_CONSENSUS_COMMITS_RETRIES,
    )
}

type GetConsensusCommitResult = Result<
    tonic::Response<GetConsensusCommitResponse>,
    transport_channel_pool::RequestError<tonic::Status>,
>;
