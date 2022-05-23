use collection::shard::{Shard, ShardId};
use collection::PeerId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct PeerShardCount {
    shard_count: usize, // self.shard_count and other.shard_count are compared first to determine eq & ord
    peer_id: PeerId,
}

impl PeerShardCount {
    fn new(shard_count: usize, peer_id: PeerId) -> Self {
        Self {
            shard_count,
            peer_id,
        }
    }

    fn inc_shard_count(&mut self) {
        self.shard_count += 1;
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
pub struct ShardDistributionProposal {
    pub distribution: Vec<(ShardId, PeerId)>,
}

impl ShardDistributionProposal {
    /// Builds a proposal for the distribution of shards.
    /// It will propose to allocate shards so that all peers have the same number of shards at the end.
    pub fn new(
        config_shard_number: u32,
        this_peer_id: PeerId,
        known_peers: &[PeerId],
        known_shards: Vec<&Shard>,
    ) -> Self {
        // min number of shard_count on top to make this a min-heap
        let mut min_heap: BinaryHeap<Reverse<PeerShardCount>> =
            BinaryHeap::with_capacity(known_peers.len());

        // count number of existing shards per peers
        for &peer in known_peers {
            let shard_count_on_peer = known_shards
                .iter()
                .filter(|shard| match shard {
                    Shard::Remote(remote_shard) if remote_shard.peer_id == peer => true,
                    Shard::Local(_) if this_peer_id == peer => true,
                    _ => false,
                })
                .count();
            min_heap.push(Reverse(PeerShardCount::new(shard_count_on_peer, peer)))
        }

        // no known peers with shards - add minimal entry for this peer so it gets all the shards
        if min_heap.is_empty() {
            min_heap.push(Reverse(PeerShardCount::new(1, this_peer_id)))
        }

        let mut distribution: Vec<(ShardId, PeerId)> =
            Vec::with_capacity(config_shard_number as usize);

        // propose the peer with the least amount of existing shards to host the next shard
        for shard_id in 0..config_shard_number {
            let mut least_loaded_peer = min_heap.peek_mut().unwrap();
            let selected_peer = least_loaded_peer.0.peer_id;
            least_loaded_peer.0.inc_shard_count();
            distribution.push((shard_id, selected_peer));
        }

        Self { distribution }
    }

    pub fn local_shards_for(&self, peer_id: PeerId) -> Vec<ShardId> {
        self.distribution
            .iter()
            .filter_map(
                |(shard, peer)| {
                    if peer == &peer_id {
                        Some(*shard)
                    } else {
                        None
                    }
                },
            )
            .collect()
    }

    pub fn remote_shards_for(&self, peer_id: PeerId) -> Vec<(ShardId, PeerId)> {
        self.distribution
            .iter()
            .filter(|(_shard, peer)| peer != &peer_id)
            .copied()
            .collect()
    }
}
