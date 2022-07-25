use std::cmp::Reverse;
use std::collections::BinaryHeap;

use collection::shard::{PeerId, ShardId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
        known_peers: &[PeerId],
        current_distribution: Vec<(ShardId, PeerId)>,
    ) -> Self {
        // min number of shard_count on top to make this a min-heap
        let mut min_heap: BinaryHeap<Reverse<PeerShardCount>> =
            BinaryHeap::with_capacity(known_peers.len());

        // count number of existing shards per peers
        for &peer in known_peers {
            let shard_count_on_peer = current_distribution
                .iter()
                .filter(|(_shard_id, peer_id)| *peer_id == peer)
                .count();
            min_heap.push(Reverse(PeerShardCount::new(shard_count_on_peer, peer)))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution_without_known_shards() {
        let known_peers = vec![1, 2, 3, 4];
        let distribution = ShardDistributionProposal::new(6, &known_peers, vec![]);

        // Check it distribution is as even as possible
        let mut new_shard_counts: Vec<usize> = vec![0; known_peers.len()];
        for (_shard_id, peer_id) in &distribution.distribution {
            let peer_offset = known_peers
                .iter()
                .enumerate()
                .find(|(_, x)| *x == peer_id)
                .unwrap()
                .0;
            new_shard_counts[peer_offset] += 1;
        }

        assert_eq!(new_shard_counts.iter().sum::<usize>(), 6);
        assert_eq!(new_shard_counts.iter().min(), Some(&1));
        assert_eq!(new_shard_counts.iter().max(), Some(&2));

        // Check it distribution is as even as possible
        assert_eq!(new_shard_counts[0], 2);
        assert_eq!(new_shard_counts[1], 2);
        assert_eq!(new_shard_counts[2], 1);
        assert_eq!(new_shard_counts[3], 1);
    }

    #[test]
    fn test_distribution_with_known_shards() {
        let known_peers = vec![1, 2, 3, 4];

        // list of pairs (ShardId, peerId)
        // the first peer has already 2 shards
        let known_shards = vec![(1, 1), (2, 1)];

        let distribution = ShardDistributionProposal::new(6, &known_peers, known_shards);

        let mut new_shard_counts: Vec<usize> = vec![0; known_peers.len()];
        for (_shard_id, peer_id) in &distribution.distribution {
            let peer_offset = known_peers
                .iter()
                .enumerate()
                .find(|(_, x)| *x == peer_id)
                .unwrap()
                .0;
            new_shard_counts[peer_offset] += 1;
        }

        assert_eq!(new_shard_counts.iter().sum::<usize>(), 6);
        assert_eq!(new_shard_counts.iter().min(), Some(&0));
        assert_eq!(new_shard_counts.iter().max(), Some(&2));

        // Check it distribution is as even as possible
        assert_eq!(new_shard_counts[0], 0); // first peer does not get any new shards allocated
        assert_eq!(new_shard_counts[1], 2);
        assert_eq!(new_shard_counts[2], 2);
        assert_eq!(new_shard_counts[3], 2);
    }
}
