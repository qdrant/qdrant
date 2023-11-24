use std::cmp::{self, Reverse};
use std::collections::BinaryHeap;
use std::iter::repeat_with;
use std::num::NonZeroU32;

use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::shard::{PeerId, ShardId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct PeerShardCount {
    shard_count: usize, // self.shard_count and other.shard_count are compared first to determine eq & ord
    /// Randomized bias value, to prevent having a consistent order of peers across multiple
    /// generated distributions. This rougly balances nodes across all nodes, if the number of
    /// shards is less than the number of nodes.
    bias: usize,
    peer_id: PeerId,
}

impl PeerShardCount {
    fn new(peer_id: PeerId) -> Self {
        Self {
            shard_count: 0,
            bias: rand::random(),
            peer_id,
        }
    }

    fn get_and_inc_shard_count(&mut self) -> PeerId {
        self.shard_count += 1;
        self.peer_id
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
pub struct ShardDistributionProposal {
    /// A shard can be located on several peers if it has replicas
    pub distribution: Vec<(ShardId, Vec<PeerId>)>,
}

impl ShardDistributionProposal {
    /// Suggest an empty shard distribution placement
    /// This is useful when a collection is configured for custom sharding and
    /// we don't want to create any shards in advance.
    pub fn empty() -> Self {
        Self {
            distribution: Vec::new(),
        }
    }

    /// Builds a proposal for the distribution of shards.
    /// It will propose to allocate shards so that all peers have the same number of shards of this collection  at the end.
    pub fn new(
        shard_number: NonZeroU32,
        replication_factor: NonZeroU32,
        known_peers: &[PeerId],
    ) -> Self {
        // min number of shard_count on top to make this a min-heap
        let mut min_heap: BinaryHeap<Reverse<PeerShardCount>> = known_peers
            .iter()
            .map(|peer| Reverse(PeerShardCount::new(*peer)))
            .collect();

        // There should not be more than 1 replica per peer
        let n_replicas = cmp::min(replication_factor.get() as usize, known_peers.len());

        // Get fair distribution of shards on peers
        let distribution = (0..shard_number.get())
            .map(|shard_id| {
                let replicas =
                    repeat_with(|| min_heap.peek_mut().unwrap().0.get_and_inc_shard_count())
                        .take(n_replicas)
                        .collect();
                (shard_id, replicas)
            })
            .collect();

        Self { distribution }
    }

    pub fn local_shards_for(&self, peer_id: PeerId) -> Vec<ShardId> {
        self.distribution
            .iter()
            .filter_map(|(shard, peers)| {
                if peers.contains(&peer_id) {
                    Some(shard)
                } else {
                    None
                }
            })
            .copied()
            .collect()
    }

    pub fn remote_shards_for(&self, peer_id: PeerId) -> Vec<(ShardId, Vec<PeerId>)> {
        self.distribution
            .iter()
            .filter(|(_shard, peers)| !peers.contains(&peer_id))
            .cloned()
            .collect()
    }
}

impl From<ShardDistributionProposal> for CollectionShardDistribution {
    fn from(proposal: ShardDistributionProposal) -> Self {
        CollectionShardDistribution {
            shards: proposal
                .distribution
                .into_iter()
                .map(|(shard_id, peers)| (shard_id, peers.into_iter().collect()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution() {
        let known_peers = vec![1, 2, 3, 4];
        let distribution = ShardDistributionProposal::new(
            NonZeroU32::new(6).unwrap(),
            NonZeroU32::new(1).unwrap(),
            &known_peers,
        );

        // Check it distribution is as even as possible
        let mut shard_counts: Vec<usize> = vec![0; known_peers.len()];
        for (_shard_id, peers) in &distribution.distribution {
            for peer_id in peers {
                let peer_offset = known_peers
                    .iter()
                    .enumerate()
                    .find(|(_, x)| *x == peer_id)
                    .unwrap()
                    .0;
                shard_counts[peer_offset] += 1;
            }
        }

        assert_eq!(shard_counts.iter().sum::<usize>(), 6);
        assert_eq!(shard_counts.iter().min(), Some(&1));
        assert_eq!(shard_counts.iter().max(), Some(&2));
    }
}
