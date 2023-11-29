use std::cmp::{self, Reverse};
use std::collections::BinaryHeap;
use std::iter::repeat_with;
use std::num::NonZeroU32;

use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::shard::{PeerId, ShardId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq)]
struct PeerShardCount {
    shard_count: usize,
    /// Randomized bias value, to prevent having a consistent order of peers across multiple
    /// generated distributions. This roughly balances nodes across all nodes, if the number of
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

impl PartialOrd for PeerShardCount {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Explicitly implement ordering to make sure we don't accidentally break this.
///
/// Ordering:
/// - shard_count: lowest number of shards first
/// - bias: randomize order of peers with same number of shards
/// - peer_id
impl Ord for PeerShardCount {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.shard_count
            .cmp(&other.shard_count)
            .then(self.bias.cmp(&other.bias))
            // It is very unlikely that we need this, so `then_with` is a bit faster
            .then_with(|| self.peer_id.cmp(&other.peer_id))
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
        // Min-heap: peer with lowest number of shards is on top
        let mut min_heap: BinaryHeap<_> = known_peers
            .iter()
            .map(|peer| Reverse(PeerShardCount::new(*peer)))
            .collect();

        // There should not be more than 1 replica per peer
        let replica_number = cmp::min(replication_factor.get() as usize, known_peers.len());

        // Get fair distribution of shards on peers
        let distribution = (0..shard_number.get())
            .map(|shard_id| {
                let replicas =
                    repeat_with(|| min_heap.peek_mut().unwrap().0.get_and_inc_shard_count())
                        .take(replica_number)
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
    use std::collections::HashSet;

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

    #[test]
    fn test_distribution_is_spread() {
        let known_peers = vec![1, 2, 3, 4];
        let shard_numbers = 1..=3;
        let replication_factors = 1..=4;
        let tries = 100;

        // With 4 peers, for various shard number and replication factor ranges, always generate
        // distributions that inhabit all peers across 100 retries.
        for shard_number in shard_numbers {
            for replication_factor in replication_factors.clone() {
                let inhabited_peers = (0..tries)
                    // Generate distribution
                    .map(|_| {
                        ShardDistributionProposal::new(
                            NonZeroU32::new(shard_number).unwrap(),
                            NonZeroU32::new(replication_factor).unwrap(),
                            &known_peers,
                        )
                    })
                    // Take just the inhabited peer IDs
                    .flat_map(|proposal| {
                        proposal
                            .distribution
                            .clone()
                            .into_iter()
                            .flat_map(|(_, peers)| peers)
                    })
                    .collect::<HashSet<_>>();

                assert_eq!(
                    inhabited_peers.len(),
                    known_peers.len(),
                    "must inhabit all {} peers across {tries} distributions",
                    known_peers.len(),
                );
            }
        }
    }
}
