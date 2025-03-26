use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;

use itertools::Itertools as _;
use segment::index::field_index::CardinalityEstimation;
use segment::types::{CustomIdCheckerCondition, PointIdType};
use smallvec::SmallVec;

use crate::operations::cluster_ops::ReshardingDirection;
use crate::shards::shard::ShardId;

pub const HASH_RING_SHARD_SCALE: u32 = 100;

#[derive(Clone, Debug, PartialEq)]
pub enum HashRingRouter<T: Eq + Hash = ShardId> {
    /// Single hashring
    Single(HashRing<T>),

    /// Two hashrings when transitioning during resharding
    /// Depending on the current resharding state, points may be in either or both shards.
    Resharding { old: HashRing<T>, new: HashRing<T> },
}

impl<T: Copy + Eq + Hash> HashRingRouter<T> {
    /// Create a new single hashring.
    ///
    /// The hashring is created with a fair distribution of points and `HASH_RING_SHARD_SCALE` scale.
    pub fn single() -> Self {
        Self::Single(HashRing::fair(HASH_RING_SHARD_SCALE))
    }

    pub fn add(&mut self, shard: T) -> bool {
        match self {
            Self::Single(ring) => ring.add(shard),
            Self::Resharding { old, new } => {
                // When resharding is in progress:
                // - either `new` hashring contains a shard, that is not in `old` (when resharding *up*)
                // - or `old` contains a shard, that is not in `new` (when resharding *down*)
                //
                // This check ensures, that we don't accidentally break this invariant when adding
                // nodes to `Resharding` hashring.

                if !old.contains(&shard) && !new.contains(&shard) {
                    old.add(shard);
                    new.add(shard);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn start_resharding(&mut self, shard: T, direction: ReshardingDirection) {
        if let Self::Single(ring) = self {
            let (old, new) = (ring.clone(), ring.clone());
            *self = Self::Resharding { old, new };
        }

        let Self::Resharding { old, new } = self else {
            unreachable!();
        };

        match direction {
            ReshardingDirection::Up => {
                old.remove(&shard);
                new.add(shard);
            }

            ReshardingDirection::Down => {
                assert!(new.len() > 1, "cannot remove last shard from hash ring");

                old.add(shard);
                new.remove(&shard);
            }
        }
    }

    pub fn commit_resharding(&mut self) -> bool {
        let Self::Resharding { new, .. } = self else {
            log::warn!("committing resharding hashring, but hashring is not in resharding mode");
            return false;
        };

        *self = Self::Single(new.clone());
        true
    }

    pub fn abort_resharding(&mut self, shard: T, direction: ReshardingDirection) -> bool
    where
        T: fmt::Display,
    {
        let context = match direction {
            ReshardingDirection::Up => "reverting scale-up hashring into single mode",
            ReshardingDirection::Down => "reverting scale-down hashring into single mode",
        };

        let Self::Resharding { old, new } = self else {
            log::warn!("{context}, but hashring is not in resharding mode");
            return false;
        };

        let mut old = old.clone();
        let mut new = new.clone();

        let (expected_in_old, expected_in_new) = match direction {
            ReshardingDirection::Up => (old.remove(&shard), new.remove(&shard)),
            ReshardingDirection::Down => (old.add(shard), new.add(shard)),
        };

        match (expected_in_old, expected_in_new) {
            (false, true) => (),

            (true, false) => {
                log::error!("{context}, but expected state of hashrings is reversed");
            }

            (true, true) => {
                log::error!("{context}, but {shard} is not a target shard");
            }

            (false, false) => {
                log::warn!("{context}, but shard {shard} does not exist in the hashring");
            }
        };

        if old == new {
            log::debug!("{context}, because the rerouting for resharding is done");
            *self = Self::Single(old.clone());
            true
        } else {
            log::warn!("{context}, but rerouting for resharding is not done yet");
            false
        }
    }

    pub fn get<U: Hash>(&self, key: &U) -> ShardIds<T> {
        match self {
            Self::Single(ring) => ring.get(key).into_iter().copied().collect(),
            Self::Resharding { old, new } => old
                .get(key)
                .into_iter()
                .chain(new.get(key))
                .copied()
                .dedup() // Both hash rings may return the same shard ID, take it once
                .collect(),
        }
    }

    /// Check whether the given point is in the given shard
    ///
    /// In case of resharding, the new hashring is checked.
    pub fn is_in_shard<U: Hash>(&self, key: &U, shard: T) -> bool {
        let ring = match self {
            Self::Resharding { new, .. } => new,
            Self::Single(ring) => ring,
        };

        ring.get(key) == Some(&shard)
    }
}

impl<T: Eq + Hash> HashRingRouter<T> {
    pub fn is_resharding(&self) -> bool {
        matches!(self, Self::Resharding { .. })
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Single(ring) => ring.is_empty(),
            Self::Resharding { old, new } => old.is_empty() && new.is_empty(),
        }
    }

    /// Get unique nodes from the hashring
    pub fn nodes(&self) -> &HashSet<T> {
        match self {
            HashRingRouter::Single(ring) => ring.nodes(),
            HashRingRouter::Resharding { new, .. } => new.nodes(),
        }
    }
}

/// List type for shard IDs
///
/// Uses a `SmallVec` putting two IDs on the stack. That's the maximum number of shards we expect
/// with the current resharding implementation.
pub type ShardIds<T = ShardId> = SmallVec<[T; 2]>;

#[derive(Clone, Debug, PartialEq)]
pub enum HashRing<T: Eq + Hash> {
    Raw {
        nodes: HashSet<T>,
        ring: hashring::HashRing<T>,
    },

    Fair {
        nodes: HashSet<T>,
        ring: hashring::HashRing<(T, u32)>,
        scale: u32,
    },
}

impl<T: Copy + Eq + Hash> HashRing<T> {
    pub fn raw() -> Self {
        Self::Raw {
            nodes: HashSet::new(),
            ring: hashring::HashRing::new(),
        }
    }

    /// Constructs a HashRing that tries to give all shards equal space on the ring.
    /// The higher the `scale` - the more equal the distribution of points on the shards will be,
    /// but shard search might be slower.
    pub fn fair(scale: u32) -> Self {
        Self::Fair {
            nodes: HashSet::new(),
            ring: hashring::HashRing::new(),
            scale,
        }
    }

    pub fn add(&mut self, shard: T) -> bool {
        if !self.nodes_mut().insert(shard) {
            return false;
        }

        match self {
            HashRing::Raw { ring, .. } => {
                ring.add(shard);
            }

            HashRing::Fair { ring, scale, .. } => {
                for idx in 0..*scale {
                    ring.add((shard, idx));
                }
            }
        }

        true
    }

    pub fn remove(&mut self, shard: &T) -> bool {
        if !self.nodes_mut().remove(shard) {
            return false;
        }

        match self {
            HashRing::Raw { ring, .. } => {
                ring.remove(shard);
            }

            HashRing::Fair { ring, scale, .. } => {
                for idx in 0..*scale {
                    ring.remove(&(*shard, idx));
                }
            }
        }

        true
    }
}

impl<T: Eq + Hash> HashRing<T> {
    pub fn get<U: Hash>(&self, key: &U) -> Option<&T> {
        match self {
            HashRing::Raw { ring, .. } => ring.get(key),
            HashRing::Fair { ring, .. } => ring.get(key).map(|(shard, _)| shard),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nodes().is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes().len()
    }

    pub fn contains(&self, shard: &T) -> bool {
        self.nodes().contains(shard)
    }

    pub fn nodes(&self) -> &HashSet<T> {
        match self {
            HashRing::Raw { nodes, .. } => nodes,
            HashRing::Fair { nodes, .. } => nodes,
        }
    }

    fn nodes_mut(&mut self) -> &mut HashSet<T> {
        match self {
            HashRing::Raw { nodes, .. } => nodes,
            HashRing::Fair { nodes, .. } => nodes,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HashRingFilter {
    ring: HashRing<ShardId>,
    expected_shard_id: ShardId,
}

impl HashRingFilter {
    pub fn new(ring: HashRing<ShardId>, expected_shard_id: ShardId) -> Self {
        Self {
            ring,
            expected_shard_id,
        }
    }
}

impl CustomIdCheckerCondition for HashRingFilter {
    fn estimate_cardinality(&self, points: usize) -> CardinalityEstimation {
        CardinalityEstimation {
            primary_clauses: vec![],
            min: 0,
            exp: points / self.ring.len(),
            max: points,
        }
    }

    fn check(&self, point_id: PointIdType) -> bool {
        self.ring.get(&point_id) == Some(&self.expected_shard_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_seq_keys() {
        let mut ring = HashRing::fair(100);
        ring.add(5);
        ring.add(7);
        ring.add(8);
        ring.add(20);

        for i in 0..20 {
            match ring.get(&i) {
                None => panic!("Key {i} has no shard"),
                Some(x) => assert!([5, 7, 8, 20].contains(x)),
            }
        }
    }

    #[test]
    fn test_repartition() {
        let mut ring = HashRing::fair(100);

        ring.add(1);
        ring.add(2);
        ring.add(3);

        let mut pre_split = Vec::new();
        let mut post_split = Vec::new();

        for i in 0..100 {
            match ring.get(&i) {
                None => panic!("Key {i} has no shard"),
                Some(x) => pre_split.push(*x),
            }
        }

        ring.add(4);

        for i in 0..100 {
            match ring.get(&i) {
                None => panic!("Key {i} has no shard"),
                Some(x) => post_split.push(*x),
            }
        }

        assert_ne!(pre_split, post_split);

        for (x, y) in pre_split.iter().zip(post_split.iter()) {
            if x != y {
                assert_eq!(*y, 4);
            }
        }
    }
}
