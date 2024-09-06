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

#[derive(Clone, Debug)]
pub enum HashRingRouter<T = ShardId> {
    /// Single hashring
    Single(HashRing<T>),

    /// Two hashrings when transitioning during resharding
    /// Depending on the current resharding state, points may be in either or both shards.
    Resharding { old: HashRing<T>, new: HashRing<T> },
}

impl<T: Hash + Copy + Eq> HashRingRouter<T> {
    /// Create a new single hashring.
    ///
    /// The hashring is created with a fair distribution of points and `HASH_RING_SHARD_SCALE` scale.
    pub fn single() -> Self {
        Self::Single(HashRing::fair(HASH_RING_SHARD_SCALE))
    }

    /// Create a new resharding hashring, with resharding shard already added into `new` hashring.
    ///
    /// The hashring is created with a fair distribution of points and `HASH_RING_SHARD_SCALE` scale.
    pub fn resharding(shard: T, direction: ReshardingDirection) -> Self {
        let mut ring = Self::Resharding {
            old: HashRing::fair(HASH_RING_SHARD_SCALE),
            new: HashRing::fair(HASH_RING_SHARD_SCALE),
        };

        ring.start_resharding(shard, direction);

        ring
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Single(ring) => ring.is_empty(),
            Self::Resharding { old, new } => old.is_empty() && new.is_empty(),
        }
    }

    pub fn is_resharding(&self) -> bool {
        matches!(self, Self::Resharding { .. })
    }

    pub fn add(&mut self, shard: T) {
        match self {
            Self::Single(ring) => ring.add(shard),
            Self::Resharding { old, new } => {
                old.add(shard);
                new.add(shard);
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

    pub fn end_resharding(&mut self, shard: T, direction: ReshardingDirection) -> bool
    where
        T: fmt::Display,
    {
        let Self::Resharding { old, new } = self else {
            log::warn!("ending resharding hashring, but it is not in resharding mode");
            return false;
        };

        let mut old = old.clone();
        let mut new = new.clone();

        let (updated_old, updated_new) = match direction {
            ReshardingDirection::Up => (old.remove(&shard), new.remove(&shard)),
            ReshardingDirection::Down => {
                old.add(shard);
                new.add(shard);
                (false, true)
            }
        };

        let updated = match (updated_old, updated_new) {
            (false, true) => true,

            (true, true) => {
                log::error!(
                    "ending resharding shard, \
                     but {shard} is not resharding shard"
                );

                false
            }

            (true, false) => {
                log::error!(
                    "ending resharding shard, \
                     but shard {shard} only exists in the old hashring"
                );

                false
            }

            (false, false) => {
                log::warn!(
                    "ending resharding shard, \
                     but shard {shard} does not exist in the hashring"
                );

                false
            }
        };

        if old == new {
            log::debug!(
                "switching hashring into single mode, \
                 because the rerouting for resharding is done",
            );

            *self = Self::Single(old);
        }

        updated
    }

    pub fn get<U: Hash>(&self, key: &U) -> ShardIds<T>
    where
        T: PartialEq,
    {
        match self {
            Self::Single(ring) => ring.get(key).into_iter().cloned().collect(),
            Self::Resharding { old, new } => old
                .get(key)
                .into_iter()
                .chain(new.get(key))
                // Both hash rings may return the same shard ID, take it once
                .dedup()
                .cloned()
                .collect(),
        }
    }

    /// Check whether the given point is in the given shard
    ///
    /// In case of resharding, the new hashring is checked.
    pub fn is_in_shard<U: Hash>(&self, key: &U, shard: T) -> bool
    where
        T: PartialEq,
    {
        let ring = match self {
            Self::Resharding { new, .. } => new,
            Self::Single(ring) => ring,
        };

        ring.get(key) == Some(&shard)
    }

    fn single_ring(&self) -> Option<&HashRing<T>> {
        match self {
            Self::Single(ring) => Some(ring),
            _ => None,
        }
    }

    fn old_ring(&self) -> Option<&HashRing<T>> {
        match self {
            Self::Resharding { old, .. } => Some(old),
            _ => None,
        }
    }

    fn new_ring(&self) -> Option<&HashRing<T>> {
        match self {
            Self::Resharding { new, .. } => Some(new),
            _ => None,
        }
    }
}

impl<T: Hash + Copy + Eq> HashRingRouter<T> {
    /// Get unique nodes from the hashring
    pub fn unique_nodes(&self) -> HashSet<T> {
        let nodes = match self {
            HashRingRouter::Single(ring) => ring.nodes(),
            HashRingRouter::Resharding { old, new } => {
                if old.len() > new.len() {
                    old.nodes()
                } else {
                    new.nodes()
                }
            }
        };

        nodes.clone()
    }
}

impl<T: Copy + Eq + Hash> PartialEq for HashRingRouter<T> {
    fn eq(&self, other: &Self) -> bool {
        self.single_ring() == other.single_ring()
            && self.old_ring() == other.old_ring()
            && self.new_ring() == other.new_ring()
    }
}

impl<T: Copy + Eq + Hash> Eq for HashRingRouter<T> {}

/// List type for shard IDs
///
/// Uses a `SmallVec` putting two IDs on the stack. That's the maximum number of shards we expect
/// with the current resharding implementation.
pub type ShardIds<T = ShardId> = SmallVec<[T; 2]>;

#[derive(Clone, Debug)]
pub enum HashRing<T> {
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

impl<T: Hash + Copy + Eq> HashRing<T> {
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

    pub fn add(&mut self, shard: T) {
        if !self.nodes_mut().insert(shard) {
            return;
        }

        match self {
            HashRing::Raw { ring, .. } => ring.add(shard),
            HashRing::Fair { ring, scale, .. } => {
                for i in 0..*scale {
                    ring.add((shard, i))
                }
            }
        }
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
                for i in 0..*scale {
                    ring.remove(&(*shard, i));
                }
            }
        }

        true
    }

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

    fn scale(&self) -> Option<u32> {
        match self {
            HashRing::Raw { .. } => None,
            HashRing::Fair { scale, .. } => Some(*scale),
        }
    }
}

impl<T: Copy + Eq + Hash> PartialEq for HashRing<T> {
    fn eq(&self, other: &Self) -> bool {
        self.scale() == other.scale() && self.nodes() == other.nodes()
    }
}

impl<T: Copy + Eq + Hash> Eq for HashRing<T> {}

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
