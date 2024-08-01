use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;

use itertools::Itertools as _;
use smallvec::SmallVec;

use crate::operations::cluster_ops::ReshardingDirection;
use crate::shards::shard::ShardId;

#[derive(Clone, PartialEq, Debug)]
pub enum HashRing<T = ShardId> {
    /// Single hashring
    Single(Inner<T>),

    /// Two hashrings when transitioning during resharding
    /// Depending on the current resharding state, points may be in either or both shards.
    Resharding { old: Inner<T>, new: Inner<T> },
}

impl<T: Hash + Copy + PartialEq> HashRing<T> {
    /// Create a new single hashring.
    ///
    /// The hashring is created with a fair distribution of points and `FAIR_HASH_RING_SCALE` scale.
    pub fn single() -> Self {
        Self::Single(Inner::fair(scaled_hashring::DEFAULT_FAIR_HASH_RING_SCALE))
    }

    /// Create a new resharding hashring, with resharding shard already added into `new` hashring.
    ///
    /// The hashring is created with a fair distribution of points and `FAIR_HASH_RING_SCALE` scale.
    pub fn resharding(shard: T, direction: ReshardingDirection) -> Self {
        let mut ring = Self::Resharding {
            old: Inner::fair(scaled_hashring::DEFAULT_FAIR_HASH_RING_SCALE),
            new: Inner::fair(scaled_hashring::DEFAULT_FAIR_HASH_RING_SCALE),
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
                if new.get(&shard).is_none() {
                    old.add(shard);
                    new.add(shard);
                }
            }
        }
    }

    pub fn start_resharding(&mut self, shard: T, direction: ReshardingDirection) {
        if let Self::Single(ring) = self {
            let (old, new) = (ring.clone(), ring.clone());
            *self = Self::Resharding { old, new };
        }

        let Self::Resharding { new, .. } = self else {
            unreachable!();
        };

        match direction {
            ReshardingDirection::Up => {
                new.add(shard);
            }
            ReshardingDirection::Down => {
                assert!(new.len() > 1, "cannot remove last shard from hash ring");
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
}

impl<T: Hash + Copy + PartialEq + Eq> HashRing<T> {
    /// Get unique nodes from the hashring
    pub fn unique_nodes(&self) -> HashSet<T> {
        match self {
            Self::Single(ring) => ring.unique_nodes(),
            Self::Resharding { new, .. } => new.unique_nodes(),
        }
    }
}

pub type Inner<T> = scaled_hashring::ScaledHashRing<T>;

/// List type for shard IDs
///
/// Uses a `SmallVec` putting two IDs on the stack. That's the maximum number of shards we expect
/// with the current resharding implementation.
pub type ShardIds<T = ShardId> = SmallVec<[T; 2]>;
