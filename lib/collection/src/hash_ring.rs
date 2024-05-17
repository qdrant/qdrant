use std::hash::Hash;

use smallvec::SmallVec;

use crate::shards::shard::ShardId;

const HASH_RING_SHARD_SCALE: u32 = 100;

#[derive(Clone)]
pub enum ShardHashRing {
    /// Single hashring
    Single(HashRing<ShardId>),

    /// Two hashrings when transitioning during resharding
    /// Depending on the current resharding state, points may be in either or both shards.
    Resharding {
        old: HashRing<ShardId>,
        new: HashRing<ShardId>,
    },
}

impl ShardHashRing {
    /// Create a new single hashring.
    ///
    /// The hashring is created with a fair distribution of points and `HASH_RING_SHARD_SCALE` scale.
    pub fn single() -> Self {
        Self::Single(HashRing::fair(HASH_RING_SHARD_SCALE))
    }

    /// Create a new resharding hashring, with resharding shard already added into `new` hashring.
    ///
    /// The hashring is created with a fair distribution of points and `HASH_RING_SHARD_SCALE` scale.
    pub fn resharding(shard_id: ShardId) -> Self {
        let mut ring = Self::Resharding {
            old: HashRing::fair(HASH_RING_SHARD_SCALE),
            new: HashRing::fair(HASH_RING_SHARD_SCALE),
        };

        ring.add_resharding(shard_id);

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

    pub fn add(&mut self, shard: ShardId) {
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

    pub fn add_resharding(&mut self, shard: ShardId) {
        if let Self::Single(ring) = self {
            let (old, new) = (ring.clone(), ring.clone());
            *self = Self::Resharding { old, new };
        }

        let Self::Resharding { new, .. } = self else {
            unreachable!();
        };

        new.add(shard);
    }

    pub fn get<U: Hash>(&self, key: &U) -> ShardIds {
        match self {
            Self::Single(ring) => ring.get(key).into_iter().cloned().collect(),
            // TODO(resharding): just use the old hash ring for now, never route to two shards
            // TODO(resharding): switch to both as commented below once read folding is implemented
            Self::Resharding { old, new: _ } => old.get(key).into_iter().cloned().collect(),
            // Self::Resharding { old, new } => old
            //     .get(key)
            //     .into_iter()
            //     .chain(new.get(key))
            //     // Both hash rings may return the same shard ID, take it once
            //     .dedup()
            //     .cloned()
            //     .collect(),
        }
    }
}

/// List type for shard IDs
///
/// Uses a `SmallVec` putting two IDs on the stack. That's the maximum number of shards we expect
/// with the current resharding implementation.
pub type ShardIds = SmallVec<[ShardId; 2]>;

#[derive(Clone)]
pub enum HashRing<T: Hash + Copy> {
    Raw(hashring::HashRing<T>),
    Fair {
        ring: hashring::HashRing<(T, u32)>,
        scale: u32,
    },
}

impl<T: Hash + Copy> HashRing<T> {
    pub fn raw() -> Self {
        Self::Raw(hashring::HashRing::new())
    }

    /// Constructs a HashRing that tries to give all shards equal space on the ring.
    /// The higher the `scale` - the more equal the distribution of points on the shards will be,
    /// but shard search might be slower.
    pub fn fair(scale: u32) -> Self {
        Self::Fair {
            ring: hashring::HashRing::new(),
            scale,
        }
    }

    pub fn add(&mut self, shard: T) {
        match self {
            HashRing::Raw(ring) => ring.add(shard),
            HashRing::Fair { ring, scale } => {
                for i in 0..*scale {
                    ring.add((shard, i))
                }
            }
        }
    }

    pub fn remove(&mut self, shard: &T) -> bool {
        match self {
            HashRing::Raw(ring) => ring.remove(shard).is_some(),
            HashRing::Fair { ring, scale } => {
                let mut removed = false;
                for i in 0..*scale {
                    if ring.remove(&(*shard, i)).is_some() {
                        removed = true;
                    }
                }
                removed
            }
        }
    }

    pub fn get<U: Hash>(&self, key: &U) -> Option<&T> {
        match self {
            HashRing::Raw(ring) => ring.get(key),
            HashRing::Fair { ring, .. } => ring.get(key).map(|(shard, _)| shard),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            HashRing::Raw(ring) => ring.len(),
            HashRing::Fair { ring, .. } => ring.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            HashRing::Raw(ring) => ring.is_empty(),
            HashRing::Fair { ring, .. } => ring.is_empty(),
        }
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
}
