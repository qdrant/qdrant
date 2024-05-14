use std::hash::Hash;
use std::mem;

#[derive(Clone)]
pub enum HashRing<T: Hash + Copy> {
    Raw(hashring::HashRing<T>),
    Fair {
        ring: hashring::HashRing<(T, u32)>,
        scale: u32,
    },
}

impl<T: Hash + Copy> HashRing<T> {
    // `hashring::HashRing` does not implement `Clone`, so we have to do this... 🤦‍♀️
    pub fn clone(&mut self) -> Self {
        match self {
            HashRing::Raw(ring) => {
                let (this, other) = duplicate(mem::take(ring));
                *ring = this;

                Self::Raw(other)
            }

            HashRing::Fair { ring, scale } => {
                let (this, other) = duplicate(mem::take(ring));
                *ring = this;

                Self::Fair {
                    ring: other,
                    scale: *scale,
                }
            }
        }
    }

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

// `hashring::HashRing` does not implement `Clone`, so we have to do this... 🤦‍♀️
fn duplicate<T>(ring: hashring::HashRing<T>) -> (hashring::HashRing<T>, hashring::HashRing<T>)
where
    T: Clone + Hash,
{
    let nodes: Vec<_> = ring.into_iter().collect();

    let mut first = hashring::HashRing::new();
    first.batch_add(nodes.clone());

    let mut second = hashring::HashRing::new();
    second.batch_add(nodes);

    (first, second)
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
