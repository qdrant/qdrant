use std::collections::HashSet;
use std::hash;

pub const DEFAULT_FAIR_HASH_RING_SCALE: u32 = 100;

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(from = "SerdeHelper<T>", into = "SerdeHelper<T>")]
#[serde(bound(deserialize = "T: Copy + hash::Hash + serde::Deserialize<'de>"))]
#[serde(bound(serialize = "T: Copy + Eq + hash::Hash + serde::Serialize"))]
pub enum ScaledHashRing<T> {
    Raw(hashring::HashRing<T>),

    Fair {
        ring: hashring::HashRing<(T, u32)>,
        scale: u32,
    },
}

impl<T: Copy + hash::Hash> ScaledHashRing<T> {
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
            Self::Raw(ring) => ring.add(shard),
            Self::Fair { ring, scale } => {
                for i in 0..*scale {
                    ring.add((shard, i))
                }
            }
        }
    }

    pub fn remove(&mut self, shard: &T) -> bool {
        match self {
            Self::Raw(ring) => ring.remove(shard).is_some(),
            Self::Fair { ring, scale } => {
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

    pub fn get<U: hash::Hash>(&self, key: &U) -> Option<&T> {
        match self {
            Self::Raw(ring) => ring.get(key),
            Self::Fair { ring, .. } => ring.get(key).map(|(shard, _)| shard),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Raw(ring) => ring.is_empty(),
            Self::Fair { ring, .. } => ring.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Raw(ring) => ring.len(),
            Self::Fair { ring, scale } => ring.len() / *scale as usize,
        }
    }

    /// Get unique nodes from the hashring
    pub fn unique_nodes(&self) -> HashSet<T>
    where
        T: Eq,
    {
        match self {
            Self::Raw(ring) => ring.clone().into_iter().collect(),
            Self::Fair { ring, .. } => ring.clone().into_iter().map(|(node, _)| node).collect(),
        }
    }
}

impl<T: schemars::JsonSchema> schemars::JsonSchema for ScaledHashRing<T> {
    fn schema_name() -> String {
        "ScaledHashRing".into()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        SerdeHelper::<T>::json_schema(gen)
    }

    fn is_referenceable() -> bool {
        SerdeHelper::<T>::is_referenceable()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        SerdeHelper::<T>::schema_id()
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
struct SerdeHelper<T> {
    #[serde(default)]
    scale: Scale,
    nodes: Vec<T>,
}

impl<T: Copy + Eq + hash::Hash> From<ScaledHashRing<T>> for SerdeHelper<T> {
    fn from(ring: ScaledHashRing<T>) -> Self {
        let scale = match ring {
            ScaledHashRing::Raw(_) => Scale::Raw,
            ScaledHashRing::Fair { scale, .. } => Scale::Fair(scale),
        };

        let nodes = ring.unique_nodes().into_iter().collect();

        Self { scale, nodes }
    }
}

impl<T: Copy + hash::Hash> From<SerdeHelper<T>> for ScaledHashRing<T> {
    fn from(helper: SerdeHelper<T>) -> Self {
        let mut ring = match helper.scale {
            Scale::Raw | Scale::Fair(0) => Self::raw(),
            Scale::Fair(scale) => Self::fair(scale),
        };

        for node in helper.nodes {
            ring.add(node);
        }

        ring
    }
}

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Scale {
    Raw,

    #[serde(untagged)]
    Fair(u32),
}

impl Default for Scale {
    fn default() -> Self {
        Self::Fair(DEFAULT_FAIR_HASH_RING_SCALE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_seq_keys() {
        let mut ring = ScaledHashRing::fair(100);
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
        let mut ring = ScaledHashRing::fair(100);

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
