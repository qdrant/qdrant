use std::collections::HashSet;
use std::hash;

/// Default number of virtual nodes for the `ScaledHashRing::Fair` used in Qdrant code.
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

impl From<u32> for Scale {
    fn from(scale: u32) -> Self {
        Self::Fair(scale)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

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

    // Serialization format for `ScaledHashRing` is pretty trivial... *but* we are using
    // `#[serde(from/into)]` with `SerdeHelper`, so this test ensures we convert stuff properly.
    #[rstest]
    #[case::empty_raw_hashring(Scale::Raw, 0)]
    #[case::empty_fair_hashring(100, 0)]
    #[case::empty_fair_hashring_scale_1(1, 0)]
    #[case::raw_hashring(Scale::Raw, 5)]
    #[case::fair_hashring(100, 5)]
    #[case::fair_hashring_scale_1(1, 5)]
    fn test_hashring_serde(#[case] scale: impl Into<Scale>, #[case] nodes: u32) {
        let scale = scale.into();

        let expected_hashring = hashring(scale, nodes);
        let expected_json = json(scale, nodes);

        test_serialization(expected_hashring.clone(), &expected_json);
        test_deserialization(expected_json, &expected_hashring);
    }

    // There's a special-case simplification in `SerdeHelper`: `Scale::Fair(0)` is deserialized
    // as `Scale::Raw`. This test enforces this behavior, to ensure we notice if/when it is changed.
    #[rstest]
    #[case::empty(0)]
    #[case::non_empty(5)]
    fn test_fair_hashring_scale_0_deserialization(#[case] nodes: u32) {
        test_deserialization(json(0, nodes), &hashring(Scale::Raw, nodes));
    }

    fn test_serialization(hashring: ScaledHashRing<u32>, expected_json: &serde_json::Value) {
        let mut serialized_json =
            serde_json::to_value(hashring).expect("ScaledHashRing can be serialized to JSON");

        let nodes = serialized_json
            .get_mut("nodes")
            .and_then(|nodes| nodes.as_array_mut());

        if let Some(nodes) = nodes {
            nodes.sort_unstable_by_key(|node| node.as_u64());
        }

        assert_eq!(&serialized_json, expected_json);
    }

    fn test_deserialization(json: serde_json::Value, expected_hashring: &ScaledHashRing<u32>) {
        let deserialized_hashring: ScaledHashRing<u32> =
            serde_json::from_value(json).expect("ScaledHashRing can be deserialized from JSON");

        assert_eq!(&deserialized_hashring, expected_hashring);
    }

    fn hashring(scale: impl Into<Scale>, nodes: u32) -> ScaledHashRing<u32> {
        let mut ring = match scale.into() {
            Scale::Raw => ScaledHashRing::raw(),
            Scale::Fair(scale) => ScaledHashRing::fair(scale),
        };

        for node in 0..nodes {
            ring.add(node);
        }

        ring
    }

    fn json(scale: impl Into<Scale>, nodes: u32) -> serde_json::Value {
        let scale = match scale.into() {
            Scale::Raw => json!("raw"),
            Scale::Fair(scale) => json!(scale),
        };

        let nodes: Vec<_> = (0..nodes).collect();

        json!({
            "scale": scale,
            "nodes": nodes,
        })
    }
}
