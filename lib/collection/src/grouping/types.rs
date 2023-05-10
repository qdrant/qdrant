use std::collections::HashSet;
use std::hash::Hash;

use segment::types::{ExtendedPointId, PointGroup, ScoredPoint};
use AggregatorError::BadKeyType;

#[derive(PartialEq, Debug)]
pub(super) enum AggregatorError {
    BadKeyType,
    KeyNotFound,
}
#[derive(Debug, Clone)]
pub(super) struct Group {
    pub hits: Vec<HashablePoint>,
    pub key: GroupKey,
    pub group_by: String,
}

impl Group {
    pub(super) fn hydrate_from(&mut self, set: &HashSet<HashablePoint>) {
        self.hits.iter_mut().for_each(|hit| {
            if let Some(point) = set.get(hit) {
                hit.0.payload = point.0.payload.clone();
                hit.0.vector = point.0.vector.clone();
            }
        });
    }
}

impl From<Group> for PointGroup {
    fn from(group: Group) -> Self {
        let mut group_id = serde_json::Map::new();
        group_id.insert(group.group_by, group.key.into());

        Self {
            hits: group.hits.into_iter().map(|hp| hp.0).collect(),
            group_id,
        }
    }
}

/// Abstraction over serde_json::Value to be used as a key in a HashMap/HashSet
#[derive(Debug, Eq, PartialEq, Clone)]
pub(super) enum GroupKey {
    String(String),
    Number(serde_json::Number),
}

impl TryFrom<serde_json::Value> for GroupKey {
    type Error = AggregatorError;

    /// Only allows Strings and Numbers to be converted into GroupKey
    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::String(s) => Ok(Self::String(s)),
            serde_json::Value::Number(n) => Ok(Self::Number(n)),
            _ => Err(BadKeyType),
        }
    }
}

impl From<GroupKey> for serde_json::Value {
    fn from(key: GroupKey) -> Self {
        match key {
            GroupKey::String(s) => serde_json::Value::String(s),
            GroupKey::Number(n) => serde_json::Value::Number(n),
        }
    }
}

impl Hash for GroupKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self {
            Self::Number(n) => n.hash(state),
            Self::String(s) => s.hash(state),
        }
    }
}

/// Abstraction over ScoredPoint to be used in a HashSet
#[derive(Eq, Debug, PartialEq, Clone)]
pub(super) struct HashablePoint(ScoredPoint);

impl HashablePoint {
    pub fn minimal_from(point: ScoredPoint) -> Self {
        Self(ScoredPoint {
            id: point.id,
            version: point.version,
            score: point.score,
            payload: None,
            vector: None,
        })
    }

    pub fn id(&self) -> ExtendedPointId {
        self.0.id
    }

    #[cfg(test)]
    pub fn payload(&self) -> Option<&segment::types::Payload> {
        self.0.payload.as_ref()
    }
}

impl Hash for HashablePoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.id.hash(state);
        self.0.version.hash(state);
    }
}

impl Ord for HashablePoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.score.partial_cmp(&other.0.score).unwrap()
    }
}

impl PartialOrd for HashablePoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.score.partial_cmp(&other.0.score)
    }
}

impl From<ScoredPoint> for HashablePoint {
    fn from(point: ScoredPoint) -> Self {
        Self(point)
    }
}
impl From<HashablePoint> for ScoredPoint {
    fn from(point: HashablePoint) -> Self {
        point.0
    }
}
