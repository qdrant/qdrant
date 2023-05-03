use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use segment::types::ScoredPoint;
use serde_json::Value;
use AggregatorError::BadKeyType;

#[derive(PartialEq, Debug)]
pub(super) enum AggregatorError {
    AllGroupsFull,
    BadKeyType,
    KeyNotFound,
    GroupFull,
    EnoughGroups,
}

/// Abstraction over serde_json::Value to be used as a key in a HashMap/HashSet
#[derive(Debug, Eq, PartialEq, Clone)]
pub(super) struct GroupKey(pub serde_json::Value);

impl TryFrom<serde_json::Value> for GroupKey {
    type Error = AggregatorError;

    /// Only allows Strings and Numbers to be converted into GroupKey
    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::String(_) | serde_json::Value::Number(_) => Ok(Self(value)),
            _ => Err(BadKeyType),
        }
    }
}

impl Hash for GroupKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Value::Number(n) => n.hash(state),
            Value::String(s) => s.hash(state),
            _ => unreachable!("GroupKey should only be a number or a string"),
        }
    }
}

/// Abstraction over ScoredPoint to be used in a HashSet
#[derive(Eq, Debug, PartialEq, Clone)]
pub(super) struct HashablePoint(pub ScoredPoint);

impl HashablePoint {
    pub fn minimal_from(point: &ScoredPoint) -> Self {
        Self(ScoredPoint {
            id: point.id,
            version: point.version,
            score: point.score,
            payload: None,
            vector: None,
        })
    }
}

impl Hash for HashablePoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.id.hash(state);
        self.0.version.hash(state);
    }
}

impl Deref for HashablePoint {
    type Target = ScoredPoint;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for HashablePoint {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<&ScoredPoint> for HashablePoint {
    fn from(point: &ScoredPoint) -> Self {
        Self(point.clone())
    }
}
impl From<&HashablePoint> for ScoredPoint {
    fn from(point: &HashablePoint) -> Self {
        point.0.clone()
    }
}
