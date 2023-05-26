use std::fmt::Display;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::types::PointIdType;

/// Value of the group_by key, shared across all the hits in the group
#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Hash)]
#[serde(untagged)]
pub enum GroupId {
    String(String),
    NumberU64(u64),
    NumberI64(i64),
}

impl From<PseudoId> for GroupId {
    fn from(id: PseudoId) -> Self {
        match id {
            PseudoId::String(s) => Self::String(s),
            PseudoId::NumberU64(n) => Self::NumberU64(n),
            PseudoId::NumberI64(n) => Self::NumberI64(n),
        }
    }
}

impl From<u64> for GroupId {
    fn from(id: u64) -> Self {
        GroupId::NumberU64(id)
    }
}

impl From<i64> for GroupId {
    fn from(id: i64) -> Self {
        GroupId::NumberI64(id)
    }
}

impl From<String> for GroupId {
    fn from(id: String) -> Self {
        GroupId::String(id)
    }
}

impl From<&str> for GroupId {
    fn from(id: &str) -> Self {
        GroupId::String(id.to_string())
    }
}

impl From<GroupId> for serde_json::Value {
    fn from(key: GroupId) -> Self {
        match key {
            GroupId::String(s) => serde_json::Value::String(s),
            GroupId::NumberU64(n) => json!(n),
            GroupId::NumberI64(n) => json!(n),
        }
    }
}

impl TryFrom<&serde_json::Value> for GroupId {
    type Error = ();

    /// Only allows Strings and Numbers to be converted into Scalar
    fn try_from(value: &serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::String(s) => Ok(Self::String(s.to_string())),
            serde_json::Value::Number(n) => {
                if let Some(n_i64) = n.as_i64() {
                    Ok(Self::NumberI64(n_i64))
                } else if let Some(n_u64) = n.as_u64() {
                    Ok(Self::NumberU64(n_u64))
                } else {
                    Err(())
                }
            }
            _ => Err(()),
        }
    }
}

impl GroupId {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            GroupId::NumberI64(id) => Some(*id),
            GroupId::NumberU64(id) => i64::try_from(*id).ok(),
            GroupId::String(_) => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            GroupId::NumberI64(id) => u64::try_from(*id).ok(),
            GroupId::NumberU64(id) => Some(*id),
            GroupId::String(_) => None,
        }
    }
}

/// A value that can be used as a temporary ID
#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Hash)]
#[serde(untagged)]
pub enum PseudoId {
    String(String),
    NumberU64(u64),
    NumberI64(i64),
}

impl Display for PseudoId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PseudoId::String(s) => write!(f, "{}", s),
            PseudoId::NumberU64(n) => write!(f, "{}", n),
            PseudoId::NumberI64(n) => write!(f, "{}", n),
        }
    }
}

impl From<GroupId> for PseudoId {
    fn from(id: GroupId) -> Self {
        match id {
            GroupId::String(s) => Self::String(s),
            GroupId::NumberU64(n) => Self::NumberU64(n),
            GroupId::NumberI64(n) => Self::NumberI64(n),
        }
    }
}

impl TryFrom<PseudoId> for PointIdType {
    type Error = ();

    fn try_from(value: PseudoId) -> Result<Self, Self::Error> {
        match value {
            PseudoId::String(s) => Ok(PointIdType::Uuid(Uuid::try_parse(&s).map_err(|_| ())?)),
            PseudoId::NumberU64(n) => Ok(PointIdType::NumId(n)),
            PseudoId::NumberI64(n) => Ok(PointIdType::NumId(u64::try_from(n).map_err(|_| ())?)),
        }
    }
}

impl From<PointIdType> for PseudoId {
    fn from(id: PointIdType) -> Self {
        match id {
            PointIdType::NumId(n) => PseudoId::NumberU64(n),
            PointIdType::Uuid(u) => PseudoId::String(u.to_string()),
        }
    }
}

impl From<u64> for PseudoId {
    fn from(id: u64) -> Self {
        PseudoId::NumberU64(id)
    }
}

impl From<i64> for PseudoId {
    fn from(id: i64) -> Self {
        PseudoId::NumberI64(id)
    }
}

impl From<String> for PseudoId {
    fn from(id: String) -> Self {
        PseudoId::String(id)
    }
}

impl From<&str> for PseudoId {
    fn from(id: &str) -> Self {
        PseudoId::String(id.to_string())
    }
}
