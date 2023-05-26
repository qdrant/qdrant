use std::fmt::Display;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::types::PointIdType;

pub type GroupId = PseudoId;

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

impl From<PseudoId> for serde_json::Value {
    fn from(key: PseudoId) -> Self {
        match key {
            PseudoId::String(s) => serde_json::Value::String(s),
            PseudoId::NumberU64(n) => json!(n),
            PseudoId::NumberI64(n) => json!(n),
        }
    }
}

impl TryFrom<&serde_json::Value> for PseudoId {
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

impl PseudoId {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            PseudoId::NumberI64(id) => Some(*id),
            PseudoId::NumberU64(id) => i64::try_from(*id).ok(),
            PseudoId::String(_) => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            PseudoId::NumberI64(id) => u64::try_from(*id).ok(),
            PseudoId::NumberU64(id) => Some(*id),
            PseudoId::String(_) => None,
        }
    }
}
