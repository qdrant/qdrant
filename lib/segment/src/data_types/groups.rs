use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

/// Value of the group_by key, shared across all the hits in the group
#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Hash)]
#[serde(untagged)]
pub enum GroupId {
    String(String),
    NumberU64(u64),
    NumberI64(i64),
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

impl From<GroupId> for JsonValue {
    fn from(key: GroupId) -> Self {
        match key {
            GroupId::String(s) => JsonValue::String(s),
            GroupId::NumberU64(n) => json!(n),
            GroupId::NumberI64(n) => json!(n),
        }
    }
}

impl TryFrom<&JsonValue> for GroupId {
    type Error = ();

    /// Only allows Strings and Numbers to be converted into GroupId
    fn try_from(value: &JsonValue) -> Result<Self, Self::Error> {
        match value {
            JsonValue::String(s) => Ok(Self::String(s.clone())),
            JsonValue::Number(n) if let Some(n_u64) = n.as_u64() => Ok(Self::NumberU64(n_u64)),
            JsonValue::Number(n) if let Some(n_i64) = n.as_i64() => Ok(Self::NumberI64(n_i64)),
            JsonValue::Number(_) => Err(()),
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Array(_) | JsonValue::Object(_) => {
                Err(())
            }
        }
    }
}

impl GroupId {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            GroupId::NumberI64(id) => u64::try_from(*id).ok(),
            GroupId::NumberU64(id) => Some(*id),
            GroupId::String(_) => None,
        }
    }
}
