use std::fmt::Display;

use schemars::JsonSchema;
use segment::data_types::groups::GroupId;
use segment::types::PointIdType;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::WithLookup;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum WithLookupInterface {
    Collection(String),
    WithLookup(WithLookup),
}

impl From<WithLookupInterface> for WithLookup {
    fn from(with_lookup: WithLookupInterface) -> Self {
        match with_lookup {
            WithLookupInterface::Collection(collection_name) => Self {
                collection_name,
                with_payload: Some(true.into()),
                with_vectors: Some(false.into()),
            },
            WithLookupInterface::WithLookup(with_lookup) => with_lookup,
        }
    }
}

/// A value that can be used as a temporary ID
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
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

impl From<PseudoId> for GroupId {
    fn from(id: PseudoId) -> Self {
        match id {
            PseudoId::String(s) => Self::String(s),
            PseudoId::NumberU64(n) => Self::NumberU64(n),
            PseudoId::NumberI64(n) => Self::NumberI64(n),
        }
    }
}

#[derive(Debug)]
pub enum ConversionError {
    IntError(core::num::TryFromIntError),
    ParseError(uuid::Error),
}

impl TryFrom<PseudoId> for PointIdType {
    type Error = ConversionError;

    fn try_from(value: PseudoId) -> Result<Self, Self::Error> {
        match value {
            PseudoId::String(s) => Ok(PointIdType::Uuid(
                Uuid::try_parse(&s).map_err(ConversionError::ParseError)?,
            )),
            PseudoId::NumberU64(n) => Ok(PointIdType::NumId(n)),
            PseudoId::NumberI64(n) => Ok(PointIdType::NumId(
                u64::try_from(n).map_err(ConversionError::IntError)?,
            )),
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
