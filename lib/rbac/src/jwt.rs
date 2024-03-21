use std::collections::HashMap;

use segment::json_path::JsonPath;
use segment::types::{Condition, FieldCondition, Filter, Match, ValueVariants};
use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Claims {
    /// Expiration time (seconds since UNIX epoch)
    pub exp: Option<u64>,

    /// Validate this token by looking for a value inside a collection.
    pub value_exists: Option<ValueExists>,

    /// Defines specific access within the cluster.
    ///
    /// The default is global read access (same as read-only API key)
    pub access: AccessClaim,
}

impl Validate for Claims {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        let mut errors = validator::ValidationErrors::new();
        let Self {
            exp: _,
            value_exists,
            access,
        } = self;

        if matches!(access, AccessClaim::Scoped(scoped) if scoped.collections.is_empty()) {
            errors.add(
                "access.collections",
                validator::ValidationError {
                    code: "invalid_token".into(),
                    message: Some("Scoped access must allow at least one collection".into()),
                    params: HashMap::new(),
                },
            );
        }
        if let Some(value_exists) = value_exists {
            if access.allows_collection(&value_exists.collection) {
                errors.add(
                    "value_exists.collection",
                    validator::ValidationError {
                        code: "invalid_token".into(),
                        message: Some("Collection for `value_exists` is not allowed".into()),
                        params: HashMap::new(),
                    },
                );
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, Default)]
pub enum Privilege {
    #[default]
    #[serde(rename = "r")]
    Read,

    #[serde(rename = "rw")]
    Write,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct ScopedAccess {
    /// Collection names that are allowed to be accessed
    pub collections: Vec<String>,

    /// Access privilege
    #[serde(default)]
    pub privilege: Privilege,

    /// Payload constraints.
    /// An object where each key is a JSON path, and each value is JSON value.
    pub payload: Option<PayloadClaim>,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum AccessClaim {
    Global(Privilege),
    Scoped(ScopedAccess),
}

impl Default for AccessClaim {
    fn default() -> Self {
        AccessClaim::Global(Privilege::Read)
    }
}

impl AccessClaim {
    pub fn has_some_write_privileges(&self) -> bool {
        match self {
            AccessClaim::Global(Privilege::Write) => true,
            AccessClaim::Scoped(scoped) => scoped.privilege == Privilege::Write,
            _ => false,
        }
    }

    pub fn is_scoped(&self) -> bool {
        matches!(self, AccessClaim::Scoped(_))
    }

    pub fn allows_collection(&self, collection: &str) -> bool {
        match self {
            AccessClaim::Global(_) => true,
            AccessClaim::Scoped(scoped) => scoped.collections.contains(&collection.to_string()),
        }
    }

    pub fn payload_claim(&self) -> Option<&PayloadClaim> {
        match self {
            AccessClaim::Global(_) => None,
            AccessClaim::Scoped(scoped) => scoped.payload.as_ref(),
        }
    }
}

pub type PayloadClaim = HashMap<JsonPath, ValueVariants>;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct ValueExists {
    pub collection: String,
    pub key: JsonPath,
    pub value: ValueVariants,
}

impl ValueExists {
    pub fn to_filter(&self) -> Filter {
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            self.key.clone(),
            Match::new_value(self.value.clone()),
        )))
    }
}
