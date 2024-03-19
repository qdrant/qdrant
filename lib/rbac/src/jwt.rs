use std::collections::HashMap;

use segment::json_path::JsonPath;
use segment::types::{Condition, FieldCondition, Filter, Match, ValueVariants};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Claims {
    /// Expiration time (seconds since UNIX epoch)
    pub exp: Option<u64>,

    /// Write access, default is false. Read access is always enabled with a valid token.
    pub w: Option<bool>,

    /// Collection names that are allowed to be accessed
    pub collections: Option<Vec<String>>,

    /// Payload constraints.
    /// An object where each key is a JSON path, and each value is JSON value.
    pub payload: Option<PayloadClaim>,

    /// Validate this token by looking for a value inside a collection.
    pub value_exists: Option<ValueExists>,
}

pub type PayloadClaim = HashMap<JsonPath, ValueVariants>;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct ValueExists {
    pub collection: String,
    pub key: JsonPath,
    pub value: ValueVariants,
}

impl ValueExists {
    pub fn as_filter(&self) -> Filter {
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            self.key.clone(),
            Match::new_value(self.value.clone()),
        )))
    }
}
