use segment::json_path::JsonPath;
use segment::types::{Condition, FieldCondition, Filter, Match, ValueVariants};
use serde::{Deserialize, Serialize};
use storage::rbac::access::PayloadClaim;

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

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct KeyValuePair {
    key: JsonPath,
    value: ValueVariants,
}

impl KeyValuePair {
    pub fn to_condition(&self) -> Condition {
        Condition::Field(FieldCondition::new_match(
            self.key.clone(),
            Match::new_value(self.value.clone()),
        ))
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct ValueExists {
    collection: String,
    matches: Vec<KeyValuePair>,
}

impl ValueExists {
    pub fn get_collection(&self) -> &str {
        &self.collection
    }

    pub fn to_filter(&self) -> Filter {
        let conditions = self
            .matches
            .iter()
            .map(|pair| pair.to_condition())
            .collect();

        Filter {
            should: None,
            min_should: None,
            must: Some(conditions),
            must_not: None,
        }
    }
}
