use segment::json_path::JsonPath;
use segment::types::{Condition, FieldCondition, Filter, Match, ValueVariants};
use serde::{Deserialize, Serialize};
use storage::rbac::Access;
use validator::{Validate, ValidationErrors};

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Claims {
    /// The subject ID; can be a subscription ID, cluster ID, or user ID
    pub sub: Option<String>,

    /// Expiration time (seconds since UNIX epoch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,

    #[serde(default = "default_access")]
    pub access: Access,

    /// Validate this token by looking for a value inside a collection.
    pub value_exists: Option<ValueExists>,

    /// An arbitrary subject string provided by the client, used for audit logging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
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

fn default_access() -> Access {
    Access::full("Give full access when the access field is not present")
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

impl Validate for Claims {
    fn validate(&self) -> Result<(), ValidationErrors> {
        ValidationErrors::merge_all(Ok(()), "access", self.access.validate())
    }
}
