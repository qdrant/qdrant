use std::collections::HashMap;

use segment::json_path::JsonPath;
use segment::types::ValueVariants;

#[derive(Clone)]
pub struct Access {
    /// Collection names that are allowed to be accessed
    pub collections: Option<Vec<String>>,

    /// Payload constraints.
    /// An object where each key is a JSON path, and each value is JSON value.
    pub payload: Option<PayloadClaim>,
}

impl Access {
    // TODO: add an explanation comment in all places where this is used
    pub fn full() -> Self {
        Self {
            collections: None,
            payload: None,
        }
    }
}

pub type PayloadClaim = HashMap<JsonPath, ValueVariants>;
