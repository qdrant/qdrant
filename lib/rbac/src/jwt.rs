use std::collections::HashMap;

use segment::json_path::JsonPath;
use segment::types::ValueVariants;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Claims {
    /// Expiration time (seconds since UNIX epoch)
    pub exp: Option<u64>,

    /// Write access, default is false. Read access is always enabled
    pub w: Option<bool>,

    /// Collection names that are allowed to be accessed
    pub collections: Option<Vec<String>>,

    /// Payload constraints.
    /// An object where each key is a JSON path, and each value is JSON value.
    pub payload: Option<PayloadClaim>,
}

pub type PayloadClaim = HashMap<JsonPath, ValueVariants>;
