use std::collections::HashMap;

use segment::types::{PayloadFieldSchema, PayloadKeyType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PayloadIndexSchema {
    pub schema: HashMap<PayloadKeyType, PayloadFieldSchema>,
}
