use std::default::Default;

use super::common::CollectionUpdateInfo;
use crate::common::types::{PointIdType, PayloadKeyType, PayloadType};
use serde;
use serde::{Deserialize, Serialize};


fn default_as_false() -> bool {
    false
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NamedPayload {
    key: PayloadKeyType,
    value: PayloadType,

    #[serde(default = "default_as_false")]
    index: bool
}

/// Define operations description for point payloads manipulation
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadOps {
/// Appends all given tags to each given point
    SetPayload {
        collection: CollectionUpdateInfo,
        payload: Vec<NamedPayload>,
        points: Vec<PointIdType>,
    },
    /// Deletes specified Payload if they are assigned
    DeletePayload {
        collection: CollectionUpdateInfo,
        payload: Vec<PayloadKeyType>,
        points: Vec<PointIdType>,
    },
    /// Drops all Payload associated with given points.
    ClearPayload {
        collection: CollectionUpdateInfo,
        points: Vec<PointIdType>,
    },
    /// Drops all Payload in given collection.
    WipePayload {
        collection: CollectionUpdateInfo,
    }
}
