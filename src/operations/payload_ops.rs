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
    SetTags {
        collection: CollectionUpdateInfo,
        payload: Vec<NamedPayload>,
        points: Vec<PointIdType>,
    },
    /// Deletes specified tags if they are assigned
    DeleteTags {
        collection: CollectionUpdateInfo,
        tags: Vec<PayloadKeyType>,
        points: Vec<PointIdType>,
    },
    /// Drops all tags associated with given points.
    ClearTags {
        collection: CollectionUpdateInfo,
        points: Vec<PointIdType>,
    },
    /// Drops all tags in given collection.
    WipeTags {
        collection: CollectionUpdateInfo,
    }
}
