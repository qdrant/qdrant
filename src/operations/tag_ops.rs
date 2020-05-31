use super::common::CollectionUpdateInfo;
use crate::common::types::{PointIdType, TagIdType};
/// Define operations description for point tags manipulation
use serde::{Deserialize, Serialize};

/// Appends all given tags to each given point
#[derive(Debug, Deserialize, Serialize)]
pub struct SetTags {
    pub collection: CollectionUpdateInfo,
    pub tags: Vec<TagIdType>,
    pub points: Vec<PointIdType>,
}

/// Deletes specified tags if they are assigned
#[derive(Debug, Deserialize, Serialize)]
pub struct DeleteTags {
    pub collection: CollectionUpdateInfo,
    pub tags: Vec<TagIdType>,
    pub points: Vec<PointIdType>,
}

/// Drops all tags associated with given points.
#[derive(Debug, Deserialize, Serialize)]
pub struct ClearTags {
    pub collection: CollectionUpdateInfo,
    pub points: Vec<PointIdType>,
}

/// Drops all tags in given collection.
#[derive(Debug, Deserialize, Serialize)]
pub struct WipeTags {
    pub collection: CollectionUpdateInfo,
}
