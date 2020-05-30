/// Define operations description for point tags manipulation
use serde::{Serialize, Deserialize};
use crate::common::types::{TagIdType, PointIdType};
use super::common::CollectionUpdateInfo;

/// Appends all given tags to each given point
#[derive(Debug, Deserialize, Serialize)]
struct SetTags {
  collection: CollectionUpdateInfo,
  tags: Vec<TagIdType>,
  points: Vec<PointIdType>
}

/// Deletes specified tags if they are assigned
#[derive(Debug, Deserialize, Serialize)]
struct DeleteTags {
  collection: CollectionUpdateInfo,
  tags: Vec<TagIdType>,
  points: Vec<PointIdType>
}

/// Drops all tags associated with given points.
#[derive(Debug, Deserialize, Serialize)]
struct ClearTags {
  collection: CollectionUpdateInfo,
  points: Vec<PointIdType>
}

/// Drops all tags in given collection.
#[derive(Debug, Deserialize, Serialize)]
struct WipeTags {
  collection: CollectionUpdateInfo
}
