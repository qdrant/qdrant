use super::common::CollectionUpdateInfo;
use crate::common::types::{PointIdType, TagIdType};

use serde::{Deserialize, Serialize};

/// Define operations description for point tags manipulation
#[derive(Debug, Deserialize, Serialize)]
pub enum TagOps {
/// Appends all given tags to each given point
    SetTags {
        collection: CollectionUpdateInfo,
        tags: Vec<TagIdType>,
        points: Vec<PointIdType>,
    },
    /// Deletes specified tags if they are assigned
    DeleteTags {
        collection: CollectionUpdateInfo,
        tags: Vec<TagIdType>,
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
