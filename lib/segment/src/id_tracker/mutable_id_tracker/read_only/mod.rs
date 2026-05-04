pub mod id_tracker_read;

use std::path::PathBuf;

use crate::id_tracker::point_mappings::PointMappings;
use crate::types::SeqNumberType;

/// Implementation of read-only ID tracker which operates
/// on top of appendable data format.
///
/// Structure can't modify data itself, but can consume appends from external entity by
/// doing live-reload.
pub struct ReadOnlyAppendableIdTracker {
    segment_path: PathBuf,
    internal_to_version: Vec<SeqNumberType>,
    mappings: PointMappings,
}
