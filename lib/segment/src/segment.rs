use std::path::Path;
use crate::types::{SeqNumberType, PointOffsetType, PointIdType};

/// Trait for versionable & saveable objects.
pub trait VersionedPersistable {
    fn persist(&self, directory: &Path) -> SeqNumberType;
    fn load(directory: &Path) -> Self;

    /// Save latest persisted version in memory, so the object will not be saved too much times
    fn ack_persistance(&mut self, version: SeqNumberType);
}


pub trait Segment {
    /// Get current update version of the segement
    fn version(&self) -> SeqNumberType;


    // fn update(&mut self, )
}


