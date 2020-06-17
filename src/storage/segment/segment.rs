
use crate::common::types::{PayloadKeyType, PayloadType, PointIdType, Filter, SeqNumberType};
use std::path::Path;


/// Trait for versionable & saveable objects.
pub trait VersionedPersistable {
    fn persist(&self, directory: &Path) -> SeqNumberType;
    fn load(directory: &Path) -> Self;

    /// Save latest persisted version in memory, so the object will not be saved too much times
    fn ack_persistance(&mut self, version: SeqNumberType);
}



