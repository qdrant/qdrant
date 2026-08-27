//! Types describing a single pending operation buffered by a proxy segment.

use ahash::AHashMap;

use crate::types::{PayloadFieldSchema, PointIdType, SeqNumberType};

pub type DeletedPoints = AHashMap<PointIdType, ProxyDeletedPoint>;

/// Point version information of points to delete from a wrapped proxy segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyDeletedPoint {
    /// Version the point had in the wrapped segment when the delete was scheduled.
    /// We use it to determine if some other proxy segment should move the point again with
    /// `move_if_exists` if it has newer point data.
    pub local_version: SeqNumberType,
    /// Version of the operation that caused the delete to be scheduled.
    /// We use it for the delete operations when propagating them to the wrapped or optimized
    /// segment.
    pub operation_version: SeqNumberType,
}

/// A pending payload index change buffered by a proxy segment.
#[derive(Debug, Clone)]
pub enum ProxyIndexChange {
    Create(PayloadFieldSchema, SeqNumberType),
    Delete(SeqNumberType),
    DeleteIfIncompatible(SeqNumberType, PayloadFieldSchema),
}

impl ProxyIndexChange {
    pub fn version(&self) -> SeqNumberType {
        match self {
            ProxyIndexChange::Create(_, version) => *version,
            ProxyIndexChange::Delete(version) => *version,
            ProxyIndexChange::DeleteIfIncompatible(version, _) => *version,
        }
    }
}
