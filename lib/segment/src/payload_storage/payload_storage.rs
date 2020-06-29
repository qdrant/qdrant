use std::collections::BTreeMap;
use crate::types::{PointOffsetType, PayloadKeyType, PayloadType, Filter};


pub type TheMap<K, V> = BTreeMap<K, V>;

/// Trait for payload data storage. Should allow filter checks
pub trait PayloadStorage {
    fn assign(&mut self, point_id: PointOffsetType, key: &PayloadKeyType, payload: PayloadType);

    /// Get payload for point
    fn payload(&self, point_id: PointOffsetType) -> TheMap<PayloadKeyType, PayloadType>;

    /// Delete payload
    fn delete(&mut self, point_id: PointOffsetType, key: &PayloadKeyType);

    /// Drop all payload of the point
    fn drop(&mut self, point_id: PointOffsetType);
}

pub trait DeletedFlagStorage {
    /// Assign deleted flag to a segment point. Marked point will not be used in search and might be removed on segment merge\rebuild
    fn mark_deleted(&mut self, point_id: PointOffsetType);

    /// Check if point is marked deleted
    fn is_deleted(&self, point_id: PointOffsetType) -> bool;
}


pub trait ConditionChecker {
    /// Check if point satisfies filter condition
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool;
}
