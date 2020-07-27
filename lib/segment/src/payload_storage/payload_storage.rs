use std::collections::BTreeMap;
use crate::types::{PointOffsetType, PayloadKeyType, PayloadType, Filter, TheMap};


/// Trait for payload data storage. Should allow filter checks
pub trait PayloadStorage {
    fn assign_all(&mut self, point_id: PointOffsetType, payload: TheMap<PayloadKeyType, PayloadType>) {
        self.drop(point_id);
        for (key, value) in payload {
            self.assign(point_id, &key, value)
        }
    }

    fn assign(&mut self, point_id: PointOffsetType, key: &PayloadKeyType, payload: PayloadType);

    /// Get payload for point
    fn payload(&self, point_id: PointOffsetType) -> TheMap<PayloadKeyType, PayloadType>;

    /// Delete payload by key
    fn delete(&mut self, point_id: PointOffsetType, key: &PayloadKeyType) -> Option<PayloadType>;

    /// Drop all payload of the point
    fn drop(&mut self, point_id: PointOffsetType) -> Option<TheMap<PayloadKeyType, PayloadType>>;

    /// Completely drop payload. Pufff!
    fn wipe(&mut self);
}


pub trait ConditionChecker {
    /// Check if point satisfies filter condition
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool;
}
