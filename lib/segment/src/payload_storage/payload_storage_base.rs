use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::json_path::JsonPath;
use crate::types::{Filter, Payload};

/// Trait for payload data storage. Should allow filter checks
pub trait PayloadStorage {
    /// Overwrite payload for point_id. If payload already exists, replace it
    fn overwrite(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()>;

    /// Set payload for point_id. If payload already exists, merge it with existing
    fn set(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()>;

    /// Set payload to a point_id by key. If payload already exists, merge it with existing
    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
    ) -> OperationResult<()>;

    /// Get payload for point. If no payload found, return empty payload
    fn get(&self, point_id: PointOffsetType) -> OperationResult<Payload>;

    /// Delete payload by point_id and key
    fn delete(&mut self, point_id: PointOffsetType, key: &JsonPath) -> OperationResult<Vec<Value>>;

    /// Clear all payload of the point
    fn clear(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>>;

    /// Completely delete payload storage. Pufff!
    fn wipe(&mut self) -> OperationResult<()>;

    /// Return function that forces persistence of current storage state.
    fn flusher(&self) -> Flusher;
}

pub trait ConditionChecker {
    /// Check if point satisfies filter condition. Return true if satisfies
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool;
}

pub trait FilterContext {
    /// Check if point satisfies filter condition. Return true if satisfies
    fn check(&self, point_id: PointOffsetType) -> bool;
}

pub type PayloadStorageSS = dyn PayloadStorage + Sync + Send;
pub type ConditionCheckerSS = dyn ConditionChecker + Sync + Send;
