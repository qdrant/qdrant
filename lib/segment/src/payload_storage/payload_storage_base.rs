use serde_json::Value;

use crate::entry::entry_point::OperationResult;
use crate::types::{Filter, Payload, PayloadKeyTypeRef, PointOffsetType};

/// Trait for payload data storage. Should allow filter checks
pub trait PayloadStorage {
    /// Assign same payload to each given point
    fn assign_all(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        self.drop(point_id)?;
        self.assign(point_id, payload)?;
        Ok(())
    }

    /// Assign payload to a concrete point with a concrete payload value
    fn assign(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()>;

    /// Get payload for point
    fn payload(&self, point_id: PointOffsetType) -> OperationResult<Payload>;

    /// Delete payload by key
    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<Value>>;

    /// Drop all payload of the point
    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>>;

    /// Completely drop payload. Pufff!
    fn wipe(&mut self) -> OperationResult<()>;

    /// Force persistence of current storage state.
    fn flush(&self) -> OperationResult<()>;
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
