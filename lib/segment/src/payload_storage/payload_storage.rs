use std::collections::HashMap;
use crate::types::{PointOffsetType, PayloadKeyType, PayloadType, Filter};


/// Trait for payload data storage. Should allow filter checks
pub trait PayloadStorage {
  fn assign(&mut self, point_id: PointOffsetType, key: &PayloadKeyType, payload: PayloadType);
  
  /// Estimate amount of points (min, max) which satisfies filtering condition.
  fn estimate_cardinality(&self, query: &Filter) -> (usize, usize);

  /// Return list of all point ids, which satisfy filtering criteria
  fn query_points(&self, query: &Filter) -> Vec<PointOffsetType>;

  /// Check if point satisfies filter condition
  fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool;

  /// Get payload for point
  fn payload(&self, point_id: PointOffsetType) -> HashMap<PayloadKeyType, PayloadType>;

  /// Delete payload
  fn delete(&mut self, point_id: PointOffsetType, key: &PayloadKeyType);

  // Drop all payload of the point
  fn drop(&mut self, point_id: PointOffsetType);

  // Assign deleted flag to a segment point. Marked point will not be used in search and might be removed on segment merge\rebuild
  fn mark_deleted(&mut self, point_id: PointOffsetType);

  // Check if point is marked deleted
  fn is_deleted(&self, point_id: PointOffsetType) -> bool;
}