use std::collections::HashMap;

use crate::common::types::{PayloadKeyType, PayloadType, PointIdType, Filter};


/// Trait for payload data storage. Should allow filter checks 
pub trait PayloadStorage {
  fn assign(&mut self, point_id: PointIdType, key: &PayloadKeyType, payload: PayloadType);
  
  /// Estimate amount of points (min, max) which satisfies filtering condition.
  fn estimate_cardinality(&self, query: &Filter) -> (usize, usize);

  /// Return list of all point ids
  fn query_points(&self, query: &Filter) -> Vec<PointIdType>;

  /// Check if point satisfies filter condition
  fn check(&self, point_id: PointIdType, query: &Filter) -> bool;

  /// Get payload for point
  fn payload(&self, point_id: PointIdType) -> HashMap<PayloadKeyType, PayloadType>;

  /// Delete payload
  fn delete(&mut self, point_id: PointIdType, key: &PayloadKeyType);

  // Drop all payload of the point
  fn drop(&mut self, point_id: PointIdType);
}