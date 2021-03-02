

use crate::types::{FieldCondition, PointIdType};
use std::collections::HashSet;

pub mod numeric_index;
pub mod geo_index;
pub mod map_index;
pub mod field_index;
pub mod index_selector;

#[derive(Debug, Clone)]
pub enum PrimaryCondition {
    Condition(FieldCondition),
    Ids(HashSet<PointIdType>)
}

#[derive(Debug)]
pub struct CardinalityEstimation {
    /// Conditions that could be used to mane a primary point selection.
    pub primary_clauses: Vec<PrimaryCondition>,
    /// Minimal possible matched points in best case for a query
    pub min: usize,
    /// Expected number of matched points for a query
    pub exp: usize,
    /// The largest possible number of matched points in a worst case for a query
    pub max: usize
}

impl CardinalityEstimation {
    pub fn exact(count: usize) -> Self {
        CardinalityEstimation {
            primary_clauses: vec![],
            min: count,
            exp: count,
            max: count
        }
    }
}