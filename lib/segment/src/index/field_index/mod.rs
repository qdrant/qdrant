use crate::types::{FieldCondition, IsEmptyCondition, PointOffsetType};
use std::collections::HashSet;

mod field_index_base;
#[allow(dead_code)]
pub mod geo_hash;
mod geo_index;
pub mod index_selector;
pub mod map_index;
pub mod numeric_index;
mod stat_tools;

pub use field_index_base::*;

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum PrimaryCondition {
    Condition(FieldCondition),
    IsEmpty(IsEmptyCondition),
    Ids(HashSet<PointOffsetType>),
}

#[derive(Debug, Clone)]
pub struct PayloadBlockCondition {
    pub condition: FieldCondition,
    pub cardinality: usize,
}

#[derive(Debug, Clone)]
pub struct CardinalityEstimation {
    /// Conditions that could be used to mane a primary point selection.
    pub primary_clauses: Vec<PrimaryCondition>,
    /// Minimal possible matched points in best case for a query
    pub min: usize,
    /// Expected number of matched points for a query, assuming even random distribution if stored data
    pub exp: usize,
    /// The largest possible number of matched points in a worst case for a query
    pub max: usize,
}

impl CardinalityEstimation {
    #[allow(dead_code)]
    pub fn exact(count: usize) -> Self {
        CardinalityEstimation {
            primary_clauses: vec![],
            min: count,
            exp: count,
            max: count,
        }
    }

    /// Generate estimation for unknown filter
    pub fn unknown(total: usize) -> Self {
        CardinalityEstimation {
            primary_clauses: vec![],
            min: 0,
            exp: total / 2,
            max: total,
        }
    }
}
