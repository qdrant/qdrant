use crate::entry::entry_point::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::payload_storage::FilterContext;
use crate::types::{
    Filter, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType, PointOffsetType, SearchParams,
    VectorElementType,
};
use crate::vector_storage::ScoredPointOffset;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

/// Trait for vector searching
pub trait VectorIndex {
    /// Return list of Ids with fitting
    fn search(
        &self,
        vector: &[VectorElementType],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset>;

    /// Force internal index rebuild.
    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()>;
}

pub trait PayloadIndex {
    /// Get indexed fields
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadSchemaType>;

    /// Mark field as one which should be indexed
    fn set_indexed(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_type: PayloadSchemaType,
    ) -> OperationResult<()>;

    /// Remove index
    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<()>;

    /// Estimate amount of points (min, max) which satisfies filtering condition.
    fn estimate_cardinality(&self, query: &Filter) -> CardinalityEstimation;

    /// Return list of all point ids, which satisfy filtering criteria
    fn query_points<'a>(
        &'a self,
        query: &'a Filter,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a>;

    fn filter_context<'a>(&'a self, filter: &'a Filter) -> Box<dyn FilterContext + 'a>;

    /// Iterate conditions for payload blocks with minimum size of `threshold`
    /// Required for building HNSW index
    fn payload_blocks(
        &self,
        field: PayloadKeyTypeRef,
        threshold: usize,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_>;
}

pub type VectorIndexSS = dyn VectorIndex + Sync + Send;
pub type PayloadIndexSS = dyn PayloadIndex + Sync + Send;
