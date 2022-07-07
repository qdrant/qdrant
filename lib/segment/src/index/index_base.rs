use crate::entry::entry_point::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::payload_storage::FilterContext;
use crate::types::{
    Filter, Payload, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType, PointOffsetType,
    SearchParams, VectorElementType,
};
use crate::vector_storage::ScoredPointOffset;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use tokio::runtime::Handle;

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

    fn batch_search(
        &self,
        vectors: &[Vec<VectorElementType>],
        filters: &[Option<Filter>],
        top: usize,
        params: Option<&SearchParams>,
        runtime_handle: &Handle,
    ) -> Vec<Vec<ScoredPointOffset>>;

    /// Force internal index rebuild.
    fn build_index(&self, stopped: &AtomicBool) -> OperationResult<()>;
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

    /// Infer payload type from existing payload data.
    fn infer_payload_type(
        &self,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<PayloadSchemaType>>;
}

pub type VectorIndexSS = dyn VectorIndex + Sync + Send;
