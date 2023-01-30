use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use serde_json::Value;

use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::payload_storage::FilterContext;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType,
    PointOffsetType, SearchParams,
};
use crate::vector_storage::ScoredPointOffset;

/// Trait for vector searching
pub trait VectorIndex {
    /// Return list of Ids with fitting
    fn search(
        &self,
        vectors: &[&[VectorElementType]],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<Vec<ScoredPointOffset>>;

    /// Force internal index rebuild.
    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()>;

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry;

    fn files(&self) -> Vec<PathBuf>;
}

pub trait PayloadIndex {
    /// Get indexed fields
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema>;

    /// Mark field as one which should be indexed
    fn set_indexed(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_schema: PayloadFieldSchema,
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

    /// Return number of points, indexed by this field
    fn indexed_points(&self, field: PayloadKeyTypeRef) -> usize;

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

    /// Return function that forces persistence of current storage state.
    fn flusher(&self) -> Flusher;

    /// Infer payload type from existing payload data.
    fn infer_payload_type(
        &self,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<PayloadSchemaType>>;

    fn take_database_snapshot(&self, path: &Path) -> OperationResult<()>;

    fn files(&self) -> Vec<PathBuf>;
}

pub type VectorIndexSS = dyn VectorIndex + Sync + Send;
