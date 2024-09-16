use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use serde_json::Value;

use super::field_index::FieldIndex;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::json_path::JsonPath;
use crate::payload_storage::FilterContext;
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType,
};

pub trait PayloadIndex {
    /// Get indexed fields
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema>;

    /// Build the index, if not built before, taking the caller by reference only
    fn build_index(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<Option<Vec<FieldIndex>>>;

    /// Apply already built indexes
    fn apply_index(
        &mut self,
        field: PayloadKeyType,
        payload_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<()>;

    /// Mark field as one which should be indexed
    fn set_indexed(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_schema: impl Into<PayloadFieldSchema>,
    ) -> OperationResult<()> {
        let payload_schema = payload_schema.into();

        let Some(field_index) = self.build_index(field, &payload_schema)? else {
            return Ok(());
        };

        self.apply_index(field.to_owned(), payload_schema, field_index)?;

        Ok(())
    }

    /// Remove index
    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<()>;

    /// Estimate amount of points (min, max) which satisfies filtering condition.
    ///
    /// A best estimation of the number of available points should be given.
    fn estimate_cardinality(&self, query: &Filter) -> CardinalityEstimation;

    /// Estimate amount of points (min, max) which satisfies filtering of a nested condition.
    fn estimate_nested_cardinality(
        &self,
        query: &Filter,
        nested_path: &JsonPath,
    ) -> CardinalityEstimation;

    /// Return list of all point ids, which satisfy filtering criteria
    ///
    /// A best estimation of the number of available points should be given.
    fn query_points(&self, query: &Filter) -> Vec<PointOffsetType>;

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

    /// Overwrite payload for point_id. If payload already exists, replace it.
    fn overwrite_payload(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
    ) -> OperationResult<()>;

    /// Assign payload to a concrete point with a concrete payload value
    fn set_payload(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &Option<JsonPath>,
    ) -> OperationResult<()>;

    /// Get payload for point
    fn get_payload(&self, point_id: PointOffsetType) -> OperationResult<Payload>;

    /// Delete payload by key
    fn delete_payload(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Vec<Value>>;

    /// Drop all payload of the point
    fn clear_payload(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>>;

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
