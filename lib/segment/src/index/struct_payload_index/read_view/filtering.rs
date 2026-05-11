use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::StructPayloadIndexReadView;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexRead, PrimaryCondition, ResolvedHasId,
};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_filter_context::StructFilterContext;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::types::{Condition, FieldCondition, Filter, IsEmptyCondition, IsNullCondition};
use crate::vector_storage::VectorStorageRead;

impl<'a, P, I, V, F> StructPayloadIndexReadView<'a, P, I, V, F>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
    F: FieldIndexRead,
{
    pub fn estimate_field_condition(
        &self,
        condition: &FieldCondition,
        nested_path: Option<&JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        let full_path = JsonPath::extend_or_new(nested_path, &condition.key);
        let Some(indexes) = self.field_indexes.get(&full_path) else {
            return Ok(None);
        };
        // rewrite condition with fullpath to enable cardinality estimation
        let full_path_condition = FieldCondition {
            key: full_path,
            ..condition.clone()
        };
        indexes
            .iter()
            .find_map(|index| {
                index
                    .estimate_cardinality(&full_path_condition, hw_counter)
                    .transpose()
            })
            .transpose()
    }

    pub(super) fn query_field<'q>(
        &'q self,
        condition: &'q PrimaryCondition,
        hw_counter: &'q HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'q>>> {
        match condition {
            PrimaryCondition::Condition(field_condition) => {
                let Some(field_indexes) = self.field_indexes.get(&field_condition.key) else {
                    return Ok(None);
                };
                field_indexes
                    .iter()
                    .find_map(|field_index| {
                        field_index.filter(field_condition, hw_counter).transpose()
                    })
                    .transpose()
            }
            PrimaryCondition::Ids(ids) => {
                Ok(Some(Box::new(ids.resolved_point_offsets.iter().copied())))
            }
            PrimaryCondition::HasVector(_) => Ok(None),
        }
    }

    pub fn struct_filtered_context<'q>(
        &'q self,
        filter: &'q Filter,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<StructFilterContext<'q>> {
        let payload_provider = PayloadProvider::new(self.payload.clone());

        let (optimized_filter, _) = self.optimize_filter(
            filter,
            payload_provider,
            self.available_point_count(),
            hw_counter,
        )?;

        Ok(StructFilterContext::new(optimized_filter))
    }

    pub(in crate::index) fn condition_cardinality(
        &self,
        condition: &Condition,
        nested_path: Option<&JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        Ok(match condition {
            Condition::Filter(_) => panic!("Unexpected branching"),
            Condition::Nested(nested) => {
                // propagate complete nested path in case of multiple nested layers
                let full_path = JsonPath::extend_or_new(nested_path, &nested.array_key());
                self.estimate_nested_cardinality(nested.filter(), &full_path, hw_counter)?
            }
            Condition::IsEmpty(IsEmptyCondition { is_empty: field }) => {
                let available_points = self.available_point_count();
                let condition = FieldCondition::new_is_empty(field.key.clone(), true);

                self.estimate_field_condition(&condition, nested_path, hw_counter)?
                    .unwrap_or_else(|| CardinalityEstimation::unknown(available_points))
            }
            Condition::IsNull(IsNullCondition { is_null: field }) => {
                let available_points = self.available_point_count();
                let condition = FieldCondition::new_is_null(field.key.clone(), true);

                self.estimate_field_condition(&condition, nested_path, hw_counter)?
                    .unwrap_or_else(|| CardinalityEstimation::unknown(available_points))
            }
            Condition::HasId(has_id) => {
                let point_ids = has_id.has_id.clone();
                let resolved_point_offsets: Vec<PointOffsetType> = point_ids
                    .iter()
                    .filter_map(|external_id| self.id_tracker.internal_id(*external_id))
                    .collect();
                let num_ids = resolved_point_offsets.len();
                CardinalityEstimation {
                    primary_clauses: vec![PrimaryCondition::Ids(ResolvedHasId {
                        point_ids,
                        resolved_point_offsets,
                    })],
                    min: num_ids,
                    exp: num_ids,
                    max: num_ids,
                }
            }
            Condition::HasVector(has_vectors) => {
                if let Some(vector_storage) = self.vector_storages.get(&has_vectors.has_vector) {
                    let vector_storage = vector_storage.borrow();
                    let vectors = vector_storage.available_vector_count();
                    CardinalityEstimation::exact(vectors).with_primary_clause(
                        PrimaryCondition::HasVector(has_vectors.has_vector.clone()),
                    )
                } else {
                    CardinalityEstimation::exact(0)
                }
            }
            Condition::Field(field_condition) => self
                .estimate_field_condition(field_condition, nested_path, hw_counter)?
                .unwrap_or_else(|| CardinalityEstimation::unknown(self.available_point_count())),

            Condition::CustomIdChecker(cond) => {
                cond.0.estimate_cardinality(self.available_point_count())
            }
        })
    }
}
