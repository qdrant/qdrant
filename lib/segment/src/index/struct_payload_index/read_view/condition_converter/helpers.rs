use common::counter::hardware_accumulator::HwMeasurementAcc;

use super::match_converter::get_match_checkers;
use crate::index::field_index::{FieldIndex, FieldIndexRead};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::FieldCondition;

pub(super) fn field_condition_index<'a>(
    index: &'a FieldIndex,
    field_condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'a>> {
    // Try the polymorphic dispatch first. As variants migrate their
    // condition handling into `PayloadFieldIndexRead::condition_checker`,
    // this short-circuits more cases. The legacy match below shrinks
    // until it can be deleted entirely (see
    // docs/plans/field-index-read-trait/condition-checker-migration/).
    if let Some(checker) = index.condition_checker(field_condition, hw_acc.clone()) {
        return Some(checker);
    }

    match field_condition {
        FieldCondition {
            r#match: Some(cond_match),
            ..
        } => get_match_checkers(index, cond_match.clone(), hw_acc),

        FieldCondition {
            key: _,
            r#match: None,
            // Range / geo / is_empty / is_null conditions are served via
            // `condition_checker` on the relevant typed indexes
            // (NumericIndex / GeoMapIndex / NullIndex). If we got here,
            // no condition_checker fired for this field's indexes —
            // returning `None` falls back to the payload check.
            range: _,
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            // We can't use index for this condition, since some indices don't count values,
            // like boolean index, where [true, true, true] is the same as [true]. Count should be 3 but they think is 1.
            //
            // TODO: Try to use the indices that actually support counting values.
            values_count: _,
            is_empty: _,
            is_null: _,
        } => None,
    }
}
