//! `Nested` filter conditions answered from per-element indexes, without reading payload.
//!
//! A nested filter is evaluated per element of the array: an element matches if it
//! matches the filter, a point matches if any of its elements does. When every condition
//! of the filter is served by a per-element index of the array, the filter is evaluated
//! with those indexes. Otherwise the caller falls back to checking each element in the
//! payload.
//!
//! Every per-element index has its own element offsets, but all of them agree on the
//! elements of a point and their order, see
//! [`ElementMap`](crate::index::field_index::nested_index::ElementMap). So the filter is
//! an [`OptimizedFilter`] over the element offsets of one of them, and every condition
//! translates an element to the offset of the same position in its own index.

use common::condition_checker::{
    CheckItem, ConditionChecker, ConstantConditionChecker, Rest, Select, default_check_batched,
};
use common::types::PointOffsetType;

use super::StructPayloadIndexReadView;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::IdTrackerRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::FieldIndexRead;
use crate::index::field_index::nested_index::{ElementConditionChecker, ElementMap};
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::types::{Condition, FieldCondition, Filter, MinShould, NestedCondition};
use crate::vector_storage::VectorStorageRead;

/// Matches a point if any of its elements matches the nested filter.
struct NestedElementChecker<'a> {
    /// Element map whose offsets `filter` is checked with, see
    /// [`ElementConditionChecker::with_anchor`]
    anchor: &'a ElementMap,
    filter: OptimizedFilter<'a>,
}

impl ConditionChecker for NestedElementChecker<'_> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        for element in self.anchor.point_elements(point_id) {
            if self.filter.check(element)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn check_batched<K: CheckItem>(
        &self,
        items: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        default_check_batched(items, select, rest, |point_id| self.check(point_id))
    }
}

impl<'a, P, I, V, F> StructPayloadIndexReadView<'a, P, I, V, F>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
    F: FieldIndexRead,
{
    /// Checker of a `Nested` condition from the per-element indexes of its array.
    ///
    /// `None` if some part of the nested filter is not served by them; the caller then
    /// checks the payload, see the [module docs](self).
    pub(crate) fn nested_element_checker<'b>(
        &'b self,
        nested: &NestedCondition,
    ) -> OperationResult<Option<ConditionCheckerEnum<'b>>> {
        let array_key = nested.array_key();
        let mut anchor = None;
        let Some(filter) = self.element_filter(&array_key, nested.filter(), &mut anchor)? else {
            return Ok(None);
        };
        // Without field conditions there is no index to list the elements of a point
        let Some(anchor) = anchor else {
            return Ok(None);
        };
        Ok(Some(ConditionCheckerEnum::Dyn(Box::new(
            NestedElementChecker { anchor, filter },
        ))))
    }

    /// `filter` over element offsets of `anchor`, which is set to the map of the first
    /// field condition. `None` if any condition of `filter` is not covered.
    fn element_filter<'b>(
        &'b self,
        array_key: &JsonPath,
        filter: &Filter,
        anchor: &mut Option<&'b ElementMap>,
    ) -> OperationResult<Option<OptimizedFilter<'b>>> {
        let Filter {
            should,
            min_should,
            must,
            must_not,
        } = filter;
        let (min_should, min_should_count) = match min_should {
            Some(MinShould {
                conditions,
                min_count,
            }) => (conditions.as_slice(), *min_count),
            None => (&[][..], 0),
        };

        let mut convert = |conditions: Option<&[Condition]>| {
            self.element_conditions(array_key, conditions.unwrap_or_default(), anchor)
        };
        let Some(mut should) = convert(should.as_deref())? else {
            return Ok(None);
        };
        // An empty `should` matches no element, while `OptimizedFilter` skips it
        if should.is_empty() && filter.should.is_some() {
            should.push(ConditionCheckerEnum::Constant(
                ConstantConditionChecker::MATCH_NONE,
            ));
        }
        let Some(min_should) = convert(Some(min_should))? else {
            return Ok(None);
        };
        let Some(must) = convert(must.as_deref())? else {
            return Ok(None);
        };
        let Some(must_not) = convert(must_not.as_deref())? else {
            return Ok(None);
        };

        Ok(Some(OptimizedFilter::new(
            should,
            min_should,
            min_should_count,
            must,
            must_not,
        )))
    }

    /// `Some` with all `conditions` converted, `None` if any of them is not covered.
    fn element_conditions<'b>(
        &'b self,
        array_key: &JsonPath,
        conditions: &[Condition],
        anchor: &mut Option<&'b ElementMap>,
    ) -> OperationResult<Option<Vec<ConditionCheckerEnum<'b>>>> {
        let mut converted = Vec::with_capacity(conditions.len());
        for condition in conditions {
            let checker = match condition {
                Condition::Field(condition) => {
                    let Some(checker) = self.element_field_checker(array_key, condition)? else {
                        return Ok(None);
                    };
                    let anchor = *anchor.get_or_insert(checker.element_map());
                    ConditionCheckerEnum::Dyn(Box::new(checker.with_anchor(anchor)))
                }
                Condition::Filter(filter) => {
                    let Some(filter) = self.element_filter(array_key, filter, anchor)? else {
                        return Ok(None);
                    };
                    ConditionCheckerEnum::Filter(filter)
                }
                // Answered from payload: emptiness is tracked per point, not per element,
                // and ids, vectors and deeper nesting have no per-element index
                Condition::IsEmpty(_)
                | Condition::IsNull(_)
                | Condition::HasId(_)
                | Condition::HasVector(_)
                | Condition::Slice(_)
                | Condition::Nested(_)
                | Condition::CustomIdChecker(_) => return Ok(None),
            };
            converted.push(checker);
        }
        Ok(Some(converted))
    }

    fn element_field_checker<'b>(
        &'b self,
        array_key: &JsonPath,
        condition: &FieldCondition,
    ) -> OperationResult<Option<ElementConditionChecker<'b>>> {
        let FieldCondition {
            key,
            r#match: _,
            range: _,
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            values_count,
            is_empty,
            is_null,
        } = condition;

        // Indexes don't answer these: they also match elements without values
        if values_count.is_some() || is_empty.is_some() || is_null.is_some() {
            return Ok(None);
        }

        let full_key = array_key.extend(key);
        let Some(indexes) = self.field_indexes.get(&full_key) else {
            return Ok(None);
        };
        let full_condition = FieldCondition {
            key: full_key,
            ..condition.clone()
        };
        for index in indexes {
            if let Some(checker) = index.element_condition_checker(array_key, &full_condition)? {
                return Ok(Some(checker));
            }
        }
        Ok(None)
    }
}
