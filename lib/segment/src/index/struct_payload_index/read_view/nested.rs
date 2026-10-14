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
//! [`ElementMap`](crate::index::field_index::nested_index::ElementMap). So the element at
//! a position is checked in each index through that index's offset at the same position.

use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::types::PointOffsetType;
use smallvec::SmallVec;

use super::StructPayloadIndexReadView;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::IdTrackerRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::FieldIndexRead;
use crate::index::field_index::nested_index::{ElementConditionChecker, ElementMap, PointElements};
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::types::{Condition, FieldCondition, Filter, MinShould, NestedCondition};
use crate::vector_storage::VectorStorageRead;

/// A nested filter over element positions.
///
/// Mirrors [`check_filter`](crate::payload_storage::query_checker::check_filter), which
/// evaluates it over the payload of an element.
struct ElementFilter {
    should: Option<Vec<ElementCondition>>,
    min_should: Option<(Vec<ElementCondition>, usize)>,
    must: Option<Vec<ElementCondition>>,
    must_not: Option<Vec<ElementCondition>>,
}

enum ElementCondition {
    /// A field condition, by its index in [`NestedElementChecker::fields`]
    Field(usize),
    Filter(ElementFilter),
}

/// Element offsets of one point in every field index, in array order.
///
/// Borrowed from the maps: a range or a slice, so an offset is found by position
/// without collecting them.
type PointOffsets<'a> = SmallVec<[PointElements<'a>; 4]>;

/// Checks the conditions of one element, given the offsets of its point.
struct ElementContext<'c, 'a> {
    fields: &'c [ElementConditionChecker<'a>],
    offsets: &'c PointOffsets<'a>,
    position: usize,
}

impl ElementContext<'_, '_> {
    fn check_field(&self, field: usize) -> OperationResult<bool> {
        match self.offsets[field].clone().nth(self.position) {
            Some(element) => self.fields[field].check(element),
            // All maps of an array agree on the elements, so this is not expected
            None => Ok(false),
        }
    }

    fn check(&self, condition: &ElementCondition) -> OperationResult<bool> {
        match condition {
            ElementCondition::Field(field) => self.check_field(*field),
            ElementCondition::Filter(filter) => self.check_filter(filter),
        }
    }

    fn any(&self, conditions: &[ElementCondition]) -> OperationResult<bool> {
        for condition in conditions {
            if self.check(condition)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn check_filter(&self, filter: &ElementFilter) -> OperationResult<bool> {
        let ElementFilter {
            should,
            min_should,
            must,
            must_not,
        } = filter;

        if let Some(should) = should
            && !self.any(should)?
        {
            return Ok(false);
        }

        if let Some((conditions, min_count)) = min_should {
            let mut matched = 0;
            for condition in conditions {
                if matched == *min_count {
                    break;
                }
                if self.check(condition)? {
                    matched += 1;
                }
            }
            if matched < *min_count {
                return Ok(false);
            }
        }

        if let Some(must) = must {
            for condition in must {
                if !self.check(condition)? {
                    return Ok(false);
                }
            }
        }

        if let Some(must_not) = must_not
            && self.any(must_not)?
        {
            return Ok(false);
        }

        Ok(true)
    }
}

/// Matches a point if any of its elements matches the nested filter.
struct NestedElementChecker<'a> {
    /// Element map of any per-element index of the array, to count the elements of a point
    element_map: &'a ElementMap,
    fields: Vec<ElementConditionChecker<'a>>,
    filter: ElementFilter,
}

impl ConditionChecker for NestedElementChecker<'_> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        let elements = self.element_map.point_elements(point_id).len();
        if elements == 0 {
            return Ok(false);
        }

        let offsets: PointOffsets = self
            .fields
            .iter()
            .map(|field| field.element_map().point_elements(point_id))
            .collect();

        for position in 0..elements {
            let context = ElementContext {
                fields: &self.fields,
                offsets: &offsets,
                position,
            };
            if context.check_filter(&self.filter)? {
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

        // Any per-element index of the array counts its elements
        let Some(element_map) = self
            .field_indexes
            .values()
            .flatten()
            .find_map(|index| index.element_map(&array_key))
        else {
            return Ok(None);
        };

        let mut fields = Vec::new();
        let Some(filter) = self.element_filter(&array_key, nested.filter(), &mut fields)? else {
            return Ok(None);
        };

        Ok(Some(ConditionCheckerEnum::Dyn(Box::new(
            NestedElementChecker {
                element_map,
                fields,
                filter,
            },
        ))))
    }

    /// `None` if any condition of `filter` is not covered.
    fn element_filter<'b>(
        &'b self,
        array_key: &JsonPath,
        filter: &Filter,
        fields: &mut Vec<ElementConditionChecker<'b>>,
    ) -> OperationResult<Option<ElementFilter>> {
        let Filter {
            should,
            min_should,
            must,
            must_not,
        } = filter;

        // `Ok(None)` if the conditions are absent, `None` if they are not covered
        let convert = |conditions: &Option<Vec<Condition>>,
                       fields: &mut Vec<ElementConditionChecker<'b>>|
         -> OperationResult<Option<Option<Vec<ElementCondition>>>> {
            match conditions {
                None => Ok(Some(None)),
                Some(conditions) => Ok(self
                    .element_conditions(array_key, conditions, fields)?
                    .map(Some)),
            }
        };

        let Some(should) = convert(should, fields)? else {
            return Ok(None);
        };
        let Some(must) = convert(must, fields)? else {
            return Ok(None);
        };
        let Some(must_not) = convert(must_not, fields)? else {
            return Ok(None);
        };
        let min_should = match min_should {
            Some(MinShould {
                conditions,
                min_count,
            }) => match self.element_conditions(array_key, conditions, fields)? {
                Some(conditions) => Some((conditions, *min_count)),
                None => return Ok(None),
            },
            None => None,
        };

        Ok(Some(ElementFilter {
            should,
            min_should,
            must,
            must_not,
        }))
    }

    /// `Some` with all `conditions` converted, `None` if any of them is not covered.
    fn element_conditions<'b>(
        &'b self,
        array_key: &JsonPath,
        conditions: &[Condition],
        fields: &mut Vec<ElementConditionChecker<'b>>,
    ) -> OperationResult<Option<Vec<ElementCondition>>> {
        let mut converted = Vec::with_capacity(conditions.len());
        for condition in conditions {
            match self.element_condition(array_key, condition, fields)? {
                Some(condition) => converted.push(condition),
                None => return Ok(None),
            }
        }
        Ok(Some(converted))
    }

    fn element_condition<'b>(
        &'b self,
        array_key: &JsonPath,
        condition: &Condition,
        fields: &mut Vec<ElementConditionChecker<'b>>,
    ) -> OperationResult<Option<ElementCondition>> {
        match condition {
            Condition::Field(condition) => {
                let Some(checker) = self.element_field_checker(array_key, condition)? else {
                    return Ok(None);
                };
                fields.push(checker);
                Ok(Some(ElementCondition::Field(fields.len() - 1)))
            }
            Condition::Filter(filter) => Ok(self
                .element_filter(array_key, filter, fields)?
                .map(ElementCondition::Filter)),
            // Answered from payload: emptiness is tracked per point, not per element, and
            // ids, vectors and deeper nesting have no per-element index
            Condition::IsEmpty(_)
            | Condition::IsNull(_)
            | Condition::HasId(_)
            | Condition::HasVector(_)
            | Condition::Slice(_)
            | Condition::Nested(_)
            | Condition::CustomIdChecker(_) => Ok(None),
        }
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
