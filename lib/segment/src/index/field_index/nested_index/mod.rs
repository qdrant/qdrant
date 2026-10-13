//! Per-element payload indexes for fields inside arrays of objects.
//!
//! A [`NestedFieldIndex`] indexes a field like `offers[].price` with every object of the
//! `offers` array as its own entry: the wrapped index stores values under element offsets
//! from the shared [`ElementMap`] of the array instead of under point offsets. That keeps
//! track of which values belong to the same element.
//!
//! Towards everything else the wrapper is a regular field index over points: every read
//! translates element offsets back to the points that own them, so results are the same
//! as those of the flattened index of the field.

mod element_map;
mod facet;
mod numeric;
#[cfg(test)]
mod tests;

use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::{AtomicRef, AtomicRefCell};
use common::bitvec::BitVec;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::types::{PointOffsetType, ScoredPointOffset};
use serde_json::{Map, Value};

pub use self::element_map::{ElementMap, ElementOffsetType, ElementUpdate, nested_elements};
pub use self::facet::{FacetView, NestedFacetView};
pub use self::numeric::{NestedNumericView, NumericView};
use super::numeric_index::NumericFieldIndexRead;
use super::{
    CardinalityEstimation, FacetIndex, FieldIndex, FieldIndexRead, PayloadBlockCondition,
    PayloadFieldIndex, PayloadFieldIndexRead,
};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::data_types::query_context::{TextFieldStats, TextQueryContext};
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::full_text_index::Bm25Params;
use crate::index::payload_config::FullPayloadIndexType;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::json_path::JsonPath;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

/// The element map of one nested array, shared by all its per-element indexes.
pub type SharedElementMap = Arc<AtomicRefCell<ElementMap>>;

/// A field index of the elements of a nested array, read as an index of points.
///
/// See the [module docs](self).
pub struct NestedFieldIndex {
    /// The array whose elements are indexed, as its array key, e.g. `offers[]`
    array_key: JsonPath,
    /// The indexed field inside an element, e.g. `price` for `offers[].price`
    relative_path: JsonPath,
    element_map: SharedElementMap,
    /// Index keyed by element offsets
    inner: FieldIndex,
    /// Number of points with at least one value in `inner`
    indexed_points: usize,
}

impl NestedFieldIndex {
    /// Wrap `inner`, an index of `field` keyed by the element offsets of `element_map`.
    pub fn new(
        field: &JsonPath,
        array_key: JsonPath,
        element_map: SharedElementMap,
        inner: FieldIndex,
    ) -> OperationResult<Self> {
        debug_assert!(!matches!(
            inner,
            FieldIndex::Nested(_) | FieldIndex::NullIndex(_)
        ));
        let relative_path = field.strip_prefix(&array_key).ok_or_else(|| {
            OperationError::service_error(format!(
                "field `{field}` is not inside the elements of `{array_key}`",
            ))
        })?;

        let mut index = Self {
            array_key,
            relative_path,
            element_map,
            inner,
            indexed_points: 0,
        };

        let map = index.element_map.borrow();
        let mut indexed_points = 0;
        for point in 0..map.points_upper_bound() as PointOffsetType {
            if index.has_values(map.point_elements(point))? {
                indexed_points += 1;
            }
        }
        drop(map);
        index.indexed_points = indexed_points;

        Ok(index)
    }

    /// The array whose elements are indexed, as its array key.
    pub fn array_key(&self) -> &JsonPath {
        &self.array_key
    }

    pub fn element_map(&self) -> &SharedElementMap {
        &self.element_map
    }

    /// The wrapped index, keyed by element offsets.
    pub fn inner(&self) -> &FieldIndex {
        &self.inner
    }

    fn has_values(&self, elements: Range<ElementOffsetType>) -> OperationResult<bool> {
        for element in elements {
            if !self.inner.values_is_empty(element)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Index the `elements` of `point`, after the element map moved it from
    /// `update.old` to `update.new`.
    pub fn update_point(
        &mut self,
        update: &ElementUpdate,
        elements: &[&Map<String, Value>],
    ) -> OperationResult<()> {
        debug_assert_eq!(update.new.len(), elements.len());
        let had_values = self.has_values(update.old.clone())?;

        for element in update.old.clone() {
            if !update.new.contains(&element) {
                self.inner.remove_point(element)?;
            }
        }
        for (element, object) in update.new.clone().zip(elements) {
            let values = self.relative_path.value_get(object);
            if values.is_empty() {
                self.inner.remove_point(element)?;
            } else {
                self.inner.add_point(element, &values)?;
            }
        }

        let has_values = self.has_values(update.new.clone())?;
        match (had_values, has_values) {
            (false, true) => self.indexed_points += 1,
            (true, false) => self.indexed_points -= 1,
            (false, false) | (true, true) => {}
        }
        Ok(())
    }

    /// Remove the values of all current elements of `point`.
    ///
    /// The element map is not changed: the caller updates it.
    pub fn remove_point(&mut self, point: PointOffsetType) -> OperationResult<()> {
        let elements = self.element_map.borrow().point_elements(point);
        if self.has_values(elements.clone())? {
            self.indexed_points -= 1;
        }
        for element in elements {
            self.inner.remove_point(element)?;
        }
        Ok(())
    }

    pub fn ram_usage_bytes(&self) -> usize {
        self.inner.ram_usage_bytes()
    }

    pub fn is_cold(&self) -> bool {
        self.inner.is_cold()
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.inner.populate()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.inner.clear_cache()
    }

    pub fn get_full_index_type(&self) -> FullPayloadIndexType {
        self.inner.get_full_index_type()
    }

    /// Order-by view over points, if the inner index is numeric.
    pub fn numeric_view(&self) -> Option<NestedNumericView<'_>> {
        Some(NestedNumericView::new(
            self.inner.numeric_view()?,
            self.element_map.borrow(),
        ))
    }

    /// Facet view over points, if the inner index supports faceting.
    pub fn facet_view(&self) -> Option<NestedFacetView<'_>> {
        Some(NestedFacetView::new(
            self.inner.facet_view()?,
            self.element_map.borrow(),
        ))
    }

    /// Whether the flattened index of the field would keep each value once per point.
    fn dedups_values(&self) -> bool {
        matches!(
            self.inner,
            FieldIndex::IntMapIndex(_)
                | FieldIndex::KeywordIndex(_)
                | FieldIndex::UuidMapIndex(_)
                | FieldIndex::BoolIndex(_)
        )
    }

    /// Convert an estimation over elements into one over points.
    fn to_points_estimation(
        &self,
        estimation: CardinalityEstimation,
        map: &ElementMap,
    ) -> CardinalityEstimation {
        let CardinalityEstimation {
            primary_clauses,
            min,
            exp,
            max,
        } = estimation;

        let points = self.indexed_points;
        let elements = map.elements_count().max(1);
        let max = max.min(points);
        let min = min.div_ceil(map.max_elements_per_point()).min(max);
        // Assume matching elements spread like all elements
        let exp = ((exp as u128 * points as u128) / elements as u128) as usize;

        CardinalityEstimation {
            // Field conditions resolve through this index, so they select points too
            primary_clauses,
            min,
            exp: exp.clamp(min, max),
            max,
        }
    }
}

/// Matches a point if any of its elements matches the condition of the inner index.
struct NestedConditionChecker<'a> {
    inner: ConditionCheckerEnum<'a>,
    element_map: AtomicRef<'a, ElementMap>,
}

impl ConditionChecker for NestedConditionChecker<'_> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        for element in self.element_map.point_elements(point_id) {
            if self.inner.check(element)? {
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

impl PayloadFieldIndexRead for NestedFieldIndex {
    fn count_indexed_points(&self) -> OperationResult<usize> {
        Ok(self.indexed_points)
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        let Some(elements) = self.inner.filter(condition)? else {
            return Ok(None);
        };
        let map = self.element_map.borrow();
        let mut seen = BitVec::repeat(false, map.points_upper_bound());
        Ok(Some(Box::new(elements.filter_map(move |element| {
            let point = map.element_point(element)?;
            (!seen.replace(point as usize, true)).then_some(point)
        }))))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        let Some(estimation) = self.inner.estimate_cardinality(condition)? else {
            return Ok(None);
        };
        let map = self.element_map.borrow();
        Ok(Some(self.to_points_estimation(estimation, &map)))
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // A block has at most as many points as elements
        let points = self.indexed_points;
        self.inner
            .for_each_payload_block(threshold, key, &mut |block| {
                f(PayloadBlockCondition {
                    condition: block.condition,
                    cardinality: block.cardinality.min(points),
                })
            })
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        let Some(inner) = self.inner.condition_checker(condition)? else {
            return Ok(None);
        };
        Ok(Some(ConditionCheckerEnum::Dyn(Box::new(
            NestedConditionChecker {
                inner,
                element_map: self.element_map.borrow(),
            },
        ))))
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
    ) -> OperationResult<Option<bool>> {
        self.inner.special_check_condition(condition, payload_value)
    }
}

/// The element map is not part of the index: [`StructPayloadIndex`] persists and wipes
/// it, as it is shared with the other per-element indexes of the array.
///
/// [`StructPayloadIndex`]: crate::index::struct_payload_index::StructPayloadIndex
impl PayloadFieldIndex for NestedFieldIndex {
    fn wipe(self) -> OperationResult<()> {
        self.inner.wipe()
    }

    fn flusher(&self) -> Flusher {
        self.inner.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        self.inner.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.inner.immutable_files()
    }
}

impl FieldIndexRead for NestedFieldIndex {
    fn get_telemetry_data(&self) -> OperationResult<PayloadIndexTelemetry> {
        let telemetry = self.inner.get_telemetry_data()?;
        Ok(PayloadIndexTelemetry {
            points_count: self.indexed_points,
            ..telemetry
        })
    }

    fn values_count(&self, point_id: PointOffsetType) -> OperationResult<usize> {
        let elements = self.element_map.borrow().point_elements(point_id);
        let mut count = 0;
        for element in elements {
            count += self.inner.values_count(element)?;
        }
        Ok(count)
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        let elements = self.element_map.borrow().point_elements(point_id);
        Ok(!self.has_values(elements)?)
    }

    fn value_retriever<'a, 'q>(&'a self) -> OperationResult<Option<VariableRetrieverFn<'q>>>
    where
        'a: 'q,
    {
        let Some(inner) = self.inner.value_retriever()? else {
            return Ok(None);
        };
        let map = self.element_map.borrow();
        let dedup = self.dedups_values();
        Ok(Some(Box::new(move |point_id| {
            let mut values = MultiValue::new();
            for element in map.point_elements(point_id) {
                for value in inner(element) {
                    if !dedup || !values.contains(&value) {
                        values.push(value);
                    }
                }
            }
            values
        })))
    }

    fn as_numeric(&self) -> Option<impl NumericFieldIndexRead + '_> {
        self.numeric_view()
    }

    fn fill_text_statistics(
        &self,
        _stats: &mut TextFieldStats,
        _is_stopped: &AtomicBool,
    ) -> OperationResult<bool> {
        // Full-text indexes are not supported per element
        Ok(false)
    }

    fn score_bm25(
        &self,
        _terms: &[String],
        _context: &TextQueryContext<'_>,
        _params: Bm25Params,
        _accept: &dyn Fn(PointOffsetType) -> bool,
        _limit: usize,
    ) -> OperationResult<Option<Vec<ScoredPointOffset>>> {
        // Full-text indexes are not supported per element
        Ok(None)
    }

    fn as_facet_index(&self) -> Option<impl FacetIndex + '_> {
        self.facet_view()
    }
}
