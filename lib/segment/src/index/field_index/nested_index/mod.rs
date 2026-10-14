//! Per-element payload indexes for fields inside arrays of objects.
//!
//! A [`NestedFieldIndex`] indexes a field like `offers[].price` with every object of the
//! `offers` array as its own entry: the wrapped index stores values under element offsets
//! from its own [`ElementMap`] instead of under point offsets. That keeps track of which
//! values belong to the same element.
//!
//! Towards everything else the wrapper is a regular field index over points: every read
//! translates element offsets back to the points that own them, so results are the same
//! as those of the flattened index of the field.

mod builder;
mod element_map;
mod element_points;
mod facet;
mod numeric;
mod point_runs;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::bitvec::BitVec;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::types::{PointOffsetType, ScoredPointOffset};
use serde_json::Value;

pub use self::builder::NestedFieldIndexBuilder;
use self::element_map::nested_elements;
pub use self::element_map::{ElementMap, ElementMapBuilder, ElementOffsetType, PointElements};
pub use self::facet::{FacetView, NestedFacetView};
pub use self::numeric::{NestedNumericView, NumericView};
use super::numeric_index::NumericFieldIndexRead;
use super::stat_tools::estimate_multi_value_selection_cardinality;
use super::{
    CardinalityEstimation, FacetIndex, FieldIndex, FieldIndexRead, PayloadBlockCondition,
    PayloadFieldIndex, PayloadFieldIndexRead,
};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::data_types::query_context::{TextFieldStats, TextQueryContext};
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::full_text_index::full_text_index_read::{
    fill_text_statistics, score_bm25,
};
use crate::index::field_index::full_text_index::{Bm25Accept, Bm25Params, FullTextIndex};
use crate::index::payload_config::FullPayloadIndexType;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::json_path::JsonPath;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

/// A field index of the elements of a nested array, read as an index of points.
///
/// See the [module docs](self).
pub struct NestedFieldIndex {
    /// The array whose elements are indexed, as its array key, e.g. `offers[]`
    array_key: JsonPath,
    /// The indexed field inside an element, e.g. `price` for `offers[].price`
    relative_path: JsonPath,
    element_map: ElementMap,
    /// Index keyed by element offsets
    inner: FieldIndex,
    /// Number of points with at least one value in `inner`. For immutable indexes,
    /// an upper bound: opening restores the build-time count, ignoring prior deletions.
    indexed_points: usize,
}

/// A condition checked on the elements of one per-element index.
///
/// Element offsets belong to one index, so an element is checked through the
/// [`ElementMap`] of the index that answers the condition: the offset of a point's
/// element at a position is its place in [`ElementMap::point_elements`].
pub struct ElementConditionChecker<'a> {
    element_map: &'a ElementMap,
    checker: ConditionCheckerEnum<'a>,
}

impl<'a> ElementConditionChecker<'a> {
    pub fn element_map(&self) -> &'a ElementMap {
        self.element_map
    }

    /// Check the element at `element`, an offset from [`Self::element_map`].
    pub fn check(&self, element: ElementOffsetType) -> OperationResult<bool> {
        self.checker.check(element)
    }
}

impl NestedFieldIndex {
    /// Wrap `inner`, an index of `field` keyed by the element offsets of `element_map`.
    ///
    /// `indexed_points` is an upper bound on points with a value in `inner`, if available.
    /// Otherwise it is counted from `inner`, which reads all of it.
    pub fn new(
        field: &JsonPath,
        array_key: JsonPath,
        element_map: ElementMap,
        inner: FieldIndex,
        indexed_points: Option<usize>,
    ) -> OperationResult<Self> {
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

        index.indexed_points = match indexed_points {
            Some(indexed_points) => indexed_points,
            None => {
                let mut indexed_points = 0;
                for point in 0..index.element_map.points_upper_bound() as PointOffsetType {
                    if index.has_values(index.element_map.point_elements(point))? {
                        indexed_points += 1;
                    }
                }
                indexed_points
            }
        };

        Ok(index)
    }

    /// The array whose elements are indexed, as its array key.
    pub fn array_key(&self) -> &JsonPath {
        &self.array_key
    }

    pub fn element_map(&self) -> &ElementMap {
        &self.element_map
    }

    /// The wrapped index, keyed by element offsets.
    pub fn inner(&self) -> &FieldIndex {
        &self.inner
    }

    fn has_values(
        &self,
        elements: impl IntoIterator<Item = ElementOffsetType>,
    ) -> OperationResult<bool> {
        for element in elements {
            if !self.inner.values_is_empty(element)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Index the elements extracted at [`Self::array_key`] for `point`. Returns whether
    /// any element holds a value of the inner index's type.
    pub fn add_point(
        &mut self,
        point: PointOffsetType,
        values: &[&Value],
    ) -> OperationResult<bool> {
        let elements = nested_elements(values);
        let update = self.element_map.assign(point, elements.len())?;
        let had_values = self.has_values(update.old.iter().copied())?;

        for &element in update.dropped() {
            self.inner.remove_point(element)?;
        }
        for (&element, object) in update.new.iter().zip(elements) {
            let values = self.relative_path.value_get(object);
            if values.is_empty() {
                self.inner.remove_point(element)?;
            } else {
                self.inner.add_point(element, &values)?;
            }
        }

        let has_values = self.has_values(update.new.iter().copied())?;
        match (had_values, has_values) {
            (false, true) => self.indexed_points += 1,
            (true, false) => self.indexed_points -= 1,
            (false, false) | (true, true) => {}
        }
        Ok(has_values)
    }

    /// Remove `point` and the values of its elements.
    pub fn remove_point(&mut self, point: PointOffsetType) -> OperationResult<()> {
        let elements = self.element_map.point_elements(point);
        if self.has_values(elements.clone())? {
            self.indexed_points -= 1;
        }
        for element in elements {
            self.inner.remove_point(element)?;
        }
        self.element_map.assign(point, 0)?;
        Ok(())
    }

    pub fn ram_usage_bytes(&self) -> usize {
        self.inner.ram_usage_bytes() + self.element_map.ram_usage_bytes()
    }

    pub fn is_cold(&self) -> bool {
        self.inner.is_cold()
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.inner.populate()?;
        self.element_map.populate()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.inner.clear_cache()?;
        self.element_map.clear_cache()
    }

    pub fn get_full_index_type(&self) -> FullPayloadIndexType {
        self.inner.get_full_index_type()
    }

    /// Order-by view over points, if the inner index is numeric.
    pub fn numeric_view(&self) -> Option<NestedNumericView<'_>> {
        Some(NestedNumericView::new(
            self.inner.numeric_view()?,
            &self.element_map,
        ))
    }

    /// Facet view over points, if the inner index supports faceting.
    pub fn facet_view(&self) -> Option<NestedFacetView<'_>> {
        Some(NestedFacetView::new(
            self.inner.facet_view()?,
            &self.element_map,
        ))
    }

    fn text_index(&self) -> Option<&FullTextIndex> {
        match &self.inner {
            FieldIndex::FullTextIndex(index) => Some(index),
            FieldIndex::IntIndex(_)
            | FieldIndex::DatetimeIndex(_)
            | FieldIndex::IntMapIndex(_)
            | FieldIndex::KeywordIndex(_)
            | FieldIndex::FloatIndex(_)
            | FieldIndex::GeoIndex(_)
            | FieldIndex::BoolIndex(_)
            | FieldIndex::UuidIndex(_)
            | FieldIndex::UuidMapIndex(_)
            | FieldIndex::NullIndex(_)
            | FieldIndex::Nested(_) => None,
        }
    }

    /// Whether the flattened index of the field would keep each value once per point.
    fn dedups_values(&self) -> bool {
        match &self.inner {
            FieldIndex::IntMapIndex(_)
            | FieldIndex::KeywordIndex(_)
            | FieldIndex::UuidMapIndex(_)
            | FieldIndex::BoolIndex(_) => true,
            FieldIndex::IntIndex(_)
            | FieldIndex::DatetimeIndex(_)
            | FieldIndex::FloatIndex(_)
            | FieldIndex::GeoIndex(_)
            | FieldIndex::FullTextIndex(_)
            | FieldIndex::UuidIndex(_)
            | FieldIndex::NullIndex(_)
            | FieldIndex::Nested(_) => false,
        }
    }

    /// Convert an estimation over elements into one over points.
    fn to_points_estimation(
        &self,
        estimation: CardinalityEstimation,
        map: &ElementMap,
    ) -> OperationResult<CardinalityEstimation> {
        let CardinalityEstimation {
            primary_clauses,
            min,
            exp,
            max,
        } = estimation;

        // The elements the inner index can match, and an upper bound on the points owning
        // them. A value index matches elements with values, the null index any element.
        let (elements, points) = match &self.inner {
            FieldIndex::IntIndex(_)
            | FieldIndex::DatetimeIndex(_)
            | FieldIndex::IntMapIndex(_)
            | FieldIndex::KeywordIndex(_)
            | FieldIndex::FloatIndex(_)
            | FieldIndex::GeoIndex(_)
            | FieldIndex::FullTextIndex(_)
            | FieldIndex::BoolIndex(_)
            | FieldIndex::UuidIndex(_)
            | FieldIndex::UuidMapIndex(_)
            | FieldIndex::Nested(_) => {
                // The inner index is keyed by elements, so it counts elements with values
                let elements = self.inner.count_indexed_points()?;
                // `indexed_points` of an immutable index ignores deletions, so it can
                // exceed the elements still holding values, which the estimator requires
                // to bound it
                (elements, self.indexed_points.min(elements))
            }
            FieldIndex::NullIndex(_) => {
                let elements = map.elements_count();
                (elements, map.points_upper_bound().min(elements))
            }
        };
        let max = max.min(points);
        let min = min.div_ceil(map.max_elements_per_point()).min(max);
        let exp = if points == 0 {
            0
        } else {
            // Assume matching elements spread randomly among points
            estimate_multi_value_selection_cardinality(points, elements, exp.min(elements)).round()
                as usize
        };

        Ok(CardinalityEstimation {
            // Field conditions resolve through this index, so they select points too
            primary_clauses,
            min,
            exp: exp.clamp(min, max),
            max,
        })
    }
}

/// Matches a point if any of its elements matches the condition of the inner index.
struct NestedConditionChecker<'a> {
    inner: ConditionCheckerEnum<'a>,
    element_map: &'a ElementMap,
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
        let map = &self.element_map;
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
        Ok(Some(
            self.to_points_estimation(estimation, &self.element_map)?,
        ))
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // Inner blocks count elements. The flattened index of a deduplicating type counts
        // each value once per point, so its blocks are estimated in points, and the ones
        // that would fall below `threshold` are skipped. The estimate never exceeds the
        // element count, so the inner index can apply `threshold` to elements as is.
        let dedups_values = self.dedups_values();
        let points = self.indexed_points;
        self.inner
            .for_each_payload_block(threshold, key, &mut |block| {
                let PayloadBlockCondition {
                    condition,
                    cardinality,
                } = block;
                let cardinality = if dedups_values {
                    let estimation = CardinalityEstimation::exact(cardinality);
                    self.to_points_estimation(estimation, &self.element_map)?
                        .exp
                } else {
                    // A block has at most as many points as elements
                    cardinality.min(points)
                };
                if cardinality < threshold {
                    return Ok(());
                }
                f(PayloadBlockCondition {
                    condition,
                    cardinality,
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
                element_map: &self.element_map,
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

impl PayloadFieldIndex for NestedFieldIndex {
    fn wipe(self) -> OperationResult<()> {
        let Self {
            array_key: _,
            relative_path: _,
            element_map,
            inner,
            indexed_points: _,
        } = self;
        inner.wipe()?;
        element_map.wipe()
    }

    fn flusher(&self) -> Flusher {
        let inner_flusher = self.inner.flusher();
        let map_flusher = self.element_map.flusher();
        Box::new(move || {
            // Map last: an element offset that the index knows but the map doesn't is
            // never resolved, see `ElementMap`
            inner_flusher()?;
            map_flusher()
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.inner.files();
        files.extend(self.element_map.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.inner.immutable_files();
        files.extend(self.element_map.immutable_files());
        files
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
        let elements = self.element_map.point_elements(point_id);
        let mut count = 0;
        for element in elements {
            count += self.inner.values_count(element)?;
        }
        Ok(count)
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        let elements = self.element_map.point_elements(point_id);
        Ok(!self.has_values(elements)?)
    }

    fn value_retriever<'a, 'q>(&'a self) -> OperationResult<Option<VariableRetrieverFn<'q>>>
    where
        'a: 'q,
    {
        let Some(inner) = self.inner.value_retriever()? else {
            return Ok(None);
        };
        let map = &self.element_map;
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

    /// Statistics over elements: every element is a document of its own.
    fn fill_text_statistics(
        &self,
        stats: &mut TextFieldStats,
        corpus: Option<&[PointOffsetType]>,
        is_stopped: &AtomicBool,
    ) -> OperationResult<bool> {
        let Some(index) = self.text_index() else {
            return Ok(false);
        };
        let corpus_elements = corpus.map(|points| {
            let map = &self.element_map;
            points
                .iter()
                .flat_map(|&point| map.point_elements(point))
                .collect::<Vec<ElementOffsetType>>()
        });
        fill_text_statistics(index, stats, corpus_elements.as_deref(), is_stopped)?;
        Ok(true)
    }

    /// Scores every element as a document; a point scores as its best element.
    fn score_bm25(
        &self,
        terms: &[String],
        context: &TextQueryContext<'_>,
        params: Bm25Params,
        accept: &Bm25Accept<'_>,
        limit: usize,
    ) -> OperationResult<Option<Vec<ScoredPointOffset>>> {
        let Some(index) = self.text_index() else {
            return Ok(None);
        };
        let map = &self.element_map;
        let element_point = |element: ElementOffsetType| map.element_point(element);
        let visible = |element: ElementOffsetType| {
            map.element_point(element)
                .is_some_and(|point| accept.is_visible(point))
        };
        let element_accept = accept.for_elements(&visible, &element_point);

        // Every point above the `limit`-th best point has at most
        // `max_elements_per_point` elements, so the best element of each of the
        // `limit` best points is among this many best elements.
        let elements_limit = limit
            .saturating_mul(map.max_elements_per_point())
            .min(map.elements_count());
        let scored_elements = score_bm25(
            index,
            terms,
            context,
            params,
            &element_accept,
            elements_limit,
            context.is_stopped(),
        )?;

        // Highest first, so the first element of a point is its best
        let mut seen = BitVec::repeat(false, map.points_upper_bound());
        let scored_points = scored_elements
            .into_iter()
            .filter_map(|ScoredPointOffset { idx, score }| {
                let point = map.element_point(idx)?;
                (!seen.replace(point as usize, true))
                    .then_some(ScoredPointOffset { idx: point, score })
            })
            .take(limit)
            .collect();
        Ok(Some(scored_points))
    }

    fn as_facet_index(&self) -> Option<impl FacetIndex + '_> {
        self.facet_view()
    }

    fn element_map(&self, array_key: &JsonPath) -> Option<&ElementMap> {
        (&self.array_key == array_key).then_some(&self.element_map)
    }

    fn element_condition_checker<'a>(
        &'a self,
        array_key: &JsonPath,
        condition: &FieldCondition,
    ) -> OperationResult<Option<ElementConditionChecker<'a>>> {
        if &self.array_key != array_key {
            return Ok(None);
        }
        // The inner index is keyed by element offsets
        let Some(checker) = self.inner.condition_checker(condition)? else {
            return Ok(None);
        };
        Ok(Some(ElementConditionChecker {
            element_map: &self.element_map,
            checker,
        }))
    }
}
