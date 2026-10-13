use std::borrow::Cow;

use atomic_refcell::AtomicRef;
use common::types::PointOffsetType;

use super::ElementMap;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetValue, FacetValueRef};
use crate::index::field_index::facet_index::{FacetIndex, FacetIndexEnum};

/// Facet view of a per-element index, over points.
///
/// Like the flattened index, it counts a point once per value, however many of its
/// elements have that value.
pub struct NestedFacetView<'a> {
    inner: FacetIndexEnum<'a>,
    element_map: AtomicRef<'a, ElementMap>,
}

impl<'a> NestedFacetView<'a> {
    pub fn new(inner: FacetIndexEnum<'a>, element_map: AtomicRef<'a, ElementMap>) -> Self {
        Self { inner, element_map }
    }

    /// Sorted unique points owning `elements`.
    fn to_points(
        &self,
        elements: &mut dyn Iterator<Item = PointOffsetType>,
    ) -> Vec<PointOffsetType> {
        let mut points: Vec<_> = elements
            .filter_map(|element| self.element_map.element_point(element))
            .collect();
        points.sort_unstable();
        points.dedup();
        points
    }
}

fn facet_value_ref(value: &FacetValue) -> FacetValueRef<'_> {
    match value {
        FacetValue::Keyword(keyword) => FacetValueRef::Keyword(Cow::Borrowed(keyword)),
        FacetValue::Int(int) => FacetValueRef::Int(*int),
        FacetValue::Uuid(uuid) => FacetValueRef::Uuid(*uuid),
        FacetValue::Bool(bool) => FacetValueRef::Bool(*bool),
    }
}

impl FacetIndex for NestedFacetView<'_> {
    fn unique_values_count(&self) -> usize {
        self.inner.unique_values_count()
    }

    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        mut f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        let mut values: Vec<FacetValue> = Vec::new();
        for point in points {
            values.clear();
            self.inner.for_points_values(
                self.element_map.point_elements(point),
                |_element, element_values| {
                    for value in element_values {
                        let value = value.to_owned();
                        if !values.contains(&value) {
                            values.push(value);
                        }
                    }
                },
            )?;
            f(point, &mut values.iter().map(facet_value_ref));
        }
        Ok(())
    }

    fn for_values_map(
        &self,
        values: impl Iterator<Item = FacetValue>,
        mut f: impl FnMut(FacetValue, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner.for_values_map(values, |value, elements| {
            f(value, &mut self.to_points(elements).into_iter())
        })
    }

    fn for_each_value(
        &self,
        f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner.for_each_value(f)
    }

    fn for_each_value_map(
        &self,
        mut f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner.for_each_value_map(|value, elements| {
            f(value, &mut self.to_points(elements).into_iter())
        })
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let max_id = deferred_internal_id.unwrap_or(PointOffsetType::MAX);
        self.inner.for_each_value_map(|value, elements| {
            let count = self
                .to_points(elements)
                .into_iter()
                .take_while(|&point| point < max_id)
                .count();
            f(FacetHit { value, count })
        })
    }
}

/// Facet view of any writable field index: a flattened one, or a per-element one.
pub enum FacetView<'a> {
    Plain(FacetIndexEnum<'a>),
    Nested(NestedFacetView<'a>),
}

impl FacetIndex for FacetView<'_> {
    fn unique_values_count(&self) -> usize {
        match self {
            FacetView::Plain(index) => index.unique_values_count(),
            FacetView::Nested(index) => index.unique_values_count(),
        }
    }

    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        match self {
            FacetView::Plain(index) => index.for_points_values(points, f),
            FacetView::Nested(index) => index.for_points_values(points, f),
        }
    }

    fn for_values_map(
        &self,
        values: impl Iterator<Item = FacetValue>,
        f: impl FnMut(FacetValue, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetView::Plain(index) => index.for_values_map(values, f),
            FacetView::Nested(index) => index.for_values_map(values, f),
        }
    }

    fn for_each_value(
        &self,
        f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetView::Plain(index) => index.for_each_value(f),
            FacetView::Nested(index) => index.for_each_value(f),
        }
    }

    fn for_each_value_map(
        &self,
        f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetView::Plain(index) => index.for_each_value_map(f),
            FacetView::Nested(index) => index.for_each_value_map(f),
        }
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetView::Plain(index) => index.for_each_count_per_value(deferred_internal_id, f),
            FacetView::Nested(index) => index.for_each_count_per_value(deferred_internal_id, f),
        }
    }
}
