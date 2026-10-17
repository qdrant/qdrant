use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};

use super::{ElementMap, ElementOffsetType};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::condition_checker::ConditionCheckerEnum;

/// A condition checked on the elements of one per-element index.
///
/// [`ConditionChecker::check`] takes element offsets of the `anchor` map, so that
/// conditions answered by different indexes of the array can be combined in one
/// [`OptimizedFilter`](crate::index::query_optimization::optimized_filter::OptimizedFilter).
/// Each offset is translated to the index's own map, see [`ElementMap::translate`].
pub struct ElementConditionChecker<'a> {
    anchor: &'a ElementMap,
    element_map: &'a ElementMap,
    checker: ConditionCheckerEnum<'a>,
}

impl<'a> ElementConditionChecker<'a> {
    /// `checker` is a checker of an index keyed by the offsets of `element_map`, which
    /// are also the offsets it takes until [`Self::with_anchor`].
    pub(super) fn new(element_map: &'a ElementMap, checker: ConditionCheckerEnum<'a>) -> Self {
        Self {
            anchor: element_map,
            element_map,
            checker,
        }
    }

    pub fn element_map(&self) -> &'a ElementMap {
        self.element_map
    }

    /// Take element offsets of `anchor` instead of the index's own map.
    pub fn with_anchor(self, anchor: &'a ElementMap) -> Self {
        Self { anchor, ..self }
    }
}

impl ConditionChecker for ElementConditionChecker<'_> {
    type Error = OperationError;

    fn check(&self, element: ElementOffsetType) -> OperationResult<bool> {
        match self.element_map.translate(self.anchor, element) {
            Some(element) => self.checker.check(element),
            None => Ok(false),
        }
    }

    fn check_batched<K: CheckItem>(
        &self,
        items: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        default_check_batched(items, select, rest, |element| self.check(element))
    }
}
