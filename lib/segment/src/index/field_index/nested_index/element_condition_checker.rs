use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};

use super::{ElementMapRead, ElementOffsetType};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::condition_checker::ConditionCheckerEnum;

/// A condition checked on the elements of one per-element index.
///
/// Every per-element index has its own element offsets, so the conditions of one nested
/// filter can't share them as they are. Instead, all conditions of a filter take offsets
/// of one `input_map`, the map of one of the indexes, and each translates them to the
/// offsets of its own index: input offset → (point, position) → own offset, see
/// [`ElementMap::element_position`]. That way they can be combined in one
/// [`OptimizedFilter`](crate::index::query_optimization::optimized_filter::OptimizedFilter).
pub struct ElementConditionChecker<'a> {
    /// Map of the offsets passed to [`ConditionChecker::check`]. Starts as `own_map`, see
    /// [`Self::with_input_map`].
    input_map: &'a dyn ElementMapRead,
    /// Map of the index that answers the condition, the offsets `checker` takes.
    own_map: &'a dyn ElementMapRead,
    /// Checker of the inner index, over offsets of `own_map`.
    checker: ConditionCheckerEnum<'a>,
}

impl<'a> ElementConditionChecker<'a> {
    /// `checker` takes offsets of `own_map`, the map of the index it checks.
    pub(super) fn new(own_map: &'a dyn ElementMapRead, checker: ConditionCheckerEnum<'a>) -> Self {
        Self {
            input_map: own_map,
            own_map,
            checker,
        }
    }

    /// Map of the index that answers the condition.
    pub fn own_map(&self) -> &'a dyn ElementMapRead {
        self.own_map
    }

    /// Take offsets of `input_map` instead of [`Self::own_map`].
    pub fn with_input_map(self, input_map: &'a dyn ElementMapRead) -> Self {
        Self { input_map, ..self }
    }
}

impl ConditionChecker for ElementConditionChecker<'_> {
    type Error = OperationError;

    fn check(&self, input_element: ElementOffsetType) -> OperationResult<bool> {
        if std::ptr::addr_eq(self.input_map, self.own_map) {
            return self.checker.check(input_element);
        }
        let Some((point, position)) = self.input_map.element_position(input_element) else {
            return Ok(false);
        };
        // All maps of an array agree on the elements of a point and their order
        let Some(own_element) = self.own_map.element_at(point, position) else {
            return Ok(false);
        };
        self.checker.check(own_element)
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
