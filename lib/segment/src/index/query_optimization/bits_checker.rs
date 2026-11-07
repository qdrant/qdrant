use common::bitvec::BitVec;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};

/// Checks membership in a condition's posting list that was read into a bitset up front,
/// so each check is a bit test instead of a lookup in the field index.
pub struct BitsConditionChecker(BitVec);

impl BitsConditionChecker {
    /// Build from the points of a posting list. Ids at or beyond `len` are ignored.
    pub fn from_points(len: usize, points: impl Iterator<Item = PointOffsetType>) -> Self {
        let mut bits = BitVec::repeat(false, len);
        for point in points {
            if let Some(mut bit) = bits.get_mut(point as usize) {
                *bit = true;
            }
        }
        Self(bits)
    }
}

impl ConditionChecker for BitsConditionChecker {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        Ok(self.0.get(point_id as usize).is_some_and(|bit| *bit))
    }

    fn check_batched<K: CheckItem>(
        &self,
        ids: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        default_check_batched(ids, select, rest, |id| self.check(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership() {
        let checker = BitsConditionChecker::from_points(10, [1, 3, 9, 42].into_iter());
        let matched: Vec<_> = (0..12).filter(|&id| checker.check(id).unwrap()).collect();
        assert_eq!(matched, vec![1, 3, 9]);
    }
}
