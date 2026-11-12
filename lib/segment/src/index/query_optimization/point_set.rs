//! Point sets read from posting lists up front, so that checking a point is a bit test or
//! a binary search instead of a lookup in the field index.
//!
//! A set is either a dense bitset over all point offsets of the segment or a sorted list of
//! offsets. The bitset costs `total_points / 8` bytes to allocate and zero no matter how
//! few points it holds, so it only pays off when the set is large or checked many times.
//! [`PointSet::union`] and [`PointSet::intersection`] merge the posting lists of one clause
//! into a single set, so a clause pays for at most one bitset.

use common::bitvec::BitVec;
use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};

/// Cost model constants, in nanoseconds, measured with a micro-benchmark of
/// `bitvec::BitVec<u64>` against a sorted `Vec<u32>` (dense bitset vs sorted list).
///
/// Zeroing one byte of a fresh bitset (memset, ~100 GB/s effective on 1.25 MB).
const ZERO_NS_PER_BYTE: f64 = 0.01;
/// Setting one bit of the bitset while reading a posting list.
const SET_BIT_NS: f64 = 0.6;
/// Testing one bit.
const TEST_BIT_NS: f64 = 0.7;
/// One step of a binary search in a sorted list.
const SEARCH_STEP_NS: f64 = 1.3;
/// Sorting cost per element and per `log2` of the list length.
const SORT_STEP_NS: f64 = 1.0;

/// What a [`PointSet`] is built for: the segment size and how often it will be checked.
#[derive(Debug, Clone, Copy)]
pub struct PointSetContext {
    /// Total number of point offsets in the segment, the length of a bitset.
    pub total_points: usize,
    /// How many points will be checked against the set.
    pub expected_checks: usize,
}

impl PointSetContext {
    /// Whether a bitset is cheaper than a sorted list for a set of `len` points.
    /// `sorted` tells whether the points are already sorted, so the list needs no sort.
    fn prefer_bits(&self, len: usize, sorted: bool) -> bool {
        let len_f = len as f64;
        let checks = self.expected_checks as f64;
        let bits_cost = self.total_points as f64 / 8.0 * ZERO_NS_PER_BYTE
            + len_f * SET_BIT_NS
            + checks * TEST_BIT_NS;
        let log_len = (len_f + 1.0).log2();
        let sort_cost = if sorted {
            0.0
        } else {
            len_f * log_len * SORT_STEP_NS
        };
        let list_cost = sort_cost + checks * log_len * SEARCH_STEP_NS;
        bits_cost < list_cost
    }
}

#[derive(Debug)]
pub enum PointSet {
    /// One bit per point offset of the segment.
    Bits(BitVec),
    /// Sorted, deduplicated point offsets.
    Sorted(Vec<PointOffsetType>),
}

impl PointSet {
    pub fn contains(&self, point_id: PointOffsetType) -> bool {
        match self {
            PointSet::Bits(bits) => bits.get(point_id as usize).is_some_and(|bit| *bit),
            PointSet::Sorted(points) => points.binary_search(&point_id).is_ok(),
        }
    }

    /// Number of points in the set.
    pub fn len(&self) -> usize {
        match self {
            PointSet::Bits(bits) => bits.count_ones(),
            PointSet::Sorted(points) => points.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The points of the set, in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        match self {
            PointSet::Bits(bits) => {
                itertools::Either::Left(bits.iter_ones().map(|point| point as PointOffsetType))
            }
            PointSet::Sorted(points) => itertools::Either::Right(points.iter().copied()),
        }
    }

    /// Points that match any of `postings`.
    pub fn union(postings: Vec<Vec<PointOffsetType>>, context: PointSetContext) -> Self {
        let total_len: usize = postings.iter().map(Vec::len).sum();
        let all_sorted = postings.len() <= 1 && postings.iter().all(|p| p.is_sorted());
        if context.prefer_bits(total_len, all_sorted) {
            let mut bits = BitVec::repeat(false, context.total_points);
            for point in postings.into_iter().flatten() {
                set_bit(&mut bits, point);
            }
            return PointSet::Bits(bits);
        }
        let mut points = postings.concat();
        sort_unique(&mut points);
        PointSet::Sorted(points)
    }

    /// Points that match every one of `postings`. Allocates at most one bitset.
    pub fn intersection(mut postings: Vec<Vec<PointOffsetType>>, context: PointSetContext) -> Self {
        // Smallest first: every step only keeps points of the running result.
        postings.sort_by_key(Vec::len);
        let mut postings = postings.into_iter();
        let Some(first) = postings.next() else {
            // Empty intersection of no conditions would mean "all points"; callers never
            // build one, keep it well-defined anyway.
            return PointSet::Sorted(Vec::new());
        };
        let mut result = Self::union(vec![first], context);
        for posting in postings {
            result = result.intersect(posting, context);
        }
        result
    }

    fn intersect(self, posting: Vec<PointOffsetType>, context: PointSetContext) -> Self {
        match self {
            PointSet::Sorted(points) => {
                let mut posting = posting;
                if posting.len() > points.len().saturating_mul(8) {
                    // Much larger posting: probe it per point instead of sorting it.
                    sort_unique(&mut posting);
                    let kept = points
                        .into_iter()
                        .filter(|point| posting.binary_search(point).is_ok())
                        .collect();
                    return PointSet::Sorted(kept);
                }
                sort_unique(&mut posting);
                PointSet::Sorted(intersect_sorted(&points, &posting))
            }
            PointSet::Bits(mut bits) => {
                let mut kept: Vec<PointOffsetType> = posting
                    .into_iter()
                    .filter(|&point| bits.get(point as usize).is_some_and(|bit| *bit))
                    .collect();
                let sorted = kept.is_sorted();
                if context.prefer_bits(kept.len(), sorted) {
                    // Reuse the allocation: no new bitset for this condition.
                    bits.fill(false);
                    for point in kept {
                        set_bit(&mut bits, point);
                    }
                    PointSet::Bits(bits)
                } else {
                    sort_unique(&mut kept);
                    PointSet::Sorted(kept)
                }
            }
        }
    }
}

fn set_bit(bits: &mut BitVec, point: PointOffsetType) {
    // Ids beyond the segment's point count can't be candidates; ignore them.
    if let Some(mut bit) = bits.get_mut(point as usize) {
        *bit = true;
    }
}

fn sort_unique(points: &mut Vec<PointOffsetType>) {
    if !points.is_sorted() {
        points.sort_unstable();
    }
    points.dedup();
}

fn intersect_sorted(a: &[PointOffsetType], b: &[PointOffsetType]) -> Vec<PointOffsetType> {
    let mut result = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

impl ConditionChecker for PointSet {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        Ok(self.contains(point_id))
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

    fn context(total_points: usize, expected_checks: usize) -> PointSetContext {
        PointSetContext {
            total_points,
            expected_checks,
        }
    }

    fn members(set: &PointSet, total: u32) -> Vec<PointOffsetType> {
        (0..total + 5).filter(|&id| set.contains(id)).collect()
    }

    #[test]
    fn small_sets_are_sorted_lists_large_ones_bitsets() {
        // 10 points checked 10 times in a 10M segment: zeroing 1.25 MB isn't worth it.
        let set = PointSet::union(vec![(0..10).collect()], context(10_000_000, 10));
        assert!(matches!(set, PointSet::Sorted(_)));
        // 100k points checked 100k times in a 1M segment: bitset.
        let set = PointSet::union(vec![(0..100_000).collect()], context(1_000_000, 100_000));
        assert!(matches!(set, PointSet::Bits(_)));
    }

    #[test]
    fn union_and_intersection_match_set_semantics() {
        let a: Vec<PointOffsetType> = vec![9, 1, 3, 3, 42, 7];
        let b: Vec<PointOffsetType> = vec![3, 4, 7, 8];
        let c: Vec<PointOffsetType> = vec![7, 3, 100];
        for ctx in [context(50, 1), context(50, 1_000_000)] {
            let union = PointSet::union(vec![a.clone(), b.clone()], ctx);
            assert_eq!(members(&union, 50), vec![1, 3, 4, 7, 8, 9, 42]);
            let inter = PointSet::intersection(vec![a.clone(), b.clone(), c.clone()], ctx);
            assert_eq!(members(&inter, 50), vec![3, 7]);
            let inter = PointSet::intersection(vec![b.clone(), vec![]], ctx);
            assert_eq!(members(&inter, 50), Vec::<PointOffsetType>::new());
        }
    }

    #[test]
    fn intersection_with_a_bitset_reuses_it() {
        // First posting big enough for a bitset, intersection still large.
        let total = 4096;
        let ctx = context(total, 1_000_000);
        let a: Vec<PointOffsetType> = (0..total as u32).collect();
        let b: Vec<PointOffsetType> = (0..total as u32).filter(|x| x % 2 == 0).collect();
        let inter = PointSet::intersection(vec![a, b.clone()], ctx);
        assert!(matches!(inter, PointSet::Bits(_)));
        assert_eq!(members(&inter, total as u32), b);
    }
}
