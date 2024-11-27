use std::collections::{HashMap, HashSet};
use std::hash;
use std::iter::Peekable;
use std::rc::Rc;

use itertools::Itertools;
use segment::data_types::facets::{FacetResponse, FacetValue};
use segment::types::{Payload, ScoredPoint};
use tinyvec::TinyVec;

use crate::common::transpose_iterator::transposed_iter;
use crate::operations::types::{CountResult, RecordInternal};
use crate::operations::universal_query::shard_query::ShardQueryResponse;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ResolveCondition {
    All,
    Majority,
}

impl ResolveCondition {
    fn resolution_count(&self, num_replicas: usize) -> usize {
        match self {
            Self::All => num_replicas,
            Self::Majority => num_replicas / 2 + 1,
        }
    }
}

pub trait Resolve: Sized {
    fn resolve(responses: Vec<Self>, condition: ResolveCondition) -> Self;
}

impl Resolve for CountResult {
    fn resolve(records: Vec<Self>, condition: ResolveCondition) -> Self {
        match condition {
            ResolveCondition::All => Self {
                count: records
                    .iter()
                    .map(|result| result.count)
                    .min()
                    .unwrap_or_default(),
            },
            ResolveCondition::Majority => {
                let mut counts = records
                    .iter()
                    .map(|result| result.count)
                    .collect::<Vec<_>>();
                counts.sort_unstable();
                let middle = counts.len() / 2;
                Self {
                    count: counts.get(middle).copied().unwrap_or_default(),
                }
            }
        }
    }
}

impl Resolve for FacetResponse {
    /// Resolve the counts for each value using the CountResult implementation
    fn resolve(responses: Vec<Self>, condition: ResolveCondition) -> Self {
        let num_replicas = responses.len();
        let resolution_count = condition.resolution_count(num_replicas);

        // Example responses:
        // [
        //   {
        //     hits: [
        //       { value: "a", count: 20 },
        //       { value: "b": count: 15 }
        //     ]
        //   },
        //   {
        //     hits: [
        //       { value: "a", count: 21 },
        //       { value: "b": count: 13 }
        //     ]
        //   },
        // ]

        let resolved_counts: HashMap<_, _> = responses
            .iter()
            .flat_map(|FacetResponse { hits }| hits)
            // Collect all hits into a Hashmap of {value -> Vec<CountResult>}
            .fold(
                HashMap::new(),
                |mut map: HashMap<FacetValue, Vec<CountResult>>, hit| {
                    if let Some(counts) = map.get_mut(&hit.value) {
                        counts.push(CountResult { count: hit.count });
                    } else {
                        map.entry(hit.value.clone())
                            .or_insert(Vec::with_capacity(num_replicas))
                            .push(CountResult { count: hit.count });
                    };
                    map
                },
            )
            .into_iter()
            // Filter out values that don't appear in enough replicas
            .filter(|(_, counts)| counts.len() >= resolution_count)
            // Resolve the counts with the CountResult implementation
            .map(|(value, counts)| {
                let count = CountResult::resolve(counts, condition).count;

                (value, count)
            })
            .collect();

        let filtered_iters = responses.into_iter().map(|FacetResponse { hits }| {
            hits.into_iter().filter_map(|mut hit| {
                resolved_counts.get(&hit.value).map(|&count| {
                    // Use the resolved count
                    hit.count = count;
                    hit
                })
            })
        });

        // Retain the original order of the hits (instead of always sorting in the same direction).
        let resolved_hits =
            MergeInOrder::new(filtered_iters, |hit| hit.value.clone(), resolution_count).collect();

        // resolved_hits for ResolveCondition::All:
        // [
        //  { value: "a", count: 20 },
        //  { value: "b", count: 13 }
        // ]

        FacetResponse {
            hits: resolved_hits,
        }
    }
}

impl Resolve for Vec<RecordInternal> {
    fn resolve(records: Vec<Self>, condition: ResolveCondition) -> Self {
        Resolver::resolve(records, |record| record.id, record_eq, condition)
    }
}

impl Resolve for Vec<Vec<ScoredPoint>> {
    fn resolve(batches: Vec<Self>, condition: ResolveCondition) -> Self {
        // batches: <replica_id, <batch_id, ScoredPoints>>
        // transpose to <batch_id, <replica_id, ScoredPoints>>

        let batches = transposed_iter(batches);

        batches
            .map(|points| Resolver::resolve(points, |point| point.id, scored_point_eq, condition))
            .collect()
    }
}

impl Resolve for Vec<ShardQueryResponse> {
    fn resolve(batches: Vec<Self>, condition: ResolveCondition) -> Self {
        // batches: <replica_id, <batch_id, ShardQueryResponse>>
        // transpose to <batch_id, <replica_id, ShardQueryResponse>>

        let batches = transposed_iter(batches);

        batches
            .into_iter()
            .map(|shard_responses| Resolve::resolve(shard_responses, condition))
            .collect()
    }
}

fn record_eq(this: &RecordInternal, other: &RecordInternal) -> bool {
    this.id == other.id
        && this.order_value == other.order_value
        && this.vector == other.vector
        && payload_eq(&this.payload, &other.payload)
}

fn scored_point_eq(this: &ScoredPoint, other: &ScoredPoint) -> bool {
    this.id == other.id
        && this.score == other.score
        && this.order_value == other.order_value
        && this.vector == other.vector
        && payload_eq(&this.payload, &other.payload)
}

fn payload_eq(this: &Option<Payload>, other: &Option<Payload>) -> bool {
    match (this, other) {
        (Some(payload), None) | (None, Some(payload)) => payload.is_empty(),
        (this, other) => this == other,
    }
}

/// Expected number of replicas
const EXPECTED_REPLICAS: usize = 5;

type ResolverRecords<'a, Item> = TinyVec<[ResolverRecord<'a, Item>; EXPECTED_REPLICAS]>;

struct Resolver<'a, Item, Id, Ident, Cmp> {
    items: HashMap<Id, ResolverRecords<'a, Item>>,
    identify: Ident,
    compare: Cmp,
}

impl<'a, Item, Id, Ident, Cmp> Resolver<'a, Item, Id, Ident, Cmp>
where
    Id: Eq + hash::Hash,
    Ident: Fn(&Item) -> Id + Copy,
    Cmp: Fn(&Item, &Item) -> bool,
{
    pub fn resolve(
        items: Vec<Vec<Item>>,
        identify: Ident,
        compare: Cmp,
        condition: ResolveCondition,
    ) -> Vec<Item> {
        let resolution_count = condition.resolution_count(items.len());

        // Items:
        //  [
        //      [
        //          { id: 10, item: A, score: 0.9 },
        //          { id: 3, item: B, score: 0.8 },
        //          { id: 4, item: C, score: 0.7 }
        //      ],
        //      [
        //          { id: 10, item: A, score: 0.9 },
        //          { id: 3, item: B, score: 0.8 },
        //          { id: 4, item: C, score: 0.7 }
        //      ],
        //      [
        //          { id: 10, item: A, score: 0.9 },
        //          { id: 4, item: C, score: 0.7 },
        //          { id: 2, item: D, score: 0.6 }
        //      ]
        //  ]
        let mut resolver = Resolver::new(items.first().map_or(0, Vec::len), identify, compare);
        resolver.add_all(&items);

        // resolver items:
        // {
        //     10: [ { item: A, count: 3, coordinates: [(0, 0), (1, 0), (2, 0)] } ],
        //     3:  [ { item: B, count: 2, coordinates: [(0, 1), (1, 1)] } ],
        //     4:  [ { item: C, count: 3, coordinates: [(0, 2), (1, 2), (2, 1)] } ],
        //     2:  [ { item: D, count: 1, coordinates: [(2, 2)] } ]
        // }

        // For majority, we need resolution_count = 2

        // Select coordinates of accepted items, avoiding copying
        let resolved_coords: HashSet<_> = resolver
            .items
            .into_iter()
            .filter_map(|(_, points)| {
                points
                    .into_iter()
                    .find(|point| point.count >= resolution_count)
                    .map(|point| point.coordinates.into_iter())
            })
            .flatten()
            .collect();

        // resolved coords:
        // [
        //     (0, 0), (1, 0), (2, 0),
        //     (0, 1), (1, 1),
        //     (0, 2), (1, 2), (2, 1)
        // ]

        // Shortcut if everything is consistent: return first items, avoiding filtering
        let all_items_len = items.iter().map(Vec::len).sum::<usize>();
        let is_consistent = resolved_coords.len() == all_items_len;

        if is_consistent {
            // Return the first replica result as everything is consistent
            return items.into_iter().next().unwrap_or_default();
        }

        // Items:
        //  [
        //      [
        //          { id: 3, item: B, score: 0.8 },
        //          { id: 4, item: C, score: 0.7 }
        //      ],
        //      [
        //          { id: 3, item: B, score: 0.8 },
        //          { id: 4, item: C, score: 0.7 }
        //      ],
        //      [
        //          { id: 4, item: C, score: 0.7 },
        //      ]
        //  ]
        let resolved_coords = Rc::new(resolved_coords);

        let resolved_iters = items
            .into_iter()
            .enumerate()
            .map(|(replica_id, replica_response)| {
                // replica_response:
                //  [
                //      { id: 10, item: A, score: 0.9 },
                //      { id: 4, item: C, score: 0.7 },
                //      { id: 2, item: D, score: 0.6 }
                //  ]

                let resolved_coords = resolved_coords.clone();
                replica_response
                    .into_iter()
                    .enumerate()
                    .filter_map(move |(index, item)| {
                        resolved_coords
                            .contains(&(replica_id, index))
                            .then_some(item)
                    })

                // Iterator of filtered items:
                //  Iter<
                //      { id: 10, item: A, score: 0.9 },
                //      { id: 4, item: C, score: 0.7 },
                //  >
            });

        MergeInOrder::new(resolved_iters, identify, resolution_count).collect_vec()
    }

    fn new(capacity: usize, identify: Ident, compare: Cmp) -> Self {
        Self {
            items: HashMap::with_capacity(capacity),
            identify,
            compare,
        }
    }

    fn add_all<I>(&mut self, items: I)
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = &'a Item>,
    {
        for (row, items) in items.into_iter().enumerate() {
            for (index, item) in items.into_iter().enumerate() {
                self.add((self.identify)(item), item, row, index);
            }
        }
    }

    fn add(&mut self, id: Id, item: &'a Item, row: RowId, index: ColumnId) {
        let points = self.items.entry(id).or_default();

        for point in points.iter_mut() {
            if (self.compare)(item, point.item.unwrap()) {
                point.count += 1;
                point.coordinates.push((row, index));
                return;
            }
        }

        points.push(ResolverRecord::new(item, row, index));
    }
}

type RowId = usize;
type ColumnId = usize;

#[derive(Debug, Clone)]
struct ResolverRecord<'a, T> {
    item: Option<&'a T>,
    /// Store all coordinates of equal items in `(row, index)` tuples
    coordinates: TinyVec<[(RowId, ColumnId); EXPECTED_REPLICAS]>,
    /// Keeps track of the amount of times we see this same item
    count: usize,
}

impl<T> Default for ResolverRecord<'_, T> {
    fn default() -> Self {
        Self {
            item: None,
            coordinates: Default::default(),
            count: 0,
        }
    }
}

impl<'a, T> ResolverRecord<'a, T> {
    fn new(item: &'a T, row: RowId, index: ColumnId) -> Self {
        let mut coordinates = TinyVec::new();
        coordinates.push((row, index));

        Self {
            item: Some(item),
            coordinates,
            count: 1,
        }
    }
}

/// Resolves multiple list of items by reading heads of all iterators on each step
/// and accepting the most common occurrence as the next resolved item.
///
/// [
///    [A, F, B, C],
///    [A, B, C],
///    [F, B, C],
/// ]
///
///   1   2   3   4
///  [A,  F,  B,  C]
///  [A,      B,  C]
///  [    F,  B,  C]
struct MergeInOrder<I: Iterator, Ident> {
    /// One iterator per set of results, which outputs items that comply with resolution count, in their original order
    resolved_iters: Vec<Peekable<I>>,
    /// Closure which retrieves the item's ID
    ident: Ident,

    /// Only used to debug_assert correctness
    resolution_count: usize,
}

impl<Iter, Ident, Id, Item> MergeInOrder<Iter, Ident>
where
    Id: Eq + hash::Hash,
    Ident: Fn(&Item) -> Id,
    Iter: Iterator<Item = Item>,
{
    fn new(
        resolved_iters: impl Iterator<Item = Iter>,
        identify: Ident,
        resolution_count: usize,
    ) -> Self {
        let resolved_iters = resolved_iters.map(|iter| iter.peekable()).collect();

        Self {
            resolved_iters,
            ident: identify,
            resolution_count,
        }
    }

    /// An iterator over all current heads of the resolved iterators
    fn peek_heads(&mut self) -> impl Iterator<Item = (RowId, Id)> + '_ {
        self.resolved_iters
            .iter_mut()
            .enumerate()
            .filter_map(|(row, iter)| iter.peek().map(|peeked| (row, (self.ident)(peeked))))
    }

    /// Peeks each row, then maps IDs to the peeked rows in which each ID appears
    ///
    /// Example:
    ///
    ///  resolved_iters = [
    ///     <- (10, A) <- (4, B) <- (3, C)
    ///     <- (10, A) <- (4, B) <- (3, C)
    ///     <- (4, B) <- (3, C)
    /// ]
    ///
    /// output:
    /// {
    ///     10: [0, 1],
    ///     4:  [2],
    /// }
    fn heads_map(&mut self) -> HashMap<Id, TinyVec<[RowId; EXPECTED_REPLICAS]>> {
        let capacity = self.resolved_iters.len();
        self.peek_heads()
            .fold(HashMap::with_capacity(capacity), |mut map, (row, id)| {
                let entry = map.entry(id).or_default();
                entry.push(row);
                map
            })
    }

    /// Advances the rows and returns the item in the first of them
    ///
    /// Minimum len of `row_ids` should be the resolution count.
    fn advance_rows(&mut self, row_ids: &[RowId]) -> Option<Item> {
        debug_assert!(row_ids.len() >= self.resolution_count);

        let mut merged_item = None;
        for row_id in row_ids {
            merged_item = self.resolved_iters[*row_id].next();
        }

        merged_item
    }
}

impl<Iter, Ident, Id, Item> Iterator for MergeInOrder<Iter, Ident>
where
    Id: Eq + hash::Hash,
    Ident: Fn(&Item) -> Id,
    Iter: Iterator<Item = Item>,
{
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        // Choose the item that appears the most times in the heads

        // heads_map: (id to source row_ids)
        // {
        //     10: [0, 1],
        //     4:  [2],
        // }
        let heads_map = self.heads_map();

        // Most frequent row IDs - Assume most frequent item is the one to be resolved next
        // [0, 1]
        let chosen_rows = heads_map.into_values().max_by_key(|kv| kv.len())?;

        // Pull the item from the chosen rows (return only one of them)
        self.advance_rows(&chosen_rows)
    }
}

#[cfg(test)]
mod test {
    use std::fmt;

    use common::types::ScoreType;

    use super::*;

    #[rustfmt::skip]
    fn resolve_scored_points_batch_4_data() -> [Vec<ScoredPoint>; 3] {
        [
            vec![
                point(14, 0.0), point(17, 0.1), point(15, 0.1),
                point(13, 0.2), point(11, 0.2), point(12, 0.3),
                point(18, 0.3), point(16, 0.4), point(10, 0.5),
            ],
            vec![
                point(23, 0.0), point(21, 0.1), point(25, 0.2),
                point(22, 0.2), point(20, 0.3), point(24, 0.3),
            ],
            vec![
                point(30, 0.1), point(31, 0.1), point(32, 0.1),
                point(33, 0.2), point(34, 0.2), point(35, 0.3),
            ],
        ]
    }

    fn point(id: u64, score: ScoreType) -> ScoredPoint {
        ScoredPoint {
            id: id.into(),
            version: 1,
            score,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    #[rustfmt::skip]
    fn resolve_scored_points_batch_4_input() -> Vec<Vec<Vec<ScoredPoint>>> {
        let [batch1, batch2, batch3] = resolve_scored_points_batch_4_data();

        vec![
            vec![
                batch(&batch1, [remove(2), remove(3)]),
                batch(&batch2, [remove(0), remove(3)]),
                batch(&batch3, [remove(4), remove(5)]),
            ],

            vec![
                batch(&batch1, [remove(1), modify(3)]),
                batch(&batch2, [modify(0), remove(2)]),
                batch(&batch3, [remove(3), modify(5)]),
            ],

            vec![
                batch(&batch1, [remove(1), modify(4)]),
                batch(&batch2, [modify(3), remove(5)]),
                batch(&batch3, [remove(2), modify(5)]),
            ],

            vec![
                batch1,
                batch2,
                batch3,
            ],
        ]
    }

    fn batch<const N: usize>(batch: &[ScoredPoint], mut actions: [Action; N]) -> Vec<ScoredPoint> {
        let mut batch = batch.to_owned();

        actions.sort_unstable_by_key(|action| action.index());

        let mut removed = Vec::new();

        for action in actions {
            let offset = removed
                .iter()
                .filter(|&&removed| removed <= action.index())
                .count();

            match action {
                Action::Remove(index) => {
                    batch.remove(index - offset);
                    removed.push(index);
                }

                Action::Modify(index) => {
                    batch[index - offset].score += 1.0;
                }
            }
        }

        batch
    }

    #[derive(Copy, Clone, Debug)]
    enum Action {
        Remove(usize),
        Modify(usize),
    }

    impl Action {
        pub fn index(self) -> usize {
            match self {
                Self::Remove(index) => index,
                Self::Modify(index) => index,
            }
        }
    }

    fn remove(index: usize) -> Action {
        Action::Remove(index)
    }

    fn modify(index: usize) -> Action {
        Action::Modify(index)
    }

    #[test]
    fn resolve_scored_points_batch_4_all() {
        let [mut batch1, mut batch2, mut batch3] = resolve_scored_points_batch_4_data();

        batch1.remove(4);
        batch1.remove(3);
        batch1.remove(2);
        batch1.remove(1);

        batch2.remove(5);
        batch2.remove(3);
        batch2.remove(2);
        batch2.remove(0);

        batch3.remove(5);
        batch3.remove(4);
        batch3.remove(3);
        batch3.remove(2);

        test_resolve(
            resolve_scored_points_batch_4_input(),
            [batch1, batch2, batch3],
            ResolveCondition::All,
        );
    }

    #[test]
    fn resolve_scored_points_batch_4_majority() {
        let [mut batch1, mut batch2, mut batch3] = resolve_scored_points_batch_4_data();

        batch1.remove(3);
        batch1.remove(1);

        batch2.remove(3);
        batch2.remove(0);

        batch3.remove(5);

        test_resolve(
            resolve_scored_points_batch_4_input(),
            [batch1, batch2, batch3],
            ResolveCondition::Majority,
        );
    }

    fn data_simple() -> [i32; 9] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9]
    }

    #[rustfmt::skip]
    fn input_2() -> [Vec<i32>; 2] {
        [
            vec![1, 2, 3,       6, 7,    9,     11, 12, 13],
            vec![      3, 4, 5, 6,    8,    10, 11,       ],
        ]
    }

    fn expected_2() -> [i32; 3] {
        [3, 6, 11]
    }

    #[rustfmt::skip]
    fn input_3() -> [Vec<i32>; 3] {
        [
            vec![1, 2,          6, 7, 8,       11,      13, 14, 15,       ],
            vec![   2, 3, 4,       7,    9, 10,         13, 14,     16,   ],
            vec![         4, 5, 6, 7,    9,    11, 12,      14,         17],
        ]
    }

    fn expected_3_all() -> [i32; 2] {
        [7, 14]
    }

    fn expected_3_majority() -> [i32; 8] {
        [2, 4, 6, 7, 9, 11, 13, 14]
    }

    #[rustfmt::skip]
    fn input_4() -> [Vec<i32>; 4] {
        [
            vec![1, 2, 3,                9,     11, 12, 13, 14,     16,         19,     21, 22,     24,     27,     29],
            vec![   2, 3, 4, 5, 6,                  12, 13,     15,     17,     19,         22,     24, 26, 27, 28,   ],
            vec![      3,    5, 6, 7, 8, 9,             13,     15, 16,     18,     20,     22,         26, 27, 28,   ],
            vec![               6,    8, 9, 10, 11, 12, 13,         16,     18, 19,     21,     23,     26, 27,     29],
        ]
    }

    fn expected_4_all() -> [i32; 2] {
        [13, 27]
    }

    fn expected_4_majority() -> [i32; 10] {
        [3, 6, 9, 12, 13, 16, 19, 22, 26, 27]
    }

    #[rustfmt::skip]
    fn input_5() -> [Vec<i32>; 3] {
        [
            vec![1, 2, 3, 4, 5, 6, 7],
            vec![               6, 7],
            vec![1, 2, 3, 4         ],
        ]
    }

    fn expected_5_majority() -> [i32; 6] {
        [1, 2, 3, 4, 6, 7]
    }

    fn expected_5_all() -> [i32; 0] {
        []
    }

    #[test]
    fn resolve_0_all() {
        resolve_0(ResolveCondition::All);
    }

    #[test]
    fn resolve_0_majority() {
        resolve_0(ResolveCondition::Majority);
    }

    fn resolve_0(condition: ResolveCondition) {
        test_resolve_simple(Vec::<Vec<i32>>::new(), Vec::new(), condition);
    }

    #[test]
    fn resolve_simple_all() {
        for replicas in 1..=5 {
            resolve_simple(replicas, ResolveCondition::All);
        }
    }

    #[test]
    fn resolve_simple_majority() {
        for replicas in 1..=5 {
            resolve_simple(replicas, ResolveCondition::All);
        }
    }

    fn resolve_simple(replicas: usize, condition: ResolveCondition) {
        let input: Vec<_> = (0..replicas).map(|_| data_simple()).collect();
        let expected = data_simple();

        test_resolve_simple(input, expected, condition)
    }

    #[test]
    fn resolve_2_all() {
        test_resolve_simple(input_2(), expected_2(), ResolveCondition::All);
    }

    #[test]
    fn resolve_2_majority() {
        test_resolve_simple(input_2(), expected_2(), ResolveCondition::Majority);
    }

    #[test]
    fn resolve_3_all() {
        test_resolve_simple(input_3(), expected_3_all(), ResolveCondition::All);
    }

    #[test]
    fn resolve_3_majority() {
        test_resolve_simple(input_3(), expected_3_majority(), ResolveCondition::Majority);
    }

    #[test]
    fn resolve_4_all() {
        test_resolve_simple(input_4(), expected_4_all(), ResolveCondition::All);
    }

    #[test]
    fn resolve_4_majority() {
        test_resolve_simple(input_4(), expected_4_majority(), ResolveCondition::Majority);
    }

    #[test]
    fn resolve_5_majority() {
        test_resolve_simple(input_5(), expected_5_majority(), ResolveCondition::Majority);
    }

    #[test]
    fn resolve_5_all() {
        test_resolve_simple(input_5(), expected_5_all(), ResolveCondition::All);
    }

    fn test_resolve<T, E>(input: Vec<T>, expected: E, condition: ResolveCondition)
    where
        T: Resolve + Clone + PartialEq<E> + fmt::Debug,
        E: fmt::Debug,
    {
        assert_eq!(T::resolve(input, condition), expected);
    }

    fn test_resolve_simple<I, E>(input: I, expected: E, condition: ResolveCondition)
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = i32>,
        E: IntoIterator<Item = i32>,
    {
        test_resolve(simple_input(input), simple_expected(expected), condition);
    }

    fn simple_input<I>(input: I) -> Vec<Vec<Val>>
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = i32>,
    {
        input
            .into_iter()
            .map(|items| items.into_iter().map(Val).collect())
            .collect()
    }

    fn simple_expected<E>(expected: E) -> Vec<Val>
    where
        E: IntoIterator<Item = i32>,
    {
        expected.into_iter().map(Val).collect()
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
    struct Val(i32);

    impl Resolve for Vec<Val> {
        fn resolve(values: Vec<Self>, condition: ResolveCondition) -> Self {
            Resolver::resolve(values, |val| val.0, PartialEq::eq, condition)
        }
    }
}
