use std::collections::HashMap;
use std::iter;
use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use ecow::EcoString;
use gridstore::Blob;

use super::super::MapIndex;
use super::super::key::MapIndexKey;
use super::super::prefix_index::{PrefixIndexStats, StrMapIndexPrefixRead};
use super::super::read_only::ReadOnlyMapIndex;
use super::super::read_ops::{MapConditionChecker, MapIndexRead};
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::map_index::IdIter;
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    PrimaryCondition,
};
use crate::index::query_estimator::combine_should_estimations;
use crate::types::{
    AnyVariants, FieldCondition, Match, MatchAny, MatchExcept, MatchPrefix, MatchValue,
    PayloadKeyType, ValueVariants,
};

impl PayloadFieldIndex for MapIndex<str> {
    fn wipe(self) -> OperationResult<()> {
        self.wipe()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.immutable_files()
    }
}

impl PayloadFieldIndexRead for MapIndex<str> {
    fn count_indexed_points(&self) -> OperationResult<usize> {
        Ok(MapIndexRead::get_indexed_points(self))
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        filter_impl(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        estimate_cardinality_impl(self, condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for_each_payload_block_impl(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        Ok(condition_checker_impl(self, condition, hw_acc)
            .map(ConditionCheckerEnum::MapStrWritable))
    }
}

impl<S: UniversalReadExt> PayloadFieldIndexRead for ReadOnlyMapIndex<str, S>
where
    Vec<<str as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn count_indexed_points(&self) -> OperationResult<usize> {
        Ok(MapIndexRead::get_indexed_points(self))
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        filter_impl(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        estimate_cardinality_impl(self, condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for_each_payload_block_impl(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        Ok(condition_checker_impl(self, condition, hw_acc).map(S::condition_checker_map_str))
    }
}

// Shared bodies for `MapIndex<str>` and `ReadOnlyMapIndex<str, S>`. Parameterized
// over `T: MapIndexRead<str>` so a single body serves both `PayloadFieldIndexRead`
// impls above.

fn filter_impl<'a, T: MapIndexRead<'a, str> + StrMapIndexPrefixRead>(
    index: &'a T,
    condition: &'a FieldCondition,
    hw_counter: &'a HardwareCounterCell,
) -> OperationResult<Option<IdIter<'a>>> {
    let result: Option<IdIter<'a>> = match &condition.r#match {
        Some(Match::Value(MatchValue { value })) => match value {
            ValueVariants::String(keyword) => {
                Some(Box::new(index.get_iterator(keyword.as_str(), hw_counter)))
            }
            ValueVariants::Integer(_) => None,
            ValueVariants::Bool(_) => None,
        },
        Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
            AnyVariants::Strings(keywords) => {
                Some(index.iter_for_values(keywords.iter().map(AsRef::as_ref), hw_counter)?)
            }
            AnyVariants::Integers(integers) => {
                if integers.is_empty() {
                    Some(Box::new(iter::empty()))
                } else {
                    None
                }
            }
        },
        Some(Match::Except(MatchExcept { except })) => match except {
            AnyVariants::Strings(keywords) => Some(index.except_set(keywords, hw_counter)?),
            AnyVariants::Integers(_) => None,
        },
        Some(Match::Prefix(MatchPrefix { prefix })) => {
            // `None` when this index instance has no prefix structure — the
            // caller then falls back to the generic (slow) condition check.
            match index.prefix_keys_with_counts(prefix, hw_counter)? {
                Some(keys) => Some(
                    index.iter_for_values(keys.into_iter().map(|(key, _count)| key), hw_counter)?,
                ),
                None => None,
            }
        }
        _ => None,
    };

    Ok(result)
}

fn estimate_cardinality_impl<'a, T: MapIndexRead<'a, str> + StrMapIndexPrefixRead>(
    index: &'a T,
    condition: &FieldCondition,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<CardinalityEstimation>> {
    let estimation = match &condition.r#match {
        Some(Match::Value(MatchValue { value })) => match value {
            ValueVariants::String(keyword) => {
                let mut estimation = index.match_cardinality(keyword.as_str(), hw_counter);
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(Box::new(condition.clone())));
                Some(estimation)
            }
            ValueVariants::Integer(_) => None,
            ValueVariants::Bool(_) => None,
        },
        Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
            AnyVariants::Strings(keywords) => {
                let estimations = keywords
                    .iter()
                    .map(|keyword| index.match_cardinality(keyword.as_str(), hw_counter))
                    .collect::<Vec<_>>();
                let estimation = if estimations.is_empty() {
                    CardinalityEstimation::exact(0)
                } else {
                    combine_should_estimations(&estimations, index.get_indexed_points())
                };
                Some(
                    estimation.with_primary_clause(PrimaryCondition::Condition(Box::new(
                        condition.clone(),
                    ))),
                )
            }
            AnyVariants::Integers(integers) => {
                if integers.is_empty() {
                    Some(CardinalityEstimation::exact(0).with_primary_clause(
                        PrimaryCondition::Condition(Box::new(condition.clone())),
                    ))
                } else {
                    None
                }
            }
        },
        Some(Match::Except(MatchExcept { except })) => match except {
            AnyVariants::Strings(keywords) => {
                Some(index.except_cardinality(keywords.iter().map(|k| k.as_str()), hw_counter))
            }
            AnyVariants::Integers(_) => None,
        },
        Some(Match::Prefix(MatchPrefix { prefix })) => {
            index.prefix_stats(prefix, hw_counter)?.map(|stats| {
                prefix_cardinality(index, stats)
                    .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone())))
            })
        }
        _ => None,
    };
    Ok(estimation)
}

/// Cardinality of a prefix match from aggregate `(keys, postings)` stats.
///
/// A prefix selects the union of its keys' postings. The sum of counts is an
/// upper bound (a point with several values sharing the prefix is counted
/// once per value); the union cannot be smaller than the largest single
/// posting, which is at least the average. The on-disk stats are build-time
/// counts, so with deletions this is an estimate, like other on-disk
/// count-based estimations.
fn prefix_cardinality<'a, T: MapIndexRead<'a, str>>(
    index: &'a T,
    stats: PrefixIndexStats,
) -> CardinalityEstimation {
    let PrefixIndexStats { keys, postings } = stats;
    let indexed_points = index.get_indexed_points();

    let sum = postings.min(index.get_values_count());
    let max = sum.min(indexed_points);
    let min = if keys == 0 {
        0
    } else {
        sum.div_ceil(keys).min(max)
    };
    let exp = number_of_selected_points(indexed_points, sum).clamp(min, max);

    CardinalityEstimation {
        primary_clauses: vec![],
        min,
        exp,
        max,
    }
}

fn for_each_payload_block_impl<'a, T: MapIndexRead<'a, str> + StrMapIndexPrefixRead>(
    index: &'a T,
    threshold: usize,
    key: PayloadKeyType,
    f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
) -> OperationResult<()> {
    // Prefix blocks are disjoint from each other and from the exact-value
    // blocks (see `heavy_prefix_blocks`), so emission order doesn't matter;
    // blocks come largest-first for determinism.
    for_each_prefix_payload_block(index, threshold, &key, f)?;

    index.for_each_value(|value| {
        let count = index
            // `for_each_payload_block` is only used while building HNSW, which
            // intentionally bypasses hardware measurement.
            .get_count_for_value(value, &HardwareCounterCell::disposable())
            .unwrap_or(0);
        if count > threshold {
            f(PayloadBlockCondition {
                condition: FieldCondition::new_match(key.clone(), value.to_string().into()),
                cardinality: count,
            })?;
        }
        Ok(())
    })
}

/// Emit a payload block per "heavy" prefix, so HNSW builds additional links
/// guaranteeing a navigable subgraph for prefix-filtered searches.
///
/// No-op for indexes built without the prefix option.
fn for_each_prefix_payload_block<'a, T: MapIndexRead<'a, str> + StrMapIndexPrefixRead>(
    index: &'a T,
    threshold: usize,
    key: &PayloadKeyType,
    f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
) -> OperationResult<()> {
    // HNSW build; hardware measurement intentionally bypassed (see above).
    let hw_counter = HardwareCounterCell::disposable();
    let Some(entries) = index.prefix_keys_with_counts("", &hw_counter)? else {
        return Ok(());
    };

    for (prefix, cardinality) in heavy_prefix_blocks(&entries, threshold) {
        f(PayloadBlockCondition {
            condition: FieldCondition::new_match(key.clone(), Match::new_prefix(&prefix)),
            cardinality,
        })?;
    }
    Ok(())
}

/// Enumerate the *smallest* heavy prefixes over sorted `(key, count)`
/// entries — the same principle as the geo index's `large_hashes`, which
/// emits only the deepest geohash regions above the threshold.
///
/// A prefix qualifies when it covers at least two keys and more postings
/// than the threshold, and it is emitted only if nothing heavy is nested
/// inside it: neither a longer qualifying prefix nor a single heavy value
/// (which already gets its own exact-match block). The emitted blocks are
/// therefore mutually disjoint and disjoint from the exact-value blocks —
/// no subgraph is built twice for nested subsets. Broader prefix queries
/// span several blocks, each internally navigable, exactly like a large
/// bounding box spans several geohash tile blocks.
///
/// All prefixes along a single-child trie chain select the same keys; each
/// chain collapses to its longest prefix — the longest common prefix (LCP)
/// of its key range.
///
/// Single streaming pass with a stack of open LCP intervals: an interval
/// opens when consecutive keys share a longer prefix and closes when the
/// shared length drops; totals and coverage propagate from closed intervals
/// into their parents. O(total key bytes).
fn heavy_prefix_blocks(entries: &[(EcoString, usize)], threshold: usize) -> Vec<(String, usize)> {
    struct OpenInterval {
        /// Prefix length (in bytes) shared by every key in the interval.
        lcp: usize,
        keys: usize,
        postings: usize,
        /// Whether something heavy nested in this interval already produced
        /// a block (a deeper interval or a single heavy value).
        covered: bool,
    }

    let lcp_len = |a: &str, b: &str| {
        a.as_bytes()
            .iter()
            .zip(b.as_bytes())
            .take_while(|(x, y)| x == y)
            .count()
    };

    // The byte-level LCP may end mid-codepoint; round down to a char boundary
    // to emit a valid prefix string. Rounding can collapse a node onto its
    // ancestor, so keep the larger count per emitted prefix.
    let mut heavy: HashMap<String, usize> = HashMap::new();
    // Returns whether the interval's subtree now contains an emitted block,
    // i.e. whether its ancestors must be suppressed.
    let mut close = |last_key: &str, node: &OpenInterval| -> bool {
        if node.keys < 2 || node.postings <= threshold {
            return false;
        }
        if node.covered {
            return true;
        }
        let mut boundary = node.lcp;
        while boundary > 0 && !last_key.is_char_boundary(boundary) {
            boundary -= 1;
        }
        if boundary == 0 {
            // Heavy but unrepresentable at this depth; let an ancestor with
            // a valid boundary produce the block instead.
            return false;
        }
        let entry = heavy.entry(last_key[..boundary].to_string()).or_default();
        *entry = (*entry).max(node.postings);
        true
    };

    let n = entries.len();
    let mut stack = vec![OpenInterval {
        lcp: 0,
        keys: 0,
        postings: 0,
        covered: false,
    }];
    for i in 0..n {
        let (key, count) = &entries[i];
        let lcp_left = if i == 0 {
            0
        } else {
            lcp_len(&entries[i - 1].0, key)
        };
        let lcp_right = if i + 1 == n {
            0
        } else {
            lcp_len(key, &entries[i + 1].0)
        };

        // Close intervals deeper than what the current key shares with its
        // predecessor: they ended at the previous key.
        let mut carry_keys = 0;
        let mut carry_postings = 0;
        let mut carry_covered = false;
        while stack.last().is_some_and(|top| top.lcp > lcp_left) {
            let mut node = stack.pop().unwrap();
            node.keys += carry_keys;
            node.postings += carry_postings;
            node.covered |= carry_covered;
            carry_covered = close(&entries[i - 1].0, &node);
            carry_keys = node.keys;
            carry_postings = node.postings;
        }
        // Merge the closed subtree's totals into its parent at `lcp_left`.
        let top = stack.last_mut().unwrap();
        if top.lcp == lcp_left {
            top.keys += carry_keys;
            top.postings += carry_postings;
            top.covered |= carry_covered;
        } else {
            stack.push(OpenInterval {
                lcp: lcp_left,
                keys: carry_keys,
                postings: carry_postings,
                covered: carry_covered,
            });
        }
        // A deeper interval opens if the next key shares more than the
        // previous one did.
        if lcp_right > lcp_left {
            stack.push(OpenInterval {
                lcp: lcp_right,
                keys: 0,
                postings: 0,
                covered: false,
            });
        }
        // The key itself belongs to the deepest open interval. A single
        // heavy value gets its own exact-match block, which counts as
        // covering every prefix above it.
        let top = stack.last_mut().unwrap();
        top.keys += 1;
        top.postings += count;
        top.covered |= *count > threshold;
    }
    // Close everything still open; those intervals end at the last key.
    let mut carry_keys = 0;
    let mut carry_postings = 0;
    let mut carry_covered = false;
    while let Some(mut node) = stack.pop() {
        node.keys += carry_keys;
        node.postings += carry_postings;
        node.covered |= carry_covered;
        carry_covered = match entries.last() {
            Some((last_key, _)) => close(last_key, &node),
            None => false,
        };
        carry_keys = node.keys;
        carry_postings = node.postings;
    }

    let mut blocks: Vec<(String, usize)> = heavy.into_iter().collect();
    blocks.sort_unstable_by(|(prefix_a, count_a), (prefix_b, count_b)| {
        count_b.cmp(count_a).then_with(|| prefix_a.cmp(prefix_b))
    });
    blocks
}

fn condition_checker_impl<'a, T: MapIndexRead<'a, str> + 'a>(
    index: &'a T,
    condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
) -> Option<MapConditionChecker<'a, str, T>> {
    // Destructure explicitly (no `..`) so a new field added to
    // `FieldCondition` forces this method to be revisited.
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let cond_match = r#match.as_ref()?;
    let hw_counter = hw_acc.get_counter_cell();
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::String(keyword),
        }) => Some(index.match_value_checker(hw_counter, keyword.clone())),
        Match::Any(MatchAny {
            any: AnyVariants::Strings(list),
        }) => Some(index.match_any_checker(hw_counter, list.clone(), false)),
        Match::Except(MatchExcept {
            except: AnyVariants::Strings(list),
        }) => Some(index.match_any_checker(hw_counter, list.clone(), true)),
        // Served through the forward index; works with or without the prefix
        // structures, which only accelerate `filter`/`estimate_cardinality`.
        Match::Prefix(MatchPrefix { prefix }) => {
            Some(index.match_prefix_checker(hw_counter, prefix.as_str()))
        }
        // Conditions this index can't serve: Match::Text/TextAny/Phrase
        // (handled by FullTextIndex) and value-type mismatches (e.g.
        // Match::Value(Integer) against a string-keyed map).
        Match::Value(MatchValue {
            value: ValueVariants::Integer(_) | ValueVariants::Bool(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Integers(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Integers(_),
        })
        | Match::Text(_)
        | Match::TextAny(_)
        | Match::Phrase(_) => None,
    }
}
