use std::collections::{HashMap, HashSet};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::ScoredPointOffset;
use itertools::Itertools;

use super::Segment;
use crate::common::operation_error::OperationResult;
use crate::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::types::{Order, ScoredPoint};

impl Segment {
    /// Rescores points of the prefetches, and returns the internal ids with the scores.
    pub(super) fn do_rescore_with_formula(
        &self,
        formula: &ParsedFormula,
        prefetches_scores: &[Vec<ScoredPoint>],
        order: Order,
        limit: usize,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        // Dedup point offsets into a hashset
        let mut points_to_rescore =
            HashSet::with_capacity(prefetches_scores.first().map_or(0, |scores| scores.len()));

        // Transform prefetches results into a hashmap for faster lookup,
        let prefetches_scores = prefetches_scores
            .iter()
            .map(|scores| {
                scores
                    .iter()
                    // Discard points without internal ids
                    .filter_map(|point| {
                        let internal_id = self.get_internal_id(point.id)?;

                        // filter_map side effect: keep all uniquely seen point offsets.
                        points_to_rescore.insert(internal_id);

                        Some((internal_id, point.score))
                    })
                    .collect::<HashMap<_, _>>()
            })
            .collect::<Vec<_>>();

        let index_ref = self.payload_index.borrow();
        let scorer = index_ref.formula_scorer(formula, &prefetches_scores, hw_counter);

        let rescored_points: Vec<_> = points_to_rescore
            .into_iter()
            // TODO(score boosting): Parallelize scoring?
            .map(|internal_id| {
                let score = scorer.score(internal_id)?;
                OperationResult::Ok(ScoredPointOffset {
                    idx: internal_id,
                    score,
                })
            })
            .try_collect()?;

        let res = match order {
            Order::LargeBetter => rescored_points.into_iter().k_largest(limit).collect(),
            Order::SmallBetter => rescored_points.into_iter().k_smallest(limit).collect(),
        };
        Ok(res)
    }
}
