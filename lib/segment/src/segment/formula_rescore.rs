use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::slice::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::ScoredPointOffset;
use itertools::Itertools;

use super::Segment;
use crate::common::operation_error::OperationResult;
use crate::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::types::ScoredPoint;

impl Segment {
    /// Rescores points of the prefetches, and returns the internal ids with the scores.
    pub(super) fn do_rescore_with_formula(
        &self,
        formula: &ParsedFormula,
        prefetches_scores: &[Vec<ScoredPoint>],
        wrapped_deleted: Option<&BitSlice>,
        limit: usize,
        is_stopped: &AtomicBool,
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
                    .filter_map(|point| {
                        // Discard points without internal ids
                        let internal_id = self.get_internal_id(point.id)?;

                        // Discard points that are marked as deleted in a wrapped segment
                        if let Some(true) = wrapped_deleted.and_then(|slice| {
                            slice
                                .get(internal_id as usize)
                                .map(|is_deleted| *is_deleted)
                        }) {
                            return None;
                        }

                        // filter_map side effect: keep all uniquely seen point offsets.
                        points_to_rescore.insert(internal_id);

                        Some((internal_id, point.score))
                    })
                    .collect::<HashMap<_, _>>()
            })
            .collect::<Vec<_>>();

        let index_ref = self.payload_index.borrow();
        let scorer = index_ref.formula_scorer(formula, &prefetches_scores, hw_counter);

        // Perform rescoring
        let mut errs = Vec::new();
        let rescored = points_to_rescore
            .into_iter()
            .check_stop(|| is_stopped.load(Ordering::Relaxed))
            .filter_map(|internal_id| {
                match scorer.score(internal_id) {
                    Ok(new_score) => Some(ScoredPointOffset {
                        idx: internal_id,
                        score: new_score,
                    }),
                    Err(err) => {
                        // in case there is an error, defer handling it and continue
                        errs.push(err);
                        is_stopped.store(true, Ordering::Relaxed);
                        None
                    }
                }
            })
            // Keep only the top k results
            .k_largest(limit)
            .collect();

        if let Some(err) = errs.pop() {
            return Err(err);
        }

        Ok(rescored)
    }
}
