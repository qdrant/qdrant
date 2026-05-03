use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::{ScoreType, ScoredPointOffset};
use itertools::{Either, Itertools};

use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::FormulaContext;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::ScoredPoint;

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Rescores points of the prefetches and returns the internal ids with the
    /// scores.
    fn do_rescore_with_formula(
        &self,
        formula: &ParsedFormula,
        prefetches_scores: &[Vec<ScoredPoint>],
        limit: usize,
        score_threshold: Option<ScoreType>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        // Dedup point offsets into a hashset.
        let mut points_to_rescore =
            AHashSet::with_capacity(prefetches_scores.first().map_or(0, |scores| scores.len()));

        // Transform prefetches results into a hashmap for faster lookup.
        let prefetches_scores = prefetches_scores
            .iter()
            .map(|scores| {
                scores
                    .iter()
                    .filter_map(|point| {
                        // Discard points without internal ids.
                        let internal_id = self.id_tracker.internal_id(point.id)?;

                        // filter_map side effect: keep all uniquely seen point offsets.
                        points_to_rescore.insert(internal_id);

                        Some((internal_id, point.score))
                    })
                    .collect::<AHashMap<_, _>>()
            })
            .collect::<Vec<_>>();

        let scorer = self
            .payload_index
            .formula_scorer(formula, &prefetches_scores, hw_counter)?;

        // Perform rescoring.
        let mut error = None;
        let rescored_iter = points_to_rescore
            .into_iter()
            .stop_if(is_stopped)
            .filter_map(|internal_id| {
                match scorer.score(internal_id) {
                    Ok(new_score) => Some(ScoredPointOffset {
                        idx: internal_id,
                        score: new_score,
                    }),
                    Err(err) => {
                        // In case there is an error, defer handling it and continue.
                        error = Some(err);
                        is_stopped.store(true, Ordering::Relaxed);
                        None
                    }
                }
            });

        // Handle score threshold.
        let rescored = match score_threshold {
            Some(threshold) => {
                Either::Left(rescored_iter.filter(move |point| point.score >= threshold))
            }
            None => Either::Right(rescored_iter),
        }
        .k_largest(limit) // Keep only the top k results.
        .collect();

        if let Some(err) = error {
            return Err(err);
        }

        Ok(rescored)
    }

    pub fn rescore_with_formula(
        &self,
        ctx: Arc<FormulaContext>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let FormulaContext {
            formula,
            prefetches_results,
            limit,
            score_threshold,
            is_stopped,
        } = &*ctx;

        let internal_results = self.do_rescore_with_formula(
            formula,
            prefetches_results,
            *limit,
            *score_threshold,
            is_stopped,
            hw_counter,
        )?;

        self.process_search_result(
            internal_results,
            &false.into(),
            &false.into(),
            hw_counter,
            is_stopped,
        )
    }
}
