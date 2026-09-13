use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::{DeferredBehavior, ScoreType, ScoredPointOffset};
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
                // The batched resolution delivers pairs in read-completion order, so
                // the score is carried by external id rather than by input position.
                let scores_by_external: AHashMap<_, _> =
                    scores.iter().map(|point| (point.id, point.score)).collect();

                let mut scores_by_internal = AHashMap::with_capacity(scores_by_external.len());

                // Points without internal ids are discarded: rescoring a prefetch
                // must resolve the same visible head the search saw.
                self.id_tracker.resolve_external_ids(
                    scores_by_external.keys().copied(),
                    DeferredBehavior::VisibleOnly,
                    |external_id, internal_id| {
                        let Some(&score) = scores_by_external.get(&external_id) else {
                            // Resolved from this map's own keys, so it always hits.
                            return;
                        };

                        // callback side effect: keep all uniquely seen point offsets.
                        points_to_rescore.insert(internal_id);

                        scores_by_internal.insert(internal_id, score);
                    },
                )?;

                OperationResult::Ok(scores_by_internal)
            })
            .collect::<OperationResult<Vec<_>>>()?;

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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use tempfile::Builder;

    use crate::data_types::named_vectors::NamedVectors;
    use crate::data_types::query_context::FormulaContext;
    use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorInternal};
    use crate::entry::entry_point::{
        NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    };
    use crate::index::query_optimization::rescore_formula::parsed_formula::{
        ParsedExpression, ParsedFormula,
    };
    use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
    use crate::types::{Distance, PointIdType, ScoredPoint};

    fn scored(id: u64, score: f32) -> ScoredPoint {
        ScoredPoint {
            id: PointIdType::NumId(id),
            version: 0,
            score,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    /// The prefetch scores must land on the point they were scored for, and ids the
    /// segment cannot resolve must be dropped.
    #[test]
    fn rescore_keeps_every_prefetch_score_with_its_own_point() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
        let hw_counter = HardwareCounterCell::new();

        for id in [1, 2, 3] {
            let mut vectors = NamedVectors::default();
            vectors.insert(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorInternal::Dense(vec![id as f32, 0.0]),
            );
            segment
                .upsert_point(id, PointIdType::NumId(id), vectors, &hw_counter)
                .unwrap();
        }
        segment
            .delete_point(10, PointIdType::NumId(3), &hw_counter)
            .unwrap();

        let ctx = FormulaContext {
            // The rescored score is the prefetch score itself.
            formula: ParsedFormula {
                payload_vars: HashSet::new(),
                conditions: Vec::new(),
                defaults: HashMap::new(),
                formula: ParsedExpression::new_score_id(0),
            },
            prefetches_results: vec![vec![
                scored(1, 10.0),
                scored(2, 20.0),
                // Deleted, and never inserted: neither resolves.
                scored(3, 30.0),
                scored(99, 99.0),
            ]],
            limit: 10,
            score_threshold: None,
            is_stopped: Arc::new(AtomicBool::new(false)),
        };

        let rescored = segment
            .rescore_with_formula(Arc::new(ctx), &hw_counter)
            .unwrap();

        let scores: Vec<_> = rescored
            .into_iter()
            .map(|point| (point.id, point.score))
            .collect();
        assert_eq!(
            scores,
            vec![(PointIdType::NumId(2), 20.0), (PointIdType::NumId(1), 10.0),],
        );
    }
}
