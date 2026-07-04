use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::iterator_ext::IteratorExt;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::idf_estimate::IdfStats;
use segment::data_types::query_context::{IdfScopeStats, QueryContext};
use segment::types::{Filter, VectorName};
use sparse::common::types::DimId;

use crate::common::stopping_guard::StoppingGuard;
use crate::search::CoreSearchRequest;
use crate::segment_holder::locked::LockedSegmentHolder;

pub fn init_query_context(
    batch_request: &[CoreSearchRequest],
    // How many KBs segment should have to be considered requiring indexing for search
    search_optimized_threshold_kb: usize,
    is_stopped_guard: &StoppingGuard,
    hw_measurement_acc: HwMeasurementAcc,
    check_idf_required: impl Fn(&VectorName) -> bool,
) -> OperationResult<QueryContext> {
    let mut query_context = QueryContext::new(search_optimized_threshold_kb, hw_measurement_acc)
        .with_is_stopped(is_stopped_guard.get_is_stopped());

    for search_request in batch_request {
        let idf_params = search_request
            .params
            .as_ref()
            .and_then(|params| params.idf.as_ref());

        // The `idf` search param changes scoring, so silently ignoring it
        // when it cannot apply would be misleading.
        if idf_params.is_some() && !check_idf_required(search_request.query.get_vector_name()) {
            return Err(OperationError::validation_error(format!(
                "search param `idf` requires a sparse vector with the `idf` modifier, \
                 which vector {:?} is not",
                search_request.query.get_vector_name(),
            )));
        }

        let idf_corpus = idf_params.and_then(|idf| idf.corpus());

        search_request
            .query
            .iterate_sparse(|vector_name, sparse_vector| {
                if check_idf_required(vector_name) {
                    query_context.init_idf(vector_name, idf_corpus, &sparse_vector.indices);
                }
            })
    }

    Ok(query_context)
}

pub fn fill_query_context(
    mut query_context: QueryContext,
    segments: LockedSegmentHolder,
    timeout: Duration,
    is_stopped: &AtomicBool,
) -> OperationResult<Option<QueryContext>> {
    let start = std::time::Instant::now();

    let segments: Vec<_> = {
        let Some(holder_guard) = segments.try_read_for(timeout) else {
            return Err(OperationError::timeout(timeout, "fill query context"));
        };
        holder_guard
            .non_appendable_then_appendable_segments()
            .collect()
    };

    if segments.is_empty() {
        return Ok(None);
    }

    for locked_segment in segments.into_iter().stop_if(is_stopped) {
        let segment = locked_segment.get();
        let timeout = timeout.saturating_sub(start.elapsed());
        let Some(segment_guard) = segment.try_read_for(timeout) else {
            return Err(OperationError::timeout(timeout, "fill query context"));
        };
        segment_guard.fill_query_context(&mut query_context)?;
    }
    Ok(Some(query_context))
}

/// Collect raw IDF statistics — the document count and per-term document
/// frequencies — for the given sparse vector over the segments of a shard.
///
/// `corpus` defines the population the statistics are computed over,
/// `None` for the whole collection.
pub fn collect_idf_stats(
    segments: LockedSegmentHolder,
    vector_name: &VectorName,
    indices: &[DimId],
    corpus: Option<&Filter>,
    timeout: Duration,
    is_stopped: Arc<AtomicBool>,
    hw_measurement_acc: HwMeasurementAcc,
) -> OperationResult<IdfStats> {
    // The search-optimized threshold does not affect statistics collection.
    let mut query_context =
        QueryContext::new(usize::MAX, hw_measurement_acc).with_is_stopped(is_stopped);
    query_context.init_idf(vector_name, corpus, indices);

    let is_stopped = query_context.is_stopped_handle();
    let query_context = fill_query_context(query_context, segments, timeout, &is_stopped)?;

    // `init_idf` seeded a zero count for every query term, so terms missing
    // from the shard report a zero frequency. A shard without segments
    // contributes empty statistics.
    let mut document_frequency: HashMap<DimId, usize> =
        indices.iter().map(|&index| (index, 0)).collect();
    let mut document_count = 0;

    if let Some(mut query_context) = query_context
        && let Some(scope) = query_context.mut_idf_stats().take_scope(corpus)
    {
        let IdfScopeStats {
            corpus: _,
            mut idf,
            indexed_vectors,
        } = scope;
        if let Some(df) = idf.remove(vector_name) {
            document_frequency = df;
        }
        document_count = indexed_vectors.get(vector_name).copied().unwrap_or(0);
    }

    Ok(IdfStats {
        document_count,
        document_frequency,
    })
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::json_path::JsonPath;
    use segment::types::{
        Condition, FieldCondition, Filter, IdfCorpusParams, IdfParams, IdfScope, SearchParams,
    };
    use sparse::common::sparse_vector::SparseVector;

    use super::*;
    use crate::query::query_enum::QueryEnum;

    fn sparse_request(idf: Option<IdfParams>) -> CoreSearchRequest {
        let sparse_vector = SparseVector::new(vec![0, 2], vec![1.0, 1.0]).unwrap();
        CoreSearchRequest {
            query: QueryEnum::Nearest(NamedQuery {
                query: VectorInternal::Sparse(sparse_vector),
                using: Some("sparse".to_owned()),
            }),
            filter: None,
            params: idf.map(|idf| SearchParams {
                idf: Some(idf),
                ..Default::default()
            }),
            limit: 10,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        }
    }

    fn init(
        batch: &[CoreSearchRequest],
        check_idf_required: impl Fn(&segment::types::VectorName) -> bool,
    ) -> OperationResult<QueryContext> {
        init_query_context(
            batch,
            0,
            &StoppingGuard::new(),
            HwMeasurementAcc::new(),
            check_idf_required,
        )
    }

    #[test]
    fn init_query_context_idf_scopes() {
        let corpus = Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new("tenant"),
            "acme".to_string().into(),
        )));

        // A batch mixing global and corpus-scoped IDF gets one statistics
        // scope per distinct corpus.
        let batch = [
            sparse_request(None),
            sparse_request(Some(IdfParams::Scope(IdfScope::Global))),
            sparse_request(Some(IdfParams::Corpus(IdfCorpusParams {
                corpus: corpus.clone(),
            }))),
        ];
        let context = init(&batch, |_| true).unwrap();
        assert_eq!(context.idf_stats().scopes.len(), 2);
        assert!(context.idf_stats().scope(None).is_some());
        assert!(context.idf_stats().scope(Some(&corpus)).is_some());

        // The `idf` param requires a vector with the IDF modifier; scoring
        // params must not be silently ignored.
        let batch = [sparse_request(Some(IdfParams::Scope(IdfScope::Global)))];
        init(&batch, |_| false).unwrap_err();

        // Without the param, non-IDF vectors initialize no scopes at all.
        let batch = [sparse_request(None)];
        let context = init(&batch, |_| false).unwrap();
        assert!(context.idf_stats().scopes.is_empty());
    }
}
