use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use common::ambient;
use segment::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use segment::data_types::query_context::QueryContext;
use segment::types::VectorName;

use crate::common::stopping_guard::StoppingGuard;
use crate::locked_segment::LockedSegment;
use crate::search::CoreSearchRequest;

pub fn init_query_context(
    batch_request: &[CoreSearchRequest],
    // How many KBs segment should have to be considered requiring indexing for search
    search_optimized_threshold_kb: usize,
    is_stopped_guard: &StoppingGuard,
    check_idf_required: impl Fn(&VectorName) -> bool,
) -> OperationResult<QueryContext> {
    init_query_context_with_stopping_flag(
        batch_request,
        search_optimized_threshold_kb,
        is_stopped_guard.get_is_stopped(),
        check_idf_required,
    )
}

/// Initialize a query context sharing a caller-owned cancellation flag.
pub fn init_query_context_with_stopping_flag(
    batch_request: &[CoreSearchRequest],
    search_optimized_threshold_kb: usize,
    is_stopped: Arc<AtomicBool>,
    check_idf_required: impl Fn(&VectorName) -> bool,
) -> OperationResult<QueryContext> {
    let mut query_context = QueryContext::new(search_optimized_threshold_kb, ambient::current())
        .with_is_stopped(is_stopped.clone());

    for search_request in batch_request {
        check_process_stopped(&is_stopped)?;
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

    check_process_stopped(&is_stopped)?;
    Ok(query_context)
}

/// Fill one segment's contribution into a [`QueryContext::fork_statistics`]
/// copy of `query_context`, to fold back with
/// [`QueryContext::merge_statistics`]. Segments fill in parallel this way.
pub fn fill_segment_query_context(
    query_context: &QueryContext,
    segment: &LockedSegment,
    timeout: Duration,
    is_stopped: &AtomicBool,
) -> OperationResult<QueryContext> {
    check_process_stopped(is_stopped)?;
    let mut fork = query_context.fork_statistics();
    let segment = segment.get();
    let Some(segment_guard) = segment.try_read_for(timeout) else {
        return Err(OperationError::timeout(timeout, "fill query context"));
    };
    segment_guard.fill_query_context(&mut fork)?;
    Ok(fork)
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
        ambient::test(|| init_query_context(batch, 0, &StoppingGuard::new(), check_idf_required))
    }

    #[test]
    fn caller_owned_cancellation_reaches_segment_query_context() {
        let stopped = Arc::new(AtomicBool::new(false));
        let _scope = ambient::test_guard();
        let context =
            init_query_context_with_stopping_flag(&[], 0, stopped.clone(), |_| false).unwrap();
        assert!(Arc::ptr_eq(&context.is_stopped_handle(), &stopped));
        assert!(!context.get_segment_query_context().is_stopped());
        stopped.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(context.get_segment_query_context().is_stopped());
        assert!(matches!(
            init_query_context_with_stopping_flag(&[], 0, stopped.clone(), |_| false),
            Err(OperationError::Cancelled { .. })
        ));
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
