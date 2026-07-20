use std::collections::HashMap;
use std::num::NonZeroU32;

use ahash::AHashSet;
use api::rest::RecommendStrategy;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::reciprocal_rank_fusion::DEFAULT_RRF_K;
use segment::data_types::facets::{FacetParams, FacetValue};
use segment::data_types::order_by::{Direction, OrderBy, OrderByInterface, OrderValue};
use segment::data_types::vectors::{
    MultiDenseVectorInternal, NamedQuery, VectorInternal, VectorStructInternal,
};
use segment::types::{
    PointIdType, SearchParams, Slice, VectorNameBuf, WithPayload, WithPayloadInterface, WithVector,
};
use shard::query::query_enum::QueryEnum;
use shard::query::{FusionInternal, ScoringQuery, ShardPrefetch, ShardQueryRequest};
use shard::scroll::ScrollRequestInternal;

use super::super::op::{
    FusionKind, NamedVectors, Prefetch, ScrollFilter, canonical_sparse, dense_diff, dense_matches,
    has_num, match_has_id_filter, match_has_vector_filter, match_num_filter, match_slice_filter,
    match_tag_filter, match_url_prefix_filter, num_matches, optional_read_filter,
    passes_read_filters, tag_matches, url_prefix_matches,
};
use super::super::{Model, VectorValue};
use crate::collection::Collection;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    CoreSearchRequest, CountRequestInternal, PointRequestInternal, RecommendExample,
    RecommendRequestInternal, UsingVector,
};
use crate::recommendations::recommend_by;

/// Assert two id sets are equal; on failure, panic with only the symmetric difference
/// instead of dumping both full sets (much friendlier when sets have hundreds of entries).
fn assert_id_sets_eq(
    returned: &AHashSet<PointIdType>,
    expected: &AHashSet<PointIdType>,
    ctx: &str,
) {
    if returned == expected {
        return;
    }
    let missing: Vec<_> = expected.difference(returned).copied().collect();
    let extra: Vec<_> = returned.difference(expected).copied().collect();
    panic!(
        "{ctx}: id-set mismatch (returned {} ids, expected {}); \
         missing from returned: {missing:?}; \
         extra in returned: {extra:?}",
        returned.len(),
        expected.len(),
    );
}

/// Compare a returned `HashMap<VectorName, VectorInternal>` against the model's `NamedVectors`.
/// `ctx` is the originating op name (e.g. "RetrieveRandom" / "RetrieveExisting"), prefixed
/// into every assertion message so failures point at the exact op variant.
fn assert_named_vectors_match(
    returned: &HashMap<VectorNameBuf, VectorInternal>,
    expected: &NamedVectors,
    id: PointIdType,
    ctx: &str,
) {
    // Returned names should be a subset of expected (vectors may have been deleted from a point;
    // the engine returns whatever is currently present).
    for (name, ret_vec) in returned {
        let exp = expected.get(name).unwrap_or_else(|| {
            panic!("{ctx} for {id:?} returned vector `{name}` that the model doesn't have")
        });
        match (ret_vec, exp) {
            (VectorInternal::Dense(a), VectorValue::Dense(b)) => {
                assert!(
                    dense_matches(name, a, b),
                    "{ctx}: dense vector `{name}` value divergence for id {id:?}: \
                     engine {a:?}, model {b:?}; {}",
                    dense_diff(a, b),
                );
            }
            (VectorInternal::Sparse(a), VectorValue::Sparse(b)) => {
                assert_eq!(
                    &canonical_sparse(a),
                    b,
                    "{ctx}: sparse vector `{name}` mismatch for id {id:?}",
                );
            }
            (VectorInternal::MultiDense(a), VectorValue::MultiDense(b)) => {
                // Internal stores `{ flattened_vectors, dim }`; un-flatten into rows for
                // direct comparison against the model's `Vec<Vec<f32>>`.
                assert!(
                    a.dim > 0 && a.flattened_vectors.len() % a.dim == 0,
                    "{ctx}: multi-dense vector `{name}` has malformed internal shape for id {id:?}",
                );
                let returned_rows: Vec<Vec<f32>> = a
                    .flattened_vectors
                    .chunks(a.dim)
                    .map(<[f32]>::to_vec)
                    .collect();
                assert_eq!(
                    &returned_rows, b,
                    "{ctx}: multi-dense vector `{name}` mismatch for id {id:?}",
                );
            }
            (other_a, other_b) => panic!(
                "{ctx}: vector type mismatch for `{name}` id {id:?}: engine returned {other_a:?}, \
                 model has {other_b:?}",
            ),
        }
    }
    // Each model-side vector should also appear in the returned map.
    for name in expected.keys() {
        assert!(
            returned.contains_key(name),
            "{ctx} for {id:?} is missing vector `{name}` that the model has",
        );
    }
}

pub(super) async fn apply_retrieve(
    collection: &Collection,
    model: &Model,
    ids: &[PointIdType],
    ctx: &str,
) {
    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: ids.to_vec(),
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: WithVector::Bool(true),
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{ctx} failed: {e:?}"));
    let returned: AHashSet<PointIdType> = records.iter().map(|r| r.id).collect();
    for id in ids {
        let in_model = model.contains_key(id);
        let in_returned = returned.contains(id);
        assert_eq!(
            in_model, in_returned,
            "{ctx} membership mismatch for id {id:?}: model={in_model}, returned={in_returned}"
        );
    }
    for record in &records {
        let entry = model
            .get(&record.id)
            .unwrap_or_else(|| panic!("{ctx} returned unknown id {:?}", record.id));
        let named = match record.vector.as_ref().expect("retrieve vector missing") {
            VectorStructInternal::Named(m) => m,
            other @ (VectorStructInternal::Single(_) | VectorStructInternal::MultiDense(_)) => {
                panic!("{ctx}: expected Named vector struct, got {other:?}")
            }
        };
        assert_named_vectors_match(named, &entry.vectors, record.id, ctx);
        let actual_payload = record.payload.clone().unwrap_or_default();
        assert_eq!(
            &actual_payload, &entry.payload,
            "{ctx} payload mismatch for id {:?}",
            record.id,
        );
    }
}

/// Retrieve with `with_payload`/`with_vector` *selectors* and assert the engine returns exactly
/// the selected subset of payload + vectors for each (existing) id.
pub(super) async fn apply_retrieve_selective(
    collection: &Collection,
    model: &Model,
    ids: &[PointIdType],
    with_payload: &WithPayloadInterface,
    with_vector: &WithVector,
) {
    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: ids.to_vec(),
                with_payload: Some(with_payload.clone()),
                with_vector: with_vector.clone(),
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("RetrieveSelective failed");

    let returned: AHashSet<PointIdType> = records.iter().map(|r| r.id).collect();
    for id in ids {
        let in_model = model.contains_key(id);
        let in_returned = returned.contains(id);
        assert_eq!(
            in_model, in_returned,
            "RetrieveSelective membership mismatch for id {id:?}: model={in_model}, returned={in_returned}"
        );
    }

    let payload_enabled = with_payload.is_required();
    // Reuse the engine's own selector processing so the expected payload matches exactly,
    // regardless of include/exclude pattern semantics.
    let payload_proc = WithPayload::from(with_payload.clone());

    for record in &records {
        let entry = model
            .get(&record.id)
            .unwrap_or_else(|| panic!("RetrieveSelective returned unknown id {:?}", record.id));

        // ── payload ──
        if payload_enabled {
            let mut expected = entry.payload.clone();
            if let Some(selector) = &payload_proc.payload_selector {
                expected = selector.process(expected);
            }
            let actual = record.payload.clone().unwrap_or_default();
            assert_eq!(
                actual, expected,
                "RetrieveSelective payload mismatch for id {:?} (with_payload={with_payload:?})",
                record.id,
            );
        } else {
            assert!(
                record.payload.is_none(),
                "RetrieveSelective returned payload for id {:?} despite with_payload={with_payload:?}",
                record.id,
            );
        }

        // ── vectors ──
        match with_vector {
            WithVector::Bool(false) => {
                assert!(
                    record.vector.is_none(),
                    "RetrieveSelective returned vectors for id {:?} despite with_vector=false",
                    record.id,
                );
            }
            WithVector::Bool(true) => {
                let named = expect_named(record, "RetrieveSelective");
                assert_named_vectors_match(named, &entry.vectors, record.id, "RetrieveSelective");
            }
            WithVector::Selector(names) => {
                // Expected = the requested names that the point actually has populated.
                let expected: NamedVectors = entry
                    .vectors
                    .iter()
                    .filter(|(name, _)| names.contains(*name))
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect();
                if expected.is_empty() {
                    // Engine returns no vector struct (or an empty one) when none of the
                    // requested names are present on the point.
                    if let Some(VectorStructInternal::Named(m)) = record.vector.as_ref() {
                        assert!(
                            m.is_empty(),
                            "RetrieveSelective(selector) returned {:?} for id {:?} but model expected none",
                            m.keys().collect::<Vec<_>>(),
                            record.id,
                        );
                    }
                } else {
                    let named = expect_named(record, "RetrieveSelective");
                    assert_named_vectors_match(
                        named,
                        &expected,
                        record.id,
                        "RetrieveSelective(selector)",
                    );
                }
            }
        }
    }
}

/// Extract the `Named` vector struct from a record, panicking on the unexpected single/multi forms.
fn expect_named<'a>(
    record: &'a shard::retrieve::record_internal::RecordInternal,
    ctx: &str,
) -> &'a HashMap<VectorNameBuf, VectorInternal> {
    match record.vector.as_ref().expect("retrieve vector missing") {
        VectorStructInternal::Named(m) => m,
        other @ (VectorStructInternal::Single(_) | VectorStructInternal::MultiDense(_)) => {
            panic!("{ctx}: expected Named vector struct, got {other:?}")
        }
    }
}

/// The model-side candidate set + size expectation shared by `Search` and `Query`: a point is a
/// candidate iff it has the queried vector populated *and* passes the optional `num` filter; the
/// engine should return at most `limit` of them.
fn nearest_candidates(
    model: &Model,
    vector_name: &str,
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
    limit: usize,
) -> (AHashSet<PointIdType>, usize) {
    let candidates: AHashSet<PointIdType> = model
        .iter()
        .filter(|(_, entry)| entry.vectors.contains_key(vector_name))
        .filter(|(_, entry)| passes_read_filters(&entry.payload, filter_num, filter_url_prefix))
        .map(|(id, _)| *id)
        .collect();
    let expected_len = limit.min(candidates.len());
    (candidates, expected_len)
}

/// Whether a nearest query should match the candidate set *exactly* (top-k) rather than being a
/// mere upper bound. Only an exact scan over dense / multi-dense vectors qualifies:
/// - dense `exact=true`: exact scan → exactly top-k;
/// - dense `exact=false` (HNSW): recall < 1.0 or a restrictive filter may return fewer;
/// - sparse: the effective candidate set is "points with non-zero overlap", data-dependent and
///   smaller, so it's only ever an upper bound;
/// - multi-dense (MaxSim) over an exact scan returns every candidate, so it joins the strict path.
fn nearest_is_strict(exact: bool, query: &VectorValue) -> bool {
    exact && matches!(query, VectorValue::Dense(_) | VectorValue::MultiDense(_))
}

/// Per-result membership invariants shared by `Search` and `Query`: every returned id must exist
/// in the model, have the queried vector populated, and (when filtering) match the `num` filter.
/// `label` is the originating op name, prefixed into failure messages.
fn assert_nearest_results_valid(
    results_ids: impl IntoIterator<Item = PointIdType>,
    model: &Model,
    vector_name: &str,
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
    label: &str,
) {
    for id in results_ids {
        let entry = model
            .get(&id)
            .unwrap_or_else(|| panic!("{label} returned unknown id {id:?}"));
        assert!(
            entry.vectors.contains_key(vector_name),
            "{label} returned id {id:?} that doesn't have vector `{vector_name}`",
        );
        assert!(
            passes_read_filters(&entry.payload, filter_num, filter_url_prefix),
            "{label}(filter num={filter_num:?}, url_prefix={filter_url_prefix:?}) returned id {id:?} that does not match",
        );
    }
}

/// The full strict/upper-bound size check shared by `Search` and `Query`, including the rich
/// diagnostic probes (retrieve / count / scroll / retry) emitted on a strict-mode mismatch.
///
/// `label` distinguishes the originating API in messages. `retry_ids` is the id set from a second
/// run of the *same* API the caller just issued (so the API-specific request stays at the call
/// site); it's used only to report whether a mismatch is deterministic or flapping.
#[allow(clippy::too_many_arguments)]
async fn assert_nearest_size_invariant(
    collection: &Collection,
    label: &str,
    returned_ids: &AHashSet<PointIdType>,
    retry_ids: &AHashSet<PointIdType>,
    candidates: &AHashSet<PointIdType>,
    expected_len: usize,
    strict: bool,
    vector_name: &str,
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
    limit: usize,
) {
    if !strict {
        assert!(
            returned_ids.len() <= expected_len,
            "{label}(approx, name={vector_name}, filter_num={filter_num:?}, filter_url_prefix={filter_url_prefix:?}) returned {} points, expected at most {expected_len}",
            returned_ids.len(),
        );
        return;
    }
    if returned_ids.len() == expected_len {
        return;
    }

    let mut missing: Vec<_> = candidates.difference(returned_ids).copied().collect();
    let mut extra: Vec<_> = returned_ids.difference(candidates).copied().collect();
    missing.sort();
    extra.sort();

    // Follow-up probes before panicking — classify the bug.
    //   1. Does Retrieve see the missing point(s)?
    //   2. Does CountByFilter see the right count?
    //   3. Does scroll(filter) agree with the model's candidate set?
    //   4. Did the retry give the same wrong answer (consistent vs flapping)?
    let probe_retrieve = collection
        .retrieve(
            PointRequestInternal {
                ids: missing.clone(),
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: WithVector::Bool(true),
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await;
    let retrieved_summary = match probe_retrieve {
        Ok(records) => records
            .iter()
            .map(|r| {
                let has_vec =
                    r.vector
                        .as_ref()
                        .and_then(|v| match v {
                            VectorStructInternal::Named(m) => Some(m.contains_key(vector_name)),
                            VectorStructInternal::Single(_)
                            | VectorStructInternal::MultiDense(_) => None,
                        })
                        .unwrap_or(false);
                let has_num = r
                    .payload
                    .as_ref()
                    .is_some_and(|p| passes_read_filters(p, filter_num, filter_url_prefix));
                format!("{:?}{{vec:{has_vec}, filter_match:{has_num}}}", r.id)
            })
            .collect::<Vec<_>>()
            .join(", "),
        Err(e) => format!("retrieve failed: {e:?}"),
    };

    let probe_count = collection
        .count(
            CountRequestInternal {
                filter: optional_read_filter(filter_num, filter_url_prefix),
                exact: true,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await;
    let count_summary = match probe_count {
        Ok(c) => format!("{}", c.count),
        Err(e) => format!("err: {e:?}"),
    };

    // Scroll by the same filter — gives us the exact id list the engine thinks matches.
    // If this set differs from the model's `candidates`, the payload index is out of sync.
    let probe_scroll = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(usize::MAX),
                filter: optional_read_filter(filter_num, filter_url_prefix),
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(false),
                order_by: None,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await;
    let scroll_summary = match probe_scroll {
        Ok(s) => {
            let mut engine_ids: Vec<PointIdType> = s.points.iter().map(|r| r.id).collect();
            engine_ids.sort();
            let engine_set: AHashSet<_> = engine_ids.iter().copied().collect();
            let mut ghost: Vec<_> = engine_set.difference(candidates).copied().collect();
            let mut missing_from_engine: Vec<_> =
                candidates.difference(&engine_set).copied().collect();
            ghost.sort();
            missing_from_engine.sort();
            format!(
                "engine has {} ids matching filter; ghost (engine but not model): {ghost:?}; \
                 missing from engine (in model but not engine scroll): {missing_from_engine:?}",
                engine_ids.len(),
            )
        }
        Err(e) => format!("err: {e:?}"),
    };

    let still_missing: Vec<_> = missing
        .iter()
        .filter(|id| !retry_ids.contains(id))
        .copied()
        .collect();
    let retry_summary = format!(
        "retry returned {} points, still missing: {still_missing:?}",
        retry_ids.len(),
    );

    panic!(
        "{label}(exact, name={vector_name}, filter_num={filter_num:?}, filter_url_prefix={filter_url_prefix:?}, limit={limit}) returned \
         {} points, expected {expected_len} (candidates: {}); \
         missing from result: {missing:?}; extra in result: {extra:?}\n\
         PROBE: retrieve(missing) = [{retrieved_summary}]\n\
         PROBE: count(filter) = {count_summary} (model says {})\n\
         PROBE: scroll(filter) — {scroll_summary}\n\
         PROBE: {retry_summary}",
        returned_ids.len(),
        candidates.len(),
        candidates.len(),
    );
}

fn nearest_internal_query(query: &VectorValue) -> VectorInternal {
    match query {
        VectorValue::Dense(v) => VectorInternal::Dense(v.clone()),
        VectorValue::Sparse(s) => VectorInternal::Sparse(s.clone()),
        VectorValue::MultiDense(matrix) => VectorInternal::MultiDense(
            MultiDenseVectorInternal::try_from_matrix(matrix.clone()).expect("non-empty matrix"),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_search(
    collection: &Collection,
    model: &Model,
    vector_name: &str,
    query: &VectorValue,
    limit: usize,
    exact: bool,
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
) {
    let run = async || {
        let named_query = NamedQuery::new(nearest_internal_query(query), vector_name);
        collection
            .search(
                CoreSearchRequest {
                    query: QueryEnum::Nearest(named_query),
                    filter: optional_read_filter(filter_num, filter_url_prefix),
                    params: Some(SearchParams {
                        exact,
                        ..Default::default()
                    }),
                    limit,
                    offset: 0,
                    with_payload: None,
                    with_vector: None,
                    score_threshold: None,
                },
                None,
                None,
                &ShardSelectorInternal::All,
                None,
                HwMeasurementAcc::new(),
            )
            .await
            .expect("search failed")
            .iter()
            .map(|r| r.id)
            .collect::<AHashSet<PointIdType>>()
    };

    let returned_ids = run().await;
    let (candidates, expected_len) =
        nearest_candidates(model, vector_name, filter_num, filter_url_prefix, limit);
    let strict = nearest_is_strict(exact, query);

    // Only re-run for the diagnostic comparison when we're about to fail the strict check.
    let retry_ids = if strict && returned_ids.len() != expected_len {
        run().await
    } else {
        AHashSet::new()
    };
    assert_nearest_size_invariant(
        collection,
        "search",
        &returned_ids,
        &retry_ids,
        &candidates,
        expected_len,
        strict,
        vector_name,
        filter_num,
        filter_url_prefix,
        limit,
    )
    .await;
    assert_nearest_results_valid(
        returned_ids,
        model,
        vector_name,
        filter_num,
        filter_url_prefix,
        "search",
    );
}

/// Basic coverage of the Query API. Issues a plain Nearest query (no prefetch/fusion) via
/// `collection.query`. Shares the exact same invariant checks as [`apply_search`] (candidate set,
/// strict top-k vs. upper bound, per-result membership, and the diagnostic probes on failure) — the
/// only difference is the request goes through the unified query entrypoint. Scores are float-flaky
/// so we never compare them.
#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_query(
    collection: &Collection,
    model: &Model,
    vector_name: &str,
    query: &VectorValue,
    limit: usize,
    exact: bool,
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
) {
    let run = async || {
        let named_query = NamedQuery::new(nearest_internal_query(query), vector_name);
        collection
            .query(
                ShardQueryRequest {
                    prefetches: vec![],
                    query: Some(ScoringQuery::Vector(QueryEnum::Nearest(named_query))),
                    filter: optional_read_filter(filter_num, filter_url_prefix),
                    score_threshold: None,
                    limit,
                    offset: 0,
                    params: Some(SearchParams {
                        exact,
                        ..Default::default()
                    }),
                    with_vector: WithVector::Bool(false),
                    with_payload: WithPayloadInterface::Bool(false),
                },
                None,
                None,
                ShardSelectorInternal::All,
                None,
                HwMeasurementAcc::new(),
            )
            .await
            .expect("query failed")
            .iter()
            .map(|r| r.id)
            .collect::<AHashSet<PointIdType>>()
    };

    let returned_ids = run().await;
    let (candidates, expected_len) =
        nearest_candidates(model, vector_name, filter_num, filter_url_prefix, limit);
    let strict = nearest_is_strict(exact, query);

    let retry_ids = if strict && returned_ids.len() != expected_len {
        run().await
    } else {
        AHashSet::new()
    };
    assert_nearest_size_invariant(
        collection,
        "query",
        &returned_ids,
        &retry_ids,
        &candidates,
        expected_len,
        strict,
        vector_name,
        filter_num,
        filter_url_prefix,
        limit,
    )
    .await;
    assert_nearest_results_valid(
        returned_ids,
        model,
        vector_name,
        filter_num,
        filter_url_prefix,
        "query",
    );
}

/// Coverage of the Query API's `prefetch` + fusion path (the plain `Query` op only does a top-level
/// Nearest). Each prefetch is an independent Nearest source; the outer query fuses their union with
/// RRF or DBSF, then the optional outer `num` filter applies on top.
///
/// Fusion ranking is score-based and approximate (and which points land in each prefetch's top-k is
/// score-dependent), so the oracle is upper-bound only — like the approximate `Search`/`Query`
/// path: every returned id must be some prefetch's candidate (its vector populated + that
/// prefetch's filter) *and* pass the outer filter, and the result size can't exceed the
/// outer-filtered union capped at `limit`. Scores and order are never checked.
pub(super) async fn apply_query_fusion(
    collection: &Collection,
    model: &Model,
    prefetches: &[Prefetch],
    fusion: FusionKind,
    limit: usize,
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
) {
    let shard_prefetches = prefetches
        .iter()
        .map(|p| {
            let named_query = NamedQuery::new(nearest_internal_query(&p.query), &p.vector_name);
            ShardPrefetch {
                prefetches: vec![],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(named_query))),
                limit: p.limit,
                params: None,
                filter: p.filter_num.map(match_num_filter),
                score_threshold: None,
            }
        })
        .collect();
    let fusion_internal = match fusion {
        FusionKind::Rrf => FusionInternal::Rrf {
            k: DEFAULT_RRF_K,
            weights: None,
        },
        FusionKind::Dbsf => FusionInternal::Dbsf,
    };
    let returned = collection
        .query(
            ShardQueryRequest {
                prefetches: shard_prefetches,
                query: Some(ScoringQuery::Fusion(fusion_internal)),
                filter: optional_read_filter(filter_num, filter_url_prefix),
                score_threshold: None,
                limit,
                offset: 0,
                params: None,
                with_vector: WithVector::Bool(false),
                with_payload: WithPayloadInterface::Bool(false),
            },
            None,
            None,
            ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("query fusion failed");
    // Collect ids as a Vec first so we can assert fusion actually deduplicated overlapping prefetch
    // hits — collapsing straight into a set would silently hide an engine returning a point twice.
    let returned_vec: Vec<PointIdType> = returned.iter().map(|r| r.id).collect();
    let returned_ids: AHashSet<PointIdType> = returned_vec.iter().copied().collect();
    assert_eq!(
        returned_vec.len(),
        returned_ids.len(),
        "query_fusion(fusion={fusion:?}, filter_num={filter_num:?}, filter_url_prefix={filter_url_prefix:?}) returned duplicate ids",
    );

    // Two independent upper bounds on the fused result, both narrowed by the outer `limit`:
    // - it's a subset of the outer-filtered union of every prefetch's candidates;
    // - it's no larger than the sum of per-prefetch caps (each prefetch returns at most
    //   `min(p.limit, |its candidates|)` points, and fusion only dedups that union smaller).
    // `nearest_candidates` hands back exactly that per-prefetch cap as its second element.
    let mut union: AHashSet<PointIdType> = AHashSet::new();
    let mut prefetch_cap = 0usize;
    for p in prefetches {
        let (candidates, cap) =
            nearest_candidates(model, &p.vector_name, p.filter_num, None, p.limit);
        prefetch_cap += cap;
        union.extend(candidates);
    }
    union.retain(|id| {
        model
            .get(id)
            .is_some_and(|e| passes_read_filters(&e.payload, filter_num, filter_url_prefix))
    });
    let expected_len = limit.min(union.len()).min(prefetch_cap);
    assert!(
        returned_ids.len() <= expected_len,
        "query_fusion(fusion={fusion:?}, filter_num={filter_num:?}, filter_url_prefix={filter_url_prefix:?}) returned {} points, expected at most {expected_len}",
        returned_ids.len(),
    );

    for id in &returned_ids {
        let entry = model
            .get(id)
            .unwrap_or_else(|| panic!("query_fusion returned unknown id {id:?}"));
        let is_prefetch_candidate = prefetches.iter().any(|p| {
            entry.vectors.contains_key(&p.vector_name)
                && p.filter_num.is_none_or(|n| num_matches(&entry.payload, n))
        });
        assert!(
            is_prefetch_candidate,
            "query_fusion returned id {id:?} matching no prefetch source",
        );
        assert!(
            passes_read_filters(&entry.payload, filter_num, filter_url_prefix),
            "query_fusion(outer filter num={filter_num:?}, url_prefix={filter_url_prefix:?}) returned id {id:?} that does not match",
        );
    }
}

pub(super) async fn apply_count_by_num(collection: &Collection, model: &Model, num: i64) {
    let actual = collection
        .count(
            CountRequestInternal {
                filter: Some(match_num_filter(num)),
                exact: true,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("count by filter failed")
        .count;
    let expected = model
        .values()
        .filter(|entry| num_matches(&entry.payload, num))
        .count();
    assert_eq!(actual, expected, "count(num=={num}) mismatch");
}

pub(super) async fn apply_count_by_slice(
    collection: &Collection,
    model: &Model,
    total: NonZeroU32,
    index: u32,
) {
    let actual = collection
        .count(
            CountRequestInternal {
                filter: Some(match_slice_filter(total, index)),
                exact: true,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("count by slice failed")
        .count;
    let slice = Slice { total, index };
    let expected = model.keys().filter(|id| slice.check(**id)).count();
    assert_eq!(
        actual, expected,
        "count(slice total={total} index={index}) mismatch"
    );
}

/// Map a model payload value to its facet representation. Only the facetable JSON kinds the
/// soak produces for `num`/`b` are handled (integer, bool).
fn facet_value_of(value: &serde_json::Value) -> Option<FacetValue> {
    match value {
        serde_json::Value::Bool(b) => Some(FacetValue::Bool(*b)),
        serde_json::Value::Number(n) => n.as_i64().map(FacetValue::Int),
        serde_json::Value::Null
        | serde_json::Value::String(_)
        | serde_json::Value::Array(_)
        | serde_json::Value::Object(_) => None,
    }
}

pub(super) async fn apply_facet(
    collection: &Collection,
    model: &Model,
    key: &str,
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
) {
    // `limit` is set well above any field's cardinality (`num` is 0..100, `b` is 2) so `exact`
    // facet returns every value with a nonzero count and nothing is truncated — letting us
    // compare the full per-value count map to the model.
    const FACET_LIMIT: usize = 1024;

    let response = collection
        .facet(
            FacetParams {
                key: key.parse().unwrap(),
                limit: FACET_LIMIT,
                filter: optional_read_filter(filter_num, filter_url_prefix),
                exact: true,
            },
            ShardSelectorInternal::All,
            None,
            None,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("facet failed");

    // Drop zero-count buckets: with the optimizer off (no vacuum), facet still emits a value
    // whose points have all been deleted, with count 0 — the model can't predict those. Keeping
    // only count > 0 still catches a real count error (a value the engine wrongly drops to 0
    // would then be absent here but present in the model).
    let actual: HashMap<FacetValue, usize> = response
        .hits
        .into_iter()
        .filter(|hit| hit.count > 0)
        .map(|hit| (hit.value, hit.count))
        .collect();

    let mut expected: HashMap<FacetValue, usize> = HashMap::new();
    for entry in model.values() {
        if !passes_read_filters(&entry.payload, filter_num, filter_url_prefix) {
            continue;
        }
        if let Some(value) = entry.payload.0.get(key).and_then(facet_value_of) {
            *expected.entry(value).or_insert(0) += 1;
        }
    }

    assert_eq!(
        actual, expected,
        "facet(key={key}, filter_num={filter_num:?}, filter_url_prefix={filter_url_prefix:?}) mismatch",
    );
}

pub(super) async fn apply_scroll_filtered_by_num(collection: &Collection, model: &Model, num: i64) {
    let scroll = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(usize::MAX),
                filter: Some(match_num_filter(num)),
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(false),
                order_by: None,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("scroll(filtered) failed");
    let returned: AHashSet<PointIdType> = scroll.points.iter().map(|r| r.id).collect();
    let expected: AHashSet<PointIdType> = model
        .iter()
        .filter(|(_, entry)| num_matches(&entry.payload, num))
        .map(|(id, _)| *id)
        .collect();
    assert_id_sets_eq(&returned, &expected, &format!("scroll(num=={num})"));
}

pub(super) async fn apply_count_by_tag(collection: &Collection, model: &Model, tag: &str) {
    let actual = collection
        .count(
            CountRequestInternal {
                filter: Some(match_tag_filter(tag)),
                exact: true,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("count by tag failed")
        .count;
    let expected = model
        .values()
        .filter(|entry| tag_matches(&entry.payload, tag))
        .count();
    assert_eq!(actual, expected, "count(tag=={tag:?}) mismatch");
}

pub(super) async fn apply_scroll_filtered_by_tag(
    collection: &Collection,
    model: &Model,
    tag: &str,
) {
    let scroll = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(usize::MAX),
                filter: Some(match_tag_filter(tag)),
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(false),
                order_by: None,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("scroll(filtered by tag) failed");
    let returned: AHashSet<PointIdType> = scroll.points.iter().map(|r| r.id).collect();
    let expected: AHashSet<PointIdType> = model
        .iter()
        .filter(|(_, entry)| tag_matches(&entry.payload, tag))
        .map(|(id, _)| *id)
        .collect();
    assert_id_sets_eq(&returned, &expected, &format!("scroll(tag=={tag:?})"));
}

pub(super) async fn apply_count_by_url_prefix(
    collection: &Collection,
    model: &Model,
    prefix: &str,
) {
    let actual = collection
        .count(
            CountRequestInternal {
                filter: Some(match_url_prefix_filter(prefix)),
                exact: true,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("count by url prefix failed")
        .count;
    let expected = model
        .values()
        .filter(|entry| url_prefix_matches(&entry.payload, prefix))
        .count();
    assert_eq!(actual, expected, "count(url prefix={prefix:?}) mismatch");
}

pub(super) async fn apply_scroll_filtered_by_url_prefix(
    collection: &Collection,
    model: &Model,
    prefix: &str,
) {
    let scroll = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(usize::MAX),
                filter: Some(match_url_prefix_filter(prefix)),
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(false),
                order_by: None,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("scroll(filtered by url prefix) failed");
    let returned: AHashSet<PointIdType> = scroll.points.iter().map(|r| r.id).collect();
    let expected: AHashSet<PointIdType> = model
        .iter()
        .filter(|(_, entry)| url_prefix_matches(&entry.payload, prefix))
        .map(|(id, _)| *id)
        .collect();
    assert_id_sets_eq(
        &returned,
        &expected,
        &format!("scroll(url prefix={prefix:?})"),
    );
}

pub(super) async fn apply_scroll_paged(
    collection: &Collection,
    model: &Model,
    limit: usize,
    filter: &ScrollFilter,
) {
    let engine_filter = match filter {
        ScrollFilter::None => None,
        ScrollFilter::Num(num) => Some(match_num_filter(*num)),
        ScrollFilter::Tag(tag) => Some(match_tag_filter(tag)),
        ScrollFilter::UrlPrefix(prefix) => Some(match_url_prefix_filter(prefix)),
        ScrollFilter::HasId(ids) => Some(match_has_id_filter(ids)),
        ScrollFilter::HasVector(name) => Some(match_has_vector_filter(name)),
        ScrollFilter::Slice { total, index } => Some(match_slice_filter(*total, *index)),
    };
    // Precompute the `has_id` set once so the per-point predicate is O(1).
    let has_id_set: AHashSet<PointIdType> = match filter {
        ScrollFilter::HasId(ids) => ids.iter().copied().collect(),
        ScrollFilter::None
        | ScrollFilter::Num(_)
        | ScrollFilter::Tag(_)
        | ScrollFilter::UrlPrefix(_)
        | ScrollFilter::HasVector(_)
        | ScrollFilter::Slice { .. } => AHashSet::new(),
    };
    let expected: AHashSet<PointIdType> = model
        .iter()
        .filter(|(id, entry)| match filter {
            ScrollFilter::None => true,
            ScrollFilter::Num(num) => num_matches(&entry.payload, *num),
            ScrollFilter::Tag(tag) => tag_matches(&entry.payload, tag),
            ScrollFilter::UrlPrefix(prefix) => url_prefix_matches(&entry.payload, prefix),
            ScrollFilter::HasId(_) => has_id_set.contains(*id),
            ScrollFilter::HasVector(name) => entry.vectors.contains_key(name),
            ScrollFilter::Slice { total, index } => Slice {
                total: *total,
                index: *index,
            }
            .check(**id),
        })
        .map(|(id, _)| *id)
        .collect();

    // Walk pages following `next_page_offset`, accumulating ids and asserting no id repeats.
    let mut seen: AHashSet<PointIdType> = AHashSet::new();
    let mut offset: Option<PointIdType> = None;
    // Upper bound on iterations: at least one point consumed per page, plus slack for the final
    // empty page. Guards against an infinite loop if the cursor ever fails to advance.
    let max_pages = expected.len() + 2;
    let mut pages = 0;
    loop {
        let scroll = collection
            .scroll_by(
                ScrollRequestInternal {
                    offset,
                    limit: Some(limit),
                    filter: engine_filter.clone(),
                    with_payload: Some(WithPayloadInterface::Bool(false)),
                    with_vector: WithVector::Bool(false),
                    order_by: None,
                },
                None,
                None,
                &ShardSelectorInternal::All,
                None,
                HwMeasurementAcc::new(),
            )
            .await
            .expect("scroll(paged) failed");

        assert!(
            scroll.points.len() <= limit,
            "scroll(paged, limit={limit}) returned a page of {} points (> limit)",
            scroll.points.len(),
        );
        for record in &scroll.points {
            assert!(
                seen.insert(record.id),
                "scroll(paged) returned duplicate id {:?} across pages",
                record.id,
            );
        }

        pages += 1;
        assert!(
            pages <= max_pages + 1,
            "scroll(paged, limit={limit}) did not terminate after {pages} pages (cursor stuck?)",
        );

        match scroll.next_page_offset {
            Some(next) => offset = Some(next),
            None => break,
        }
    }

    assert_id_sets_eq(
        &seen,
        &expected,
        &format!("scroll(paged, filter={filter:?})"),
    );
}

pub(super) async fn apply_scroll_ordered(
    collection: &Collection,
    model: &Model,
    direction: Direction,
) {
    let scroll = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(usize::MAX),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(false),
                order_by: Some(OrderByInterface::Struct(OrderBy {
                    key: "num".parse().unwrap(),
                    direction: Some(direction),
                    start_from: None,
                })),
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("scroll(ordered) failed");

    // Pull (id, num) pairs from order_value on each record.
    let returned: Vec<(PointIdType, i64)> = scroll
        .points
        .iter()
        .map(|r| {
            let n = match r.order_value.as_ref().expect("order_value missing") {
                OrderValue::Int(i) => *i,
                other @ OrderValue::Float(_) => {
                    panic!("unexpected order_value variant: {other:?}")
                }
            };
            (r.id, n)
        })
        .collect();

    // 1. order_value must match the model's `num` for each returned id.
    for (id, n) in &returned {
        let entry = model.get(id).expect("ordered scroll returned unknown id");
        let model_num = entry
            .payload
            .0
            .get("num")
            .and_then(|v| v.as_i64())
            .expect("ordered scroll returned id without `num`");
        assert_eq!(
            *n, model_num,
            "ordered scroll order_value disagrees with model for id {id:?}",
        );
    }

    // 2. Returned order_values must be monotonic.
    for w in returned.windows(2) {
        let ok = match direction {
            Direction::Asc => w[0].1 <= w[1].1,
            Direction::Desc => w[0].1 >= w[1].1,
        };
        assert!(
            ok,
            "ordered scroll not monotonic ({direction:?}): {} then {}",
            w[0].1, w[1].1,
        );
    }

    // 3. Every model point with a `num` must appear in the result.
    let returned_ids: AHashSet<PointIdType> = returned.iter().map(|(id, _)| *id).collect();
    let expected_ids: AHashSet<PointIdType> = model
        .iter()
        .filter(|(_, entry)| has_num(&entry.payload))
        .map(|(id, _)| *id)
        .collect();
    assert_id_sets_eq(&returned_ids, &expected_ids, "ordered scroll");
}

pub(super) async fn apply_recommend(
    collection: &Collection,
    model: &Model,
    positive: &[PointIdType],
    negative: &[PointIdType],
    limit: usize,
    strategy: RecommendStrategy,
    vector_name: &str,
) {
    let results = recommend_by(
        RecommendRequestInternal {
            positive: positive
                .iter()
                .map(|id| RecommendExample::PointId(*id))
                .collect(),
            negative: negative
                .iter()
                .map(|id| RecommendExample::PointId(*id))
                .collect(),
            limit,
            strategy: Some(strategy),
            using: Some(UsingVector::Name(vector_name.to_string())),
            ..Default::default()
        },
        collection,
        |_name| async { unreachable!("no cross-collection lookup in this test") },
        None,
        None,
        ShardSelectorInternal::All,
        None,
        HwMeasurementAcc::new(),
    )
    .await
    .expect("recommend failed");

    let excluded: AHashSet<PointIdType> = positive.iter().chain(negative.iter()).copied().collect();
    // Candidate points must (a) not be excluded and (b) have the requested vector populated.
    let candidate_count = model
        .iter()
        .filter(|(id, _)| !excluded.contains(id))
        .filter(|(_, entry)| entry.vectors.contains_key(vector_name))
        .count();
    let expected_len = limit.min(candidate_count);
    // `recommend_by` defaults to `params: None` which lowers to an approximate search; we only
    // assert an upper bound.
    assert!(
        results.len() <= expected_len,
        "recommend(name={vector_name}, {strategy:?}) returned {} points, expected at most {}",
        results.len(),
        expected_len,
    );
    for record in &results {
        let entry = model.get(&record.id).unwrap_or_else(|| {
            panic!(
                "recommend({strategy:?}) returned unknown id {:?}",
                record.id
            )
        });
        assert!(
            entry.vectors.contains_key(vector_name),
            "recommend returned id {:?} that doesn't have vector `{vector_name}`",
            record.id,
        );
        assert!(
            !excluded.contains(&record.id),
            "recommend({strategy:?}) returned excluded id {:?}",
            record.id,
        );
    }
}
