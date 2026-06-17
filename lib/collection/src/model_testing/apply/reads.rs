use std::collections::HashMap;

use ahash::AHashSet;
use api::rest::RecommendStrategy;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::facets::{FacetParams, FacetValue};
use segment::data_types::order_by::{Direction, OrderBy, OrderByInterface, OrderValue};
use segment::data_types::vectors::{
    MultiDenseVectorInternal, NamedQuery, VectorInternal, VectorStructInternal,
};
use segment::types::{PointIdType, SearchParams, VectorNameBuf, WithPayloadInterface, WithVector};
use shard::query::query_enum::QueryEnum;
use shard::scroll::ScrollRequestInternal;

use super::super::op::{
    NamedVectors, canonical_sparse, has_num, match_num_filter, match_tag_filter, num_matches,
    tag_matches,
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
                assert_eq!(a, b, "{ctx}: dense vector `{name}` mismatch for id {id:?}");
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
        assert!(
            in_model == in_returned,
            "{ctx} membership mismatch for id {id:?}: model={in_model}, returned={in_returned}",
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_search(
    collection: &Collection,
    model: &Model,
    vector_name: &str,
    query: &VectorValue,
    limit: usize,
    exact: bool,
    filter_num: Option<i64>,
) {
    let internal_query = match query {
        VectorValue::Dense(v) => VectorInternal::Dense(v.clone()),
        VectorValue::Sparse(s) => VectorInternal::Sparse(s.clone()),
        VectorValue::MultiDense(matrix) => VectorInternal::MultiDense(
            MultiDenseVectorInternal::try_from_matrix(matrix.clone()).expect("non-empty matrix"),
        ),
    };
    let named_query = NamedQuery::new(internal_query, vector_name);
    let results = collection
        .search(
            CoreSearchRequest {
                query: QueryEnum::Nearest(named_query),
                filter: filter_num.map(match_num_filter),
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
        .expect("search failed");
    // Candidates = points that (a) have the queried vector populated and (b) pass the optional
    // filter. Points missing the vector aren't searchable.
    let candidates: AHashSet<PointIdType> = model
        .iter()
        .filter(|(_, entry)| entry.vectors.contains_key(vector_name))
        .filter(|(_, entry)| filter_num.is_none_or(|n| num_matches(&entry.payload, n)))
        .map(|(id, _)| *id)
        .collect();
    let expected_len = limit.min(candidates.len());
    // For dense exact=true we have an exact scan so the result is exactly the top-k.
    // For dense exact=false (HNSW), the engine may legally return fewer than expected when the
    // filter is restrictive or recall is below 1.0.
    // For sparse search, the effective candidate set is "points with non-zero overlap with the
    // query" rather than "all points with the vector populated", which is data-dependent and
    // smaller than `candidate_count`. So sparse search is always an upper bound.
    // Multi-dense (MaxSim) over an exact scan should also return every candidate, so it
    // can use the strict equality path alongside plain dense exact.
    let strict = exact && matches!(query, VectorValue::Dense(_) | VectorValue::MultiDense(_));
    if strict {
        if results.len() != expected_len {
            let returned_ids: AHashSet<PointIdType> = results.iter().map(|r| r.id).collect();
            let mut missing: Vec<_> = candidates.difference(&returned_ids).copied().collect();
            let mut extra: Vec<_> = returned_ids.difference(&candidates).copied().collect();
            missing.sort();
            extra.sort();

            // Follow-up probes before panicking — classify the bug.
            //   1. Does Retrieve see the missing point(s)?
            //   2. Does CountByFilter see the right count?
            //   3. Does re-running the same search give the same wrong answer (consistent
            //      vs flapping)?
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
                        let has_vec = r
                            .vector
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
                            .and_then(|p| p.0.get("num").and_then(|v| v.as_i64()))
                            == filter_num;
                        format!("{:?}{{vec:{has_vec}, num_match:{has_num}}}", r.id)
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                Err(e) => format!("retrieve failed: {e:?}"),
            };

            let probe_count = collection
                .count(
                    CountRequestInternal {
                        filter: filter_num.map(match_num_filter),
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
                        filter: filter_num.map(match_num_filter),
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
                    let mut ghost: Vec<_> = engine_set.difference(&candidates).copied().collect();
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

            // Re-run the same search to see if it's deterministic or flapping.
            let retry_internal_query = match query {
                VectorValue::Dense(v) => VectorInternal::Dense(v.clone()),
                VectorValue::Sparse(s) => VectorInternal::Sparse(s.clone()),
                VectorValue::MultiDense(matrix) => VectorInternal::MultiDense(
                    MultiDenseVectorInternal::try_from_matrix(matrix.clone())
                        .expect("non-empty matrix"),
                ),
            };
            let retry_named = NamedQuery::new(retry_internal_query, vector_name);
            let retry_results = collection
                .search(
                    CoreSearchRequest {
                        query: QueryEnum::Nearest(retry_named),
                        filter: filter_num.map(match_num_filter),
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
                .await;
            let retry_summary = match retry_results {
                Ok(r) => {
                    let retry_ids: AHashSet<PointIdType> = r.iter().map(|x| x.id).collect();
                    let still_missing: Vec<_> = missing
                        .iter()
                        .filter(|id| !retry_ids.contains(id))
                        .copied()
                        .collect();
                    format!(
                        "retry returned {} points, still missing: {still_missing:?}",
                        r.len(),
                    )
                }
                Err(e) => format!("retry err: {e:?}"),
            };

            panic!(
                "search(exact, name={vector_name}, filter={filter_num:?}, limit={limit}) returned \
                 {} points, expected {expected_len} (candidates: {}); \
                 missing from result: {missing:?}; extra in result: {extra:?}\n\
                 PROBE: retrieve(missing) = [{retrieved_summary}]\n\
                 PROBE: count(filter) = {count_summary} (model says {})\n\
                 PROBE: scroll(filter) — {scroll_summary}\n\
                 PROBE: {retry_summary}",
                results.len(),
                candidates.len(),
                candidates.len(),
            );
        }
    } else {
        assert!(
            results.len() <= expected_len,
            "search(approx, name={vector_name}, filter={filter_num:?}) returned {} points, expected at most {}",
            results.len(),
            expected_len,
        );
    }
    for record in &results {
        let entry = model
            .get(&record.id)
            .unwrap_or_else(|| panic!("search returned unknown id {:?}", record.id));
        assert!(
            entry.vectors.contains_key(vector_name),
            "search returned id {:?} that doesn't have vector `{vector_name}`",
            record.id,
        );
        if let Some(n) = filter_num {
            assert!(
                num_matches(&entry.payload, n),
                "search(filter num=={n}) returned id {:?} that does not match",
                record.id,
            );
        }
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
                filter: filter_num.map(match_num_filter),
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
        if filter_num.is_some_and(|n| !num_matches(&entry.payload, n)) {
            continue;
        }
        if let Some(value) = entry.payload.0.get(key).and_then(facet_value_of) {
            *expected.entry(value).or_insert(0) += 1;
        }
    }

    assert_eq!(
        actual, expected,
        "facet(key={key}, filter_num={filter_num:?}) mismatch",
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

#[allow(clippy::too_many_arguments)]
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
