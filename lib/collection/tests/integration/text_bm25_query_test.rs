//! BM25 over a text index through the collection's query path: tokenized and
//! gathered per shard, merged across shards.

use collection::collection::Collection;
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::ScoreType;
use itertools::Itertools;
use segment::data_types::index::TextIndexParams;
use segment::data_types::query_context::fancy_idf;
use segment::data_types::vectors::NamedQuery;
use segment::index::field_index::full_text_index::Bm25Params;
use segment::json_path::JsonPath;
use segment::types::{
    ExtendedPointId, Payload, PayloadFieldSchema, PayloadSchemaParams, ScoredPoint,
};
use serde_json::Map;
use shard::query::query_enum::QueryEnum;
use shard::query::text::TextScoringQuery;
use shard::query::{FusionInternal, ScoringQuery, ShardPrefetch, ShardQueryRequest};
use tempfile::Builder;

use crate::common::simple_collection_fixture;

const FIELD: &str = "body";
const POINTS: u64 = 35;

/// One document per pair of `alpha` (1 to 5) and `gamma` (0 to 6) counts, so
/// no two documents score the same for a query of those terms.
fn text_of(i: u64) -> String {
    ["alpha"; 5][..(i % 5 + 1) as usize].join(" ") + &" gamma".repeat((i / 5) as usize)
}

async fn text_collection(path: &std::path::Path, shard_number: u32) -> Collection {
    let collection = simple_collection_fixture(path, shard_number).await;
    // Term frequencies come from positions, which phrase matching stores.
    let params = TextIndexParams {
        phrase_matching: Some(true),
        ..TextIndexParams::default()
    };
    collection
        .create_payload_index_with_wait(
            JsonPath::new(FIELD),
            PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(params)),
            true,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();

    let batch = BatchPersisted {
        ids: (0..POINTS).map(u64::into).collect_vec(),
        vectors: BatchVectorStructPersisted::Single(
            (0..POINTS)
                .map(|i| vec![1.0, i as f32 / POINTS as f32, 0.0, 0.0])
                .collect(),
        ),
        payloads: Some(
            (0..POINTS)
                .map(|i| {
                    Some(Payload(Map::from_iter([(
                        FIELD.to_owned(),
                        text_of(i).into(),
                    )])))
                })
                .collect(),
        ),
    };
    collection
        .update_from_client_simple(
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::from(batch),
            )),
            true,
            None,
            WriteOrdering::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();
    collection
}

fn text_query(text: &str) -> ScoringQuery {
    ScoringQuery::Text(TextScoringQuery {
        field: JsonPath::new(FIELD),
        text: text.to_owned(),
        params: Bm25Params::default(),
    })
}

fn request(query: ScoringQuery, limit: usize, offset: usize) -> ShardQueryRequest {
    ShardQueryRequest {
        prefetches: vec![],
        query: Some(query),
        filter: None,
        score_threshold: None,
        limit,
        offset,
        params: None,
        with_vector: false.into(),
        with_payload: false.into(),
    }
}

async fn query(
    collection: &Collection,
    request: ShardQueryRequest,
    shards: ShardSelectorInternal,
) -> Vec<ScoredPoint> {
    collection
        .query(request, None, None, shards, None, HwMeasurementAcc::new())
        .await
        .unwrap()
}

fn id_of(point: &ScoredPoint) -> u64 {
    match point.id {
        ExtendedPointId::NumId(id) => id,
        ExtendedPointId::Uuid(_) => panic!("the fixture uses numeric ids"),
    }
}

/// One shard, so the statistics cover the whole corpus: every score equals
/// BM25 by definition. No lengths are recorded until the index params can ask
/// for them, so length normalization is off and `b` has no effect.
#[tokio::test(flavor = "multi_thread")]
async fn text_query_scores_by_definition() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = text_collection(dir.path(), 1).await;

    // Mixed case on purpose: the shard tokenizes with the field's tokenizer.
    let points = query(
        &collection,
        request(text_query("Alpha GAMMA"), POINTS as usize, 0),
        ShardSelectorInternal::All,
    )
    .await;

    let Bm25Params { k1, .. } = Bm25Params::default();
    let counts = |i: u64| {
        let text = text_of(i);
        let count = |term| text.split(' ').filter(|t| *t == term).count() as ScoreType;
        (count("alpha"), count("gamma"))
    };
    let n = POINTS as ScoreType;
    let df_alpha = POINTS as ScoreType;
    let df_gamma = (0..POINTS).filter(|&i| counts(i).1 > 0.0).count() as ScoreType;
    let term = |tf: ScoreType, df: ScoreType| {
        if tf == 0.0 {
            0.0
        } else {
            fancy_idf(n, df).max(0.0) * tf * (k1 + 1.0) / (tf + k1)
        }
    };
    let expected = |i: u64| {
        let (alpha, gamma) = counts(i);
        term(alpha, df_alpha) + term(gamma, df_gamma)
    };

    assert!(!points.is_empty());
    for pair in points.windows(2) {
        assert!(pair[0].score >= pair[1].score, "ranked best first");
    }
    for point in &points {
        let reference = expected(id_of(point));
        assert!(
            (point.score - reference).abs() <= 1e-4 * reference.max(1.0),
            "point {}: engine {}, reference {reference}",
            id_of(point),
            point.score,
        );
    }
    // Every point holds `alpha`, whose IDF is small but positive.
    assert_eq!(points.len(), POINTS as usize);
}

/// Two shards, each scoring against its own statistics: the collection's
/// ranking is the per-shard rankings merged by score, and pagination cuts that
/// merged ranking.
#[tokio::test(flavor = "multi_thread")]
async fn text_query_merges_shards() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = text_collection(dir.path(), 2).await;
    let limit = 20;

    let merged = query(
        &collection,
        request(text_query("alpha gamma"), limit, 0),
        ShardSelectorInternal::All,
    )
    .await;

    let mut per_shard = Vec::new();
    for shard_id in 0..2 {
        let points = query(
            &collection,
            request(text_query("alpha gamma"), limit, 0),
            ShardSelectorInternal::ShardId(shard_id),
        )
        .await;
        assert!(!points.is_empty(), "shard {shard_id} holds matches");
        per_shard.extend(points);
    }
    per_shard.sort_by(|a, b| b.score.total_cmp(&a.score));
    per_shard.truncate(limit);
    let ranked = |points: &[ScoredPoint]| points.iter().map(|p| (id_of(p), p.score)).collect_vec();
    assert_eq!(ranked(&merged), ranked(&per_shard));

    let page = query(
        &collection,
        request(text_query("alpha gamma"), 5, 3),
        ShardSelectorInternal::All,
    )
    .await;
    assert_eq!(ranked(&page), ranked(&merged[3..8]));
}

/// Hybrid search: BM25 as a prefetch next to a dense one, fused by RRF. The
/// text leaf resolves inside the fusion, and every fused point comes from one
/// of the two prefetches.
#[tokio::test(flavor = "multi_thread")]
async fn text_query_fuses_with_a_dense_prefetch() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = text_collection(dir.path(), 2).await;
    let dense = ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
        vec![0.0, 1.0, 0.0, 0.0].into(),
        "",
    )));
    let prefetch = |query| ShardPrefetch {
        prefetches: Vec::new(),
        query: Some(query),
        limit: 10,
        params: None,
        filter: None,
        score_threshold: None,
    };

    let fused = query(
        &collection,
        ShardQueryRequest {
            prefetches: vec![prefetch(dense.clone()), prefetch(text_query("gamma"))],
            ..request(
                ScoringQuery::Fusion(FusionInternal::Rrf {
                    k: 60,
                    weights: None,
                }),
                15,
                0,
            )
        },
        ShardSelectorInternal::All,
    )
    .await;

    let ids = |points: &[ScoredPoint]| points.iter().map(id_of).collect::<Vec<_>>();
    let from_text = ids(&query(
        &collection,
        request(text_query("gamma"), 10, 0),
        ShardSelectorInternal::All,
    )
    .await);
    let from_dense = ids(&query(
        &collection,
        request(dense, 10, 0),
        ShardSelectorInternal::All,
    )
    .await);
    assert_eq!(fused.len(), 15);
    for id in ids(&fused) {
        assert!(
            from_text.contains(&id) || from_dense.contains(&id),
            "fused point {id} comes from a prefetch",
        );
    }
    assert!(
        ids(&fused)
            .iter()
            .any(|id| from_text.contains(id) && !from_dense.contains(id)),
        "the text prefetch contributes",
    );
}
