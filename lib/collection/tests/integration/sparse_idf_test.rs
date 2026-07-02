use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;

use api::rest::SearchRequestInternal;
use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{SparseVectorParams, UpdateStatus};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::operations::{CollectionUpdateOperations, point_ops};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::modifier::Modifier;
use segment::data_types::vectors::NamedSparseVector;
use segment::json_path::JsonPath;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, IdfCorpusParams, IdfParams, IdfScope,
    ScoredPoint, SearchParams,
};
use serde_json::json;
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::common::{TEST_OPTIMIZERS_CONFIG, new_local_collection};

const SPARSE_VECTOR_NAME: &str = "sparse";

/// Advanced IDF formula, as implemented by `VectorQueryContext::remap_idf_weights`.
fn expected_idf(n: usize, df: usize) -> f32 {
    let (n, df) = (n as f32, df as f32);
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

fn tenant_filter(value: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("tenant"),
        value.to_string().into(),
    )))
}

fn idf_corpus_params(corpus: Filter) -> SearchParams {
    SearchParams {
        idf: Some(IdfParams::Corpus(IdfCorpusParams { corpus })),
        ..Default::default()
    }
}

/// Collection with a dense vector and an IDF-modified sparse vector,
/// single shard so IDF statistics cover all points:
///
/// | point | tenant | sparse dims |
/// |-------|--------|-------------|
/// | 0     | a      | 0, 1        |
/// | 1     | a      | 0           |
/// | 2     | b      | 0, 1, 2     |
/// | 3     | b      | 1           |
async fn sparse_idf_collection_fixture(path: &std::path::Path) -> Collection {
    let collection_params = CollectionParams {
        vectors: VectorParamsBuilder::new(4, Distance::Dot).build().into(),
        sparse_vectors: Some(BTreeMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorParams {
                index: None,
                modifier: Some(Modifier::Idf),
            },
        )])),
        shard_number: NonZeroU32::new(1).unwrap(),
        ..CollectionParams::empty()
    };

    let collection_config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config: WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
            wal_retain_closed: 1,
        },
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    let snapshot_path = path.join("snapshots");
    let collection = new_local_collection("test".to_string(), path, &snapshot_path, &collection_config)
        .await
        .unwrap();

    let points: &[(&str, &[u32])] = &[
        ("a", &[0, 1]),
        ("a", &[0]),
        ("b", &[0, 1, 2]),
        ("b", &[1]),
    ];

    let points = points
        .iter()
        .enumerate()
        .map(|(point_id, (tenant, dims))| {
            let sparse_vector = SparseVector::new(dims.to_vec(), vec![1.0; dims.len()]).unwrap();
            PointStructPersisted {
                id: (point_id as u64).into(),
                vector: VectorStructPersisted::Named(HashMap::from([(
                    SPARSE_VECTOR_NAME.to_owned(),
                    point_ops::VectorPersisted::Sparse(sparse_vector),
                )])),
                payload: Some(serde_json::from_value(json!({ "tenant": tenant })).unwrap()),
            }
        })
        .collect();

    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));
    let result = collection
        .update_from_client_simple(
            insert_points,
            true,
            None,
            WriteOrdering::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.status, UpdateStatus::Completed);

    collection
}

async fn search(
    collection: &Collection,
    filter: Option<Filter>,
    params: Option<SearchParams>,
) -> Vec<ScoredPoint> {
    let request = SearchRequestInternal {
        vector: api::rest::NamedVectorStruct::Sparse(NamedSparseVector {
            name: SPARSE_VECTOR_NAME.to_owned(),
            vector: SparseVector::new(vec![0, 1, 2], vec![1.0, 1.0, 1.0]).unwrap(),
        }),
        filter,
        params,
        limit: 10,
        offset: None,
        with_payload: None,
        with_vector: None,
        score_threshold: None,
    };

    collection
        .search(
            request.into(),
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap()
}

fn scores_by_id(points: &[ScoredPoint]) -> HashMap<u64, f32> {
    points
        .iter()
        .map(|point| {
            let id = match point.id {
                segment::types::ExtendedPointId::NumId(id) => id,
                segment::types::ExtendedPointId::Uuid(_) => panic!("numeric ids expected"),
            };
            (id, point.score)
        })
        .collect()
}

fn assert_score(scores: &HashMap<u64, f32>, id: u64, expected: f32) {
    let score = scores[&id];
    assert!(
        (score - expected).abs() < 1e-5,
        "point {id}: got {score}, expected {expected}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sparse_idf_corpus_search() {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = sparse_idf_collection_fixture(collection_dir.path()).await;

    // Stored values are 1.0, query values are 1.0, so each point scores the
    // sum of IDF weights of its dims that occur in the query.

    // Global statistics: N = 4, df = [3, 3, 1].
    let global = |df: usize| expected_idf(4, df);
    let baseline = search(&collection, None, None).await;
    let scores = scores_by_id(&baseline);
    assert_score(&scores, 0, global(3) + global(3));
    assert_score(&scores, 1, global(3));
    assert_score(&scores, 2, global(3) + global(3) + global(1));
    assert_score(&scores, 3, global(3));

    // Explicit `"global"` must reproduce the default exactly.
    let explicit_global = search(
        &collection,
        None,
        Some(SearchParams {
            idf: Some(IdfParams::Scope(IdfScope::Global)),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(baseline, explicit_global);

    // Corpus = tenant a: N = 2, df = [2, 1, 0] — while retrieval stays
    // unfiltered. Scoring changes for every point, including tenant b's.
    let corpus_a = |df: usize| expected_idf(2, df);
    let decoupled = search(&collection, None, Some(idf_corpus_params(tenant_filter("a")))).await;
    let scores = scores_by_id(&decoupled);
    assert_score(&scores, 0, corpus_a(2) + corpus_a(1));
    assert_score(&scores, 1, corpus_a(2));
    assert_score(&scores, 2, corpus_a(2) + corpus_a(1) + corpus_a(0));
    assert_score(&scores, 3, corpus_a(1));

    // Corpus and retrieval filter combined: same corpus-scoped scores,
    // narrowed result set.
    let filtered = search(
        &collection,
        Some(tenant_filter("a")),
        Some(idf_corpus_params(tenant_filter("a"))),
    )
    .await;
    let scores = scores_by_id(&filtered);
    assert_eq!(scores.len(), 2);
    assert_score(&scores, 0, corpus_a(2) + corpus_a(1));
    assert_score(&scores, 1, corpus_a(2));

    // Tightening the retrieval filter must NOT move the scores when the
    // corpus stays fixed — the score scale is defined by the corpus alone.
    let corpus_b = |df: usize| expected_idf(2, df);
    let broad = search(&collection, None, Some(idf_corpus_params(tenant_filter("b")))).await;
    let narrow = search(
        &collection,
        Some(tenant_filter("b")),
        Some(idf_corpus_params(tenant_filter("b"))),
    )
    .await;
    let broad_scores = scores_by_id(&broad);
    let narrow_scores = scores_by_id(&narrow);
    for id in [2, 3] {
        assert!((broad_scores[&id] - narrow_scores[&id]).abs() < 1e-6);
    }
    // Corpus tenant b: N = 2, df = [1, 2, 1].
    assert_score(&broad_scores, 2, corpus_b(1) + corpus_b(2) + corpus_b(1));

    // An empty corpus yields degenerate but corpus-scoped scores — never a
    // fallback to global statistics. idf(0, 0) = ln(2) for every term.
    let empty = search(
        &collection,
        None,
        Some(idf_corpus_params(tenant_filter("missing"))),
    )
    .await;
    let scores = scores_by_id(&empty);
    let ln2 = expected_idf(0, 0);
    assert_score(&scores, 0, 2.0 * ln2);
    assert_score(&scores, 2, 3.0 * ln2);
}

#[tokio::test(flavor = "multi_thread")]
async fn sparse_idf_params_require_idf_modifier() {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = sparse_idf_collection_fixture(collection_dir.path()).await;

    // The `idf` search param on a vector without the IDF modifier (the dense
    // default vector here) is rejected instead of being silently ignored.
    let request = SearchRequestInternal {
        vector: vec![1.0, 0.0, 0.0, 0.0].into(),
        filter: None,
        params: Some(idf_corpus_params(tenant_filter("a"))),
        limit: 10,
        offset: None,
        with_payload: None,
        with_vector: None,
        score_threshold: None,
    };

    let error = collection
        .search(
            request.into(),
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("idf"),
        "unexpected error: {error}"
    );
}
