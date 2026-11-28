use std::num::NonZeroU32;

use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::PointRequestInternal;
use collection::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryRequest, Query, VectorInputInternal, VectorQuery,
};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use common::ambient::hw::HwMetric;
use common::ambient::{AmbientContext, AmbientFutureExt};
use rstest::rstest;
use segment::data_types::vectors::VectorStructInternal;
use segment::types::{Distance, ExtendedPointId, ScoredPoint, WithVector};
use serde_json::json;
use shard::query::FusionInternal;
use tempfile::TempDir;

use crate::common::{TEST_OPTIMIZERS_CONFIG, new_local_collection};

const LIMIT: usize = 5;
const PAYLOAD_BYTES: usize = 1024;

async fn fusion_collection(path: &std::path::Path, shards: u32) -> Collection {
    let config = CollectionConfigInternal {
        params: CollectionParams {
            vectors: VectorParamsBuilder::new(4, Distance::Dot).build().into(),
            shard_number: NonZeroU32::new(shards).unwrap(),
            #[expect(deprecated)]
            on_disk_payload: Some(true),
            ..CollectionParams::empty()
        },
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config: WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
            wal_retain_closed: 1,
        },
        hnsw_config: Default::default(),
        quantization_config: None,
        strict_mode_config: None,
        uuid: None,
        metadata: None,
        created_at: None,
    };
    let collection =
        new_local_collection("test".to_owned(), path, &path.join("snapshots"), &config)
            .await
            .unwrap();
    let batch = BatchPersisted {
        ids: (0..100u64).map(Into::into).collect(),
        vectors: BatchVectorStructPersisted::Single(
            (0..100)
                .map(|i| vec![i as f32, (i * i + 1) as f32, 0.0, 0.0])
                .collect(),
        ),
        payloads: Some(
            (0..100)
                .map(|i| {
                    Some(
                        serde_json::from_value(json!({"text": "x".repeat(PAYLOAD_BYTES), "id": i}))
                            .unwrap(),
                    )
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
        )
        .measured(AmbientContext::new())
        .await
        .unwrap();
    collection
}

fn nearest(direction: f32) -> Query {
    Query::Vector(VectorQuery::Nearest(VectorInputInternal::Vector(
        vec![direction, 1.0, 0.0, 0.0].into(),
    )))
}

fn request(fusion: FusionInternal, offset: usize) -> CollectionQueryRequest {
    CollectionQueryRequest {
        prefetch: [1.0, -1.0]
            .into_iter()
            .map(|direction| CollectionPrefetch {
                prefetch: vec![],
                query: Some(nearest(direction)),
                using: String::new(),
                filter: None,
                score_threshold: None,
                limit: 80,
                params: None,
                lookup_from: None,
            })
            .collect(),
        query: Some(Query::Fusion(fusion)),
        using: String::new(),
        filter: None,
        score_threshold: None,
        limit: LIMIT,
        offset,
        params: None,
        with_vector: true.into(),
        with_payload: true.into(),
        lookup_from: None,
    }
}

async fn query_batch(
    collection: &Collection,
    requests: Vec<CollectionQueryRequest>,
) -> (Vec<Vec<ScoredPoint>>, usize) {
    let context = AmbientContext::new();
    let results = collection
        .query_batch(
            requests
                .into_iter()
                .map(|req| (req, ShardSelectorInternal::All))
                .collect(),
            |_| async { None },
            None,
            None,
            None,
        )
        .measured(context.clone())
        .await
        .unwrap();
    (results, context.hw_data()[HwMetric::PayloadIoRead])
}

#[rstest]
#[case::rrf(FusionInternal::Rrf { k: 2, weights: None }, 1, 0, false)]
#[case::dbsf(FusionInternal::Dbsf, 3, 3, false)]
#[case::mixed_batch(FusionInternal::Rrf { k: 21, weights: Some(vec![3.0.into(), 1.0.into()]) }, 3, 3, true)]
#[tokio::test(flavor = "multi_thread")]
async fn fusion_fetches_only_final_page(
    #[case] fusion: FusionInternal,
    #[case] shards: u32,
    #[case] offset: usize,
    #[case] mixed_batch: bool,
) {
    let dir = TempDir::new().unwrap();
    let collection = fusion_collection(dir.path(), shards).await;
    let request = request(fusion, offset);
    let mut bare = request.clone();
    bare.with_payload = false.into();
    bare.with_vector = false.into();
    bare.offset = 0;
    bare.limit = offset + LIMIT;
    let (expected, bare_reads) = query_batch(&collection, vec![bare.clone()]).await;
    assert_eq!(bare_reads, 0);

    // Compare with retrieving exactly the final page. Payload storage may compress
    // the text, so use measured reads rather than the uncompressed payload size.
    let retrieve_context = AmbientContext::new();
    let retrieved = collection
        .retrieve(
            PointRequestInternal {
                ids: expected[0][offset..].iter().map(|point| point.id).collect(),
                with_payload: Some(request.with_payload.clone()),
                with_vector: request.with_vector.clone(),
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .measured(retrieve_context.clone())
        .await
        .unwrap();
    assert_eq!(retrieved.len(), LIMIT);
    let expected_reads = retrieve_context.hw_data()[HwMetric::PayloadIoRead];
    assert!(expected_reads > 0);

    let mut requests = vec![request.clone()];
    if mixed_batch {
        // An ordinary, metadata-free sibling must not disable deferred fetching.
        bare.prefetch.clear();
        bare.query = Some(nearest(1.0));
        bare.limit = LIMIT;
        requests.push(bare);
    }
    let (results, payload_reads) = query_batch(&collection, requests).await;
    assert_eq!(results[0].len(), LIMIT);
    assert_eq!(
        results[0]
            .iter()
            .map(|p| (p.id, p.score))
            .collect::<Vec<_>>(),
        expected[0][offset..]
            .iter()
            .map(|p| (p.id, p.score))
            .collect::<Vec<_>>()
    );
    for point in &results[0] {
        assert_eq!(
            point.payload.as_ref().unwrap().0["text"],
            "x".repeat(PAYLOAD_BYTES)
        );
        let ExtendedPointId::NumId(id) = point.id else {
            panic!("the fixture uses numeric ids");
        };
        assert_eq!(
            point.vector,
            Some(VectorStructInternal::Single(vec![
                id as f32,
                (id * id + 1) as f32,
                0.0,
                0.0
            ]))
        );
    }
    if mixed_batch {
        assert_eq!(results[1].len(), LIMIT);
        assert!(
            results[1]
                .iter()
                .all(|p| p.payload.is_none() && p.vector.is_none())
        );
    }
    println!("payload_io_read={payload_reads}, final_page_retrieve_io_read={expected_reads}");
    assert_eq!(
        payload_reads, expected_reads,
        "payload_io_read must cover only the final {LIMIT} points"
    );

    let mut selected = request.clone();
    selected.with_payload = serde_json::from_value(json!(["id"])).unwrap();
    selected.with_vector = WithVector::Selector(vec![String::new()]);
    let (selected_results, selected_reads) = query_batch(&collection, vec![selected]).await;
    assert_eq!(selected_reads, expected_reads);
    assert_eq!(selected_results[0].len(), LIMIT);
    for (point, full) in selected_results[0].iter().zip(&results[0]) {
        assert_eq!(point.id, full.id);
        assert_eq!(point.score, full.score);
        assert_eq!(point.vector, full.vector);
        assert_eq!(point.payload.as_ref().unwrap().len(), 1);
        assert_eq!(
            point.payload.as_ref().unwrap().0["id"],
            full.payload.as_ref().unwrap().0["id"]
        );
    }

    for (payload, vectors) in [(true, false), (false, true)] {
        let mut single_metadata = request.clone();
        single_metadata.with_payload = payload.into();
        single_metadata.with_vector = vectors.into();
        let (points, reads) = query_batch(&collection, vec![single_metadata]).await;
        assert_eq!(reads, if payload { expected_reads } else { 0 });
        assert_eq!(points[0].len(), LIMIT);
        for (point, full) in points[0].iter().zip(&results[0]) {
            assert_eq!((point.id, point.score), (full.id, full.score));
            assert_eq!(
                point.payload,
                if payload { full.payload.clone() } else { None }
            );
            assert_eq!(
                point.vector,
                if vectors { full.vector.clone() } else { None }
            );
        }
    }

    let mut empty = request;
    empty.limit = 0;
    let (empty_results, empty_reads) = query_batch(&collection, vec![empty]).await;
    assert!(empty_results[0].is_empty());
    assert_eq!(empty_reads, 0);
}
