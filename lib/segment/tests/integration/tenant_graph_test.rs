#![allow(deprecated)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use common::types::TelemetryDetail;
use rand::SeedableRng;
use rand::prelude::StdRng;
use segment::data_types::index::{KeywordIndexParams, KeywordIndexType};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::{NonAppendableSegmentEntry, SegmentEntry};
use segment::fixtures::payload_fixtures::random_vector;
use segment::index::VectorIndexRead;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Match,
    PayloadFieldSchema, PayloadSchemaParams, SearchParams, SeqNumberType,
};
use tempfile::Builder;

#[test]
fn test_tenant_graph_with_second_condition() {
    let stopped = AtomicBool::new(false);
    let dim = 8;
    let num_vectors = 4_000;
    let rare_every = 50;
    let top = 10;
    let attempts = 20;

    let mut rng = StdRng::seed_from_u64(42);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let _hw = ambient::test_guard();

    let mut segment = build_simple_segment(dir.path(), dim, Distance::Cosine).unwrap();
    for n in 0..num_vectors {
        let product = if n % rare_every == 1 {
            "rare"
        } else {
            "common"
        };
        let vector = random_vector(&mut rng, dim);
        segment
            .upsert_point(n, n.into(), only_default_vector(&vector))
            .unwrap();
        let payload = payload_json! {"tenant": "A", "product": product};
        segment.set_full_payload(n, n.into(), &payload).unwrap();
    }
    for (seq, field, is_tenant) in [
        (num_vectors, "tenant", true),
        (num_vectors + 1, "product", false),
    ] {
        let schema =
            PayloadFieldSchema::FieldParams(PayloadSchemaParams::Keyword(KeywordIndexParams {
                r#type: KeywordIndexType::Keyword,
                is_tenant: Some(is_tenant),
                on_disk: None,
                memory: None,
                enable_hnsw: Some(is_tenant),
                prefix: None,
            }));
        segment
            .create_field_index(seq as SeqNumberType, &JsonPath::new(field), Some(&schema))
            .unwrap();
    }

    let vector_data = &segment.vector_data[DEFAULT_VECTOR_NAME];
    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_data.vector_storage.clone(),
            quantized_vectors: vector_data.quantized_vectors.clone(),
            payload_index: segment.payload_index.clone(),
            hnsw_config: HnswConfig {
                memory: None,
                m: 0,
                ef_construct: 32,
                full_scan_threshold: 0,
                max_indexing_threads: 1,
                on_disk: Some(false),
                payload_m: Some(8),
                inline_storage: None,
            },
        },
        VectorIndexBuildArgs {
            permit: Arc::new(ResourcePermit::dummy(1)),
            old_indices: &[],
            gpu_device: None,
            rng: &mut rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            inline_vectors: false,
            progress: ProgressTracker::new_for_test(),
        },
    )
    .unwrap();

    let filter = Filter {
        must: Some(
            [("tenant", "A"), ("product", "rare")]
                .map(|(key, value)| {
                    Condition::Field(FieldCondition::new_match(
                        JsonPath::new(key),
                        Match::from(value.to_string()),
                    ))
                })
                .to_vec(),
        ),
        ..Default::default()
    };
    let params = SearchParams {
        hnsw_ef: Some(64),
        ..Default::default()
    };
    for _ in 0..attempts {
        let query = QueryVector::from(random_vector(&mut rng, dim));
        let result = hnsw_index
            .search(
                &[&query],
                Some(&filter),
                top,
                Some(&params),
                &Default::default(),
            )
            .unwrap();
        assert_eq!(result[0].len(), top);
    }

    let telemetry = hnsw_index.get_telemetry_data(TelemetryDetail::default());
    assert_eq!(telemetry.filtered_large_cardinality.count, attempts);
}
