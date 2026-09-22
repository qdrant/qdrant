#![allow(deprecated)]

//! Regression test for filtered search on payload-block-only graphs (`m = 0`).
//!
//! Each payload block used to register exactly one entry point, and
//! `GraphLayers::search` returned an empty result when that entry point did not
//! satisfy the full filter. A filter combining the tenant condition with a second
//! condition therefore returned nothing for some tenants although matching points
//! existed, deterministically, depending on which point had been drawn as the
//! block's entry point.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use common::types::TelemetryDetail;
use rand::SeedableRng;
use rand::prelude::StdRng;
use rstest::rstest;
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

fn keyword_index(is_tenant: bool, enable_hnsw: bool) -> PayloadFieldSchema {
    PayloadFieldSchema::FieldParams(PayloadSchemaParams::Keyword(KeywordIndexParams {
        r#type: KeywordIndexType::Keyword,
        is_tenant: Some(is_tenant),
        on_disk: None,
        memory: None,
        enable_hnsw: Some(enable_hnsw),
        prefix: None,
    }))
}

/// `full_scan_threshold_kb = 1` gives a threshold of 32 points for 8-dim f32 vectors, so
/// 80 matching points route to the graph and the block's extra entry points must cover
/// the second condition. `full_scan_threshold_kb = 0` routes everything to the graph
/// with a single entry point per block, so the plain-search fallback must kick in.
#[rstest]
#[case::extra_entry_points(1, 80)]
#[case::plain_fallback(0, 2)]
fn test_tenant_graph_with_second_condition(
    #[case] full_scan_threshold_kb: usize,
    #[case] num_rare: usize,
) {
    let stopped = AtomicBool::new(false);

    let dim = 8;
    let num_vectors: u64 = 4_000;
    let top = num_rare.min(10);
    let attempts = 20;

    let mut rng = StdRng::seed_from_u64(42);

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let hw_counter = HardwareCounterCell::new();
    let mut segment = build_simple_segment(dir.path(), dim, Distance::Cosine).unwrap();
    let rare_every = num_vectors as usize / num_rare;
    for n in 0..num_vectors {
        let vector = random_vector(&mut rng, dim);
        // Offset by one so that the first point of the block, which becomes its entry
        // point, never carries the rare value: that is the production failure mode.
        let product = if (n as usize) % rare_every == 1 {
            "rare"
        } else {
            "common"
        };
        let payload = payload_json! {"tenant": "A", "product": product};
        segment
            .upsert_point(
                n as SeqNumberType,
                n.into(),
                only_default_vector(&vector),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, n.into(), &payload, &hw_counter)
            .unwrap();
    }

    // Tenant field carries the only HNSW links; the second field is indexed for
    // filtering but has no links of its own (`enable_hnsw = false`).
    segment
        .create_field_index(
            num_vectors,
            &JsonPath::new("tenant"),
            Some(&keyword_index(true, true)),
            &hw_counter,
        )
        .unwrap();
    segment
        .create_field_index(
            num_vectors + 1,
            &JsonPath::new("product"),
            Some(&keyword_index(false, false)),
            &hw_counter,
        )
        .unwrap();

    let payload_index_ptr = segment.payload_index.clone();
    let vector_storage = &segment.vector_data[DEFAULT_VECTOR_NAME].vector_storage;
    let quantized_vectors = &segment.vector_data[DEFAULT_VECTOR_NAME].quantized_vectors;

    let hnsw_config = HnswConfig {
        memory: None,
        m: 0,
        ef_construct: 32,
        full_scan_threshold: full_scan_threshold_kb,
        max_indexing_threads: 1,
        on_disk: Some(false),
        payload_m: Some(8),
        inline_storage: None,
    };

    let permit = Arc::new(ResourcePermit::dummy(1));
    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            quantized_vectors: quantized_vectors.clone(),
            payload_index: payload_index_ptr.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
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
        must: Some(vec![
            Condition::Field(FieldCondition::new_match(
                JsonPath::new("tenant"),
                Match::from("A".to_string()),
            )),
            Condition::Field(FieldCondition::new_match(
                JsonPath::new("product"),
                Match::from("rare".to_string()),
            )),
        ]),
        ..Default::default()
    };

    let mut empty = 0;
    for _ in 0..attempts {
        let query: QueryVector = random_vector(&mut rng, dim).into();
        let index_result = hnsw_index
            .search(
                &[&query],
                Some(&filter),
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(64),
                    ..Default::default()
                }),
                &Default::default(),
            )
            .unwrap();
        let plain_result = segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[&query], Some(&filter), top, None, &Default::default())
            .unwrap();
        assert_eq!(
            plain_result[0].len(),
            top,
            "plain search must find every matching point"
        );
        if index_result[0].is_empty() {
            empty += 1;
        }
    }

    // The graph path must have been exercised, not the small-cardinality plain path.
    let telemetry = hnsw_index.get_telemetry_data(TelemetryDetail::default());
    assert_eq!(
        telemetry.filtered_large_cardinality.count, attempts,
        "searches were expected to route through the HNSW graph"
    );
    assert_eq!(
        empty, 0,
        "{empty} of {attempts} filtered searches returned nothing while {num_rare} points match"
    );
}
