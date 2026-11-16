//! Heap used by one filtered HNSW search on a cold payload index.
//! Own test target: it installs a peak-tracking global allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::ambient;
use common::flags::FeatureFlags;
use ordered_float::OrderedFloat;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::index::{FloatIndexParams, FloatIndexType};
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::NonAppendableSegmentEntry;
use segment::entry::entry_point::{ReadSegmentEntry, SegmentEntry};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Indexes, Memory,
    PayloadFieldSchema, PayloadSchemaParams, PayloadStorageType, Range, SegmentConfig,
    VectorDataConfig, VectorStorageType, WithPayload, WithVector,
};
use tempfile::Builder;

struct PeakAlloc;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static GLOBAL: PeakAlloc = PeakAlloc;

const DIM: usize = 8;
const NUM_POINTS: usize = 20_000;
const FIELD: &str = "f";

fn segment_config(index: Indexes) -> SegmentConfig {
    SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Dot,
                storage_type: VectorStorageType::InRamChunkedMmap,
                index,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: PayloadStorageType::default(),
        id_tracker_memory: None,
    }
}

fn indexed_segment(dir: &std::path::Path) -> Segment {
    let mut rng = StdRng::seed_from_u64(42);
    let plain_dir = Builder::new().prefix("plain").tempdir().unwrap();
    let (mut plain, _) = build_segment(
        plain_dir.path(),
        &segment_config(Indexes::Plain {}),
        None,
        true,
    )
    .unwrap();
    for i in 0..NUM_POINTS {
        let vector: Vec<f32> = (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect();
        let op = i as u64 + 1;
        plain
            .upsert_point(op, (i as u64).into(), only_default_vector(&vector))
            .unwrap();
        let value: f64 = rng.random_range(0.0..1.0);
        plain
            .set_full_payload(op, (i as u64).into(), &payload_json! {FIELD: value})
            .unwrap();
    }
    let schema = PayloadFieldSchema::FieldParams(PayloadSchemaParams::Float(FloatIndexParams {
        r#type: FloatIndexType::Float,
        memory: Some(Memory::Cold),
        enable_hnsw: Some(false),
        ..Default::default()
    }));
    plain
        .create_field_index(NUM_POINTS as u64 + 1, &JsonPath::new(FIELD), Some(&schema))
        .unwrap();

    #[expect(deprecated, reason = "HnswConfig still carries the on_disk flag")]
    let hnsw = HnswConfig {
        m: 16,
        ef_construct: 32,
        full_scan_threshold: 1,
        max_indexing_threads: 2,
        on_disk: None,
        memory: None,
        payload_m: None,
        inline_storage: None,
    };
    let temp_dir = Builder::new().prefix("builder").tempdir().unwrap();
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &segment_config(Indexes::Hnsw(hnsw)),
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    builder.update(&[&plain], &AtomicBool::new(false)).unwrap();
    builder.build_for_test(dir)
}

fn search(segment: &Segment, filter: &Filter) {
    let query = QueryVector::from(vec![0.5f32; DIM]);
    let query_context = QueryContext::default();
    segment
        .search_batch(
            DEFAULT_VECTOR_NAME,
            &[&query],
            &WithPayload::default(),
            &WithVector::Bool(false),
            Some(filter),
            10,
            None,
            &query_context.get_segment_query_context(),
        )
        .unwrap();
}

/// A graph search checks points one at a time, so it must not need heap
/// proportional to the number of points matching a wide filter.
#[test]
fn wide_filter_graph_search_heap_does_not_scale_with_matches() {
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment").tempdir().unwrap();
    let segment = indexed_segment(dir.path());

    // Matches ~99% of points.
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
        JsonPath::new(FIELD),
        Range {
            lt: None,
            gt: None,
            gte: Some(OrderedFloat(0.01)),
            lte: None,
        },
    )));

    // Warm pools and lazily opened files.
    search(&segment, &filter);

    let base = CURRENT.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    search(&segment, &filter);
    let extra = PEAK.load(Ordering::Relaxed) - base;

    eprintln!("peak extra heap per search: {extra} bytes, {NUM_POINTS} points");
    assert!(
        extra < NUM_POINTS,
        "filtered graph search allocated {extra} bytes for {NUM_POINTS} points"
    );
}
