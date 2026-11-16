//! Heap used by one filtered HNSW search on a cold payload index.
//! Installs a global allocator for the integration test binary that only counts the
//! allocations of a thread while it is tracked.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::flags::FeatureFlags;
use ordered_float::OrderedFloat;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::index::{FloatIndexParams, FloatIndexType, TextIndexParams};
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
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Indexes, Match,
    Memory, PayloadFieldSchema, PayloadSchemaParams, PayloadStorageType, Range, SegmentConfig,
    VectorDataConfig, VectorStorageType, WithPayload, WithVector,
};
use tempfile::Builder;

/// Counts the heap of the current thread only, and only while it is tracked, so tests
/// running in parallel in the same binary don't disturb the measurement.
struct ThreadPeakAlloc;

thread_local! {
    static TRACKED: Cell<bool> = const { Cell::new(false) };
    static CURRENT: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for ThreadPeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && TRACKED.get() {
            let now = CURRENT.get() + layout.size() as isize;
            CURRENT.set(now);
            PEAK.set(PEAK.get().max(now));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        if TRACKED.get() {
            CURRENT.set(CURRENT.get() - layout.size() as isize);
        }
    }
}

#[global_allocator]
static GLOBAL: ThreadPeakAlloc = ThreadPeakAlloc;

/// Peak heap allocated by `f` on this thread above what was live when it started.
/// `realloc` falls back to alloc + copy + dealloc, so growing a buffer counts both.
fn peak_heap(f: impl FnOnce()) -> usize {
    CURRENT.set(0);
    PEAK.set(0);
    TRACKED.set(true);
    f();
    TRACKED.set(false);
    PEAK.get() as usize
}

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
    let schema = PayloadFieldSchema::FieldParams(PayloadSchemaParams::Float(FloatIndexParams {
        r#type: FloatIndexType::Float,
        memory: Some(Memory::Cold),
        enable_hnsw: Some(false),
        ..Default::default()
    }));
    let value = |rng: &mut StdRng| payload_json! {FIELD: rng.random_range(0.0..1.0)};
    indexed_segment_with(dir, NUM_POINTS, value, &schema)
}

/// HNSW segment of `num_points` points with `payload` in `FIELD`, indexed by `schema`.
fn indexed_segment_with(
    dir: &std::path::Path,
    num_points: usize,
    payload: impl Fn(&mut StdRng) -> segment::types::Payload,
    schema: &PayloadFieldSchema,
) -> Segment {
    let mut rng = StdRng::seed_from_u64(42);
    let plain_dir = Builder::new().prefix("plain").tempdir().unwrap();
    let (mut plain, _) = build_segment(
        plain_dir.path(),
        &segment_config(Indexes::Plain {}),
        None,
        true,
    )
    .unwrap();
    for i in 0..num_points {
        let vector: Vec<f32> = (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect();
        let op = i as u64 + 1;
        plain
            .upsert_point(op, (i as u64).into(), only_default_vector(&vector))
            .unwrap();
        let payload = payload(&mut rng);
        plain
            .set_full_payload(op, (i as u64).into(), &payload)
            .unwrap();
    }
    plain
        .create_field_index(num_points as u64 + 1, &JsonPath::new(FIELD), Some(schema))
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

    let extra = peak_heap(|| search(&segment, &filter));

    eprintln!("peak extra heap per search: {extra} bytes, {NUM_POINTS} points");
    assert!(
        extra < NUM_POINTS,
        "filtered graph search allocated {extra} bytes for {NUM_POINTS} points"
    );
}

/// A graph search on a cold on-disk text index reads the posting list of a wide text
/// filter into one bitset, without collecting the matches into a list first: one bitset
/// (1 bit per point) and a few chunks of extra heap, not 4-12 bytes per match.
#[test]
fn wide_text_filter_graph_search_heap_is_one_bitset() {
    const POINTS: usize = 500_000;
    let _scope = ambient::test_guard();
    let dir = Builder::new().prefix("segment").tempdir().unwrap();
    // ~99% of the points hold the token `common`.
    let segment = indexed_segment_with(
        dir.path(),
        POINTS,
        |rng| {
            let text = if rng.random_bool(0.99) {
                "common word"
            } else {
                "word"
            };
            payload_json! {FIELD: text}
        },
        &PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
            memory: Some(Memory::Cold),
            ..TextIndexParams::default()
        })),
    );
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new(FIELD),
        Match::new_text("common"),
    )));

    // Warm pools and lazily opened files.
    search(&segment, &filter);

    let extra = peak_heap(|| search(&segment, &filter));

    let bitset = POINTS.div_ceil(8);
    eprintln!("peak extra heap per search: {extra} bytes, bitset {bitset} bytes, {POINTS} points");
    // The search itself (result heaps, pooled visited lists, ...) takes a few KB.
    assert!(
        extra < bitset + 32 * 1024,
        "filtered graph search allocated {extra} bytes for a {bitset} byte bitset",
    );
}
