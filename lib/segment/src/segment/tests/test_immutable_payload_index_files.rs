//! File-system-level immutability test for payload indices on non-appendable
//! segments. After milestone #49 (immutable payload index), every payload
//! index variant — numeric, map (keyword), bool, geo, text, null — must be
//! frozen on disk once a non-appendable segment is built. This test snapshots
//! the byte content of every file under `payload_index/` after the segment is
//! built, performs operations that previously could mutate index files
//! (point deletions, flushes, segment reload), and asserts that not a single
//! byte changed.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use ordered_float::OrderedFloat;
use serde_json::json;
use tempfile::Builder;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::DEFAULT_VECTOR_NAME;
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    StorageSegmentEntry as _,
};
use crate::json_path::JsonPath;
use crate::segment::Segment;
use crate::segment_constructor::segment_builder::SegmentBuilder;
use crate::segment_constructor::{build_segment, load_segment};
use crate::types::{
    Condition, DateTimePayloadType, Distance, FieldCondition, Filter, GeoBoundingBox, GeoPoint,
    HnswGlobalConfig, Indexes, Match, Payload, PayloadFieldSchema, PayloadSchemaType, Range,
    SegmentConfig, ValueVariants, VectorDataConfig, VectorStorageType,
};

const DIM: usize = 4;
const NUM_POINTS: usize = 20;

/// All `PayloadSchemaType` variants, covering every immutable index variant
/// shipped under milestone #49. Each entry maps to a distinct
/// `payload_index/{field}-{kind}/` subdirectory once the segment is built as
/// immutable. (Datetime is backed by the numeric index; Uuid by the map index.)
const INDEXED_FIELDS: &[(&str, PayloadSchemaType)] = &[
    ("kw_field", PayloadSchemaType::Keyword),
    ("int_field", PayloadSchemaType::Integer),
    ("float_field", PayloadSchemaType::Float),
    ("bool_field", PayloadSchemaType::Bool),
    ("geo_field", PayloadSchemaType::Geo),
    ("text_field", PayloadSchemaType::Text),
    ("dt_field", PayloadSchemaType::Datetime),
    ("uuid_fld", PayloadSchemaType::Uuid),
];

/// Build a payload for point `i`. Values are unique per point for the
/// fields that support a wide value space (int, float, geo, text), so
/// that deleting any subset of points exercises the immutable index
/// loaders' "all live points for a value are deleted" code path on the
/// post-flush reload below.
fn make_payload(i: usize) -> Payload {
    let kw = ["red", "green", "blue"][i % 3];
    let int_val = (i as i64) * 7;
    let float_val = (i as f64) * 1.5;
    let bool_val = i.is_multiple_of(2);
    let lon = -73.9 + (i as f64) * 0.01;
    let lat = 40.7 + (i as f64) * 0.01;
    let text = format!("the quick brown fox number {i} jumps over the lazy dog");
    let dt = format!("2026-01-01T00:00:{:02}Z", i % 60);
    let uuid = format!("00000000-0000-0000-0000-{i:012x}");
    let value = json!({
        "kw_field": kw,
        "int_field": int_val,
        "float_field": float_val,
        "bool_field": bool_val,
        "geo_field": { "lon": lon, "lat": lat },
        "text_field": text,
        "dt_field": dt,
        "uuid_fld": uuid,
    });
    serde_json::from_value(value).unwrap()
}

fn build_immutable_segment_with_indexed_payload(segments_path: &Path, temp_path: &Path) -> Segment {
    let hw = HardwareCounterCell::new();

    // Step 1: appendable source segment with payload + field indices.
    let source_dir = Builder::new().prefix("source_seg").tempdir().unwrap();
    let mut source = build_segment(
        source_dir.path(),
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: DIM,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::default(),
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        },
        None,
        true,
    )
    .unwrap();

    for i in 0..NUM_POINTS {
        let vec = vec![(i + 1) as f32; DIM];
        let vectors = NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vec.as_slice().into());
        let point_id = (i as u64 + 1).into();
        let op_num = (i + 1) as u64;
        source.upsert_point(op_num, point_id, vectors, &hw).unwrap();
        source
            .set_full_payload(op_num, point_id, &make_payload(i), &hw)
            .unwrap();
    }

    for (offset, (name, schema)) in INDEXED_FIELDS.iter().enumerate() {
        let op_num = NUM_POINTS as u64 + 1 + offset as u64;
        source
            .create_field_index(
                op_num,
                &JsonPath::new(name),
                Some(&PayloadFieldSchema::FieldType(*schema)),
                &hw,
            )
            .unwrap();
    }

    // Step 2: build a non-appendable target via SegmentBuilder. Mmap vector
    // storage forces `is_appendable() = false`; the indexed fields are carried
    // over from the source segment automatically.
    let target_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Dot,
                storage_type: VectorStorageType::Mmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    assert!(!target_config.is_appendable());

    let mut builder =
        SegmentBuilder::new(temp_path, &target_config, &HnswGlobalConfig::default()).unwrap();
    builder
        .update(&[&source], &AtomicBool::new(false), &hw)
        .unwrap();

    let segment = builder.build_for_test(segments_path);
    assert!(!segment.appendable_flag);
    assert_eq!(segment.available_point_count(), NUM_POINTS);
    segment
}

/// Recursively collect (relative path -> file bytes) for every file under
/// `root`. Sorted by path for stable diffing.
fn snapshot_dir(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs_err::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let file_type = entry.file_type().unwrap();
            let path = entry.path();
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                let rel = path.strip_prefix(root).unwrap().to_path_buf();
                let bytes = fs_err::read(&path).unwrap();
                out.insert(rel, bytes);
            }
        }
    }
    out
}

fn assert_snapshots_equal(
    before: &BTreeMap<PathBuf, Vec<u8>>,
    after: &BTreeMap<PathBuf, Vec<u8>>,
    label: &str,
) {
    let before_paths: Vec<_> = before.keys().collect();
    let after_paths: Vec<_> = after.keys().collect();
    assert_eq!(
        before_paths, after_paths,
        "[{label}] payload_index file set changed",
    );
    for (path, before_bytes) in before {
        let after_bytes = &after[path];
        assert_eq!(
            before_bytes.len(),
            after_bytes.len(),
            "[{label}] file size changed for {}",
            path.display(),
        );
        assert!(
            before_bytes == after_bytes,
            "[{label}] file content changed for {}",
            path.display(),
        );
    }
}

/// One indexed-field query plus a predicate that decides which payload index
/// `i` is supposed to match. Used to cross-check `read_filtered` results
/// against the live point set: the number of points the segment returns must
/// equal `live.iter().enumerate().filter(|(i, &alive)| alive && pred(i)).count()`.
struct IndexedQuery {
    label: &'static str,
    filter: Filter,
    matches: fn(usize) -> bool,
}

fn must(condition: FieldCondition) -> Filter {
    Filter::new_must(Condition::Field(condition))
}

/// Build one query per indexed field, exercising a different read path of each
/// immutable index variant: map exact-match (keyword/uuid), numeric range
/// (float), datetime range, geo bounding box, full-text token match, bool
/// match, integer exact-match.
fn indexed_queries() -> Vec<IndexedQuery> {
    vec![
        // Map index — exact keyword match. "red" appears at i % 3 == 0.
        IndexedQuery {
            label: "kw_field=red (map exact)",
            filter: must(FieldCondition::new_match(
                JsonPath::new("kw_field"),
                Match::new_value(ValueVariants::String("red".into())),
            )),
            matches: |i| i.is_multiple_of(3),
        },
        // Map index, integer-keyed exact-match path. Only point i=1 has
        // int_field == 7.
        IndexedQuery {
            label: "int_field=7 (map int exact)",
            filter: must(FieldCondition::new_match(
                JsonPath::new("int_field"),
                Match::from(7_i64),
            )),
            matches: |i| (i as i64) * 7 == 7,
        },
        // Numeric (float) range: 3.0 <= i*1.5 <= 7.5 → i in {2,3,4,5}.
        IndexedQuery {
            label: "float_field in [3.0, 7.5] (numeric range)",
            filter: must(FieldCondition::new_range(
                JsonPath::new("float_field"),
                Range {
                    lt: None,
                    gt: None,
                    gte: Some(OrderedFloat(3.0)),
                    lte: Some(OrderedFloat(7.5)),
                },
            )),
            matches: |i| {
                let v = (i as f64) * 1.5;
                (3.0..=7.5).contains(&v)
            },
        },
        // Bool index — match true (every even i).
        IndexedQuery {
            label: "bool_field=true (bool exact)",
            filter: must(FieldCondition::new_match(
                JsonPath::new("bool_field"),
                Match::from(true),
            )),
            matches: |i| i.is_multiple_of(2),
        },
        // Geo index — bounding box that strictly contains points i in {0..5}.
        // GeoBoundingBox uses strict inequalities (see `check_point` in
        // `types.rs`).
        IndexedQuery {
            label: "geo_field in tight bbox (geo)",
            filter: must(FieldCondition::new_geo_bounding_box(
                JsonPath::new("geo_field"),
                GeoBoundingBox {
                    top_left: GeoPoint::new(-73.905, 40.745).unwrap(),
                    bottom_right: GeoPoint::new(-73.855, 40.695).unwrap(),
                },
            )),
            matches: |i| i < 5,
        },
        // Text inverted index — every payload contains the token "fox".
        IndexedQuery {
            label: "text_field token=fox (text)",
            filter: must(FieldCondition::new_match(
                JsonPath::new("text_field"),
                Match::new_text("fox"),
            )),
            matches: |_| true,
        },
        // Datetime numeric range: seconds in [05, 10] → i in {5..=10}.
        IndexedQuery {
            label: "dt_field in [00:05, 00:10] (datetime range)",
            filter: must(FieldCondition::new_datetime_range(
                JsonPath::new("dt_field"),
                Range {
                    lt: None,
                    gt: None,
                    gte: Some(DateTimePayloadType::from_str("2026-01-01T00:00:05Z").unwrap()),
                    lte: Some(DateTimePayloadType::from_str("2026-01-01T00:00:10Z").unwrap()),
                },
            )),
            matches: |i| (5..=10).contains(&(i % 60)),
        },
        // Map index, uuid exact-match. Each uuid is unique; pick the one for
        // i = 7.
        IndexedQuery {
            label: "uuid_fld=...0007 (uuid exact)",
            filter: must(FieldCondition::new_match(
                JsonPath::new("uuid_fld"),
                Match::new_value(ValueVariants::String(format!(
                    "00000000-0000-0000-0000-{:012x}",
                    7
                ))),
            )),
            matches: |i| i == 7,
        },
    ]
}

/// For each indexed query, run `read_filtered` and assert the returned count
/// matches the count predicted from the live set.
fn assert_query_counts(segment: &Segment, live: &[bool], queries: &[IndexedQuery], stage: &str) {
    let hw = HardwareCounterCell::new();
    for q in queries {
        let actual = segment
            .read_filtered(
                None,
                None,
                Some(&q.filter),
                &AtomicBool::new(false),
                &hw,
                DeferredBehavior::IncludeAll,
            )
            .unwrap();
        let expected = (0..NUM_POINTS)
            .filter(|&i| live[i] && (q.matches)(i))
            .count();
        assert_eq!(
            actual.len(),
            expected,
            "[{stage}] query {:?} returned {} points, expected {}",
            q.label,
            actual.len(),
            expected,
        );
    }
}

/// Sanity check: every indexed field has at least one matching subdirectory
/// under `payload_index/`, plus the per-field `-null` sidecar. Catches
/// regressions where an index type silently fails to materialize files.
///
/// Directory names look like `<segment_hash>-<truncated_field>-<kind>`, so we
/// match by a prefix of the field name (truncated to 8 chars to mirror the
/// on-disk naming).
fn assert_all_index_dirs_present(payload_index_dir: &Path) {
    let entries: Vec<_> = fs_err::read_dir(payload_index_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    for (field, schema) in INDEXED_FIELDS {
        let field_key = &field[..field.len().min(8)];
        assert!(
            entries.iter().any(|name| name.contains(field_key)),
            "no payload_index subdir for field {field}; got {entries:?}",
        );
        // Null index is materialized as a sidecar for every indexed field.
        let null_marker = format!("{field_key}-null");
        assert!(
            entries.iter().any(|name| name.ends_with(&null_marker)),
            "no -null sidecar for field {field} (schema {schema:?}); got {entries:?}",
        );
    }
}

#[test]
fn payload_index_files_are_immutable_after_build() {
    let segments_dir = Builder::new().prefix("segments").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("builder_tmp").tempdir().unwrap();

    let mut segment =
        build_immutable_segment_with_indexed_payload(segments_dir.path(), temp_dir.path());
    let segment_path = segment.data_path();
    let segment_uuid = segment.uuid;
    let payload_index_dir = segment_path.join("payload_index");

    assert!(
        payload_index_dir.is_dir(),
        "payload_index/ missing under {}",
        segment_path.display(),
    );
    assert_all_index_dirs_present(&payload_index_dir);

    // Baseline: capture every byte under payload_index/ right after build,
    // and the per-index query counts on the full live set.
    let baseline = snapshot_dir(&payload_index_dir);
    assert!(!baseline.is_empty(), "payload_index/ contained no files");
    let queries = indexed_queries();
    let mut live = vec![true; NUM_POINTS];
    assert_query_counts(&segment, &live, &queries, "baseline");

    // Operation 1: runtime point deletions. delete_point lives on
    // NonAppendableSegmentEntry — it routes through the in-memory id_tracker
    // bitvec only and must not touch payload_index/ files.
    let hw = HardwareCounterCell::new();
    let mut op_num = 1_000_u64;
    for (i, alive) in live.iter_mut().enumerate().take(NUM_POINTS / 2) {
        segment
            .delete_point(op_num, ((i as u64) + 1).into(), &hw)
            .unwrap();
        *alive = false;
        op_num += 1;
    }
    assert_eq!(segment.available_point_count(), NUM_POINTS - NUM_POINTS / 2);

    let after_deletes = snapshot_dir(&payload_index_dir);
    assert_snapshots_equal(&baseline, &after_deletes, "after delete_point");
    assert_query_counts(&segment, &live, &queries, "after delete_point");

    // Operation 2: flush. Persists id_tracker / segment state changes; must
    // not touch payload_index/ files.
    segment.flush(true).unwrap();
    let after_flush = snapshot_dir(&payload_index_dir);
    assert_snapshots_equal(&baseline, &after_flush, "after flush");
    assert_query_counts(&segment, &live, &queries, "after flush");

    // Operation 3: drop and reload. Opening an immutable segment must not
    // rewrite any index files, and the immutable index loaders must rehydrate
    // an index state that still answers queries correctly given the runtime
    // deletion bitvec.
    drop(segment);
    let mut reloaded =
        load_segment(&segment_path, segment_uuid, None, &AtomicBool::new(false)).unwrap();
    let after_reload = snapshot_dir(&payload_index_dir);
    assert_snapshots_equal(&baseline, &after_reload, "after reload");
    assert_query_counts(&reloaded, &live, &queries, "after reload");

    // Operation 4: more deletes + flush after reload. Same expectation.
    for (i, alive) in live.iter_mut().enumerate().skip(NUM_POINTS / 2) {
        reloaded
            .delete_point(op_num, ((i as u64) + 1).into(), &hw)
            .unwrap();
        *alive = false;
        op_num += 1;
    }
    reloaded.flush(true).unwrap();
    let after_second_flush = snapshot_dir(&payload_index_dir);
    assert_snapshots_equal(
        &baseline,
        &after_second_flush,
        "after delete + flush on reloaded segment",
    );
    assert_query_counts(
        &reloaded,
        &live,
        &queries,
        "after delete + flush on reloaded segment",
    );
}
