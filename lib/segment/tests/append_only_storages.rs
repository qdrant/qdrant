//! End-to-end check of the `append_only_storages` feature flag: a segment
//! built with it holds Logstore-mode storages, and the ordinary write paths —
//! upserts, payload updates, multi-step same-operation writes, deletes, index
//! builds — run against them.
//!
//! A standalone test binary, because feature flags are process-global: this
//! process runs with `serverless_compatible` on from the start, the way a
//! serverless deployment would.

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::flags::{FeatureFlags, init_feature_flags};
use common::types::DeferredBehavior;
use segment::data_types::vectors::{VectorRef, only_default_vector};
use segment::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    StorageSegmentEntry as _,
};
use segment::id_tracker::IdTrackerRead as _;
use segment::index::field_index::PayloadFieldIndexRead as _;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::load_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, FieldCondition, PayloadFieldSchema, PayloadSchemaType};
use segment::vector_storage::VectorStorage as _;
use segment::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use serde_json::json;
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;
use uuid::Uuid;

const DIM: usize = 4;

/// The persisted mode of the Blobstore at `dir`, from its own config file.
fn storage_mode(dir: &Path) -> String {
    let config: serde_json::Value =
        serde_json::from_str(&fs_err::read_to_string(dir.join("config.json")).unwrap()).unwrap();
    config["mode"].as_str().unwrap_or("mutable").to_string()
}

#[test]
fn append_only_storages_serve_the_ordinary_write_paths() {
    // The field is private, set through the same route a config file takes.
    let flags: FeatureFlags = serde_json::from_str(r#"{ "serverless_compatible": true }"#).unwrap();
    init_feature_flags(flags);
    assert!(common::flags::feature_flags().append_only_storages);

    let dir = Builder::new()
        .prefix("append_only_storages")
        .tempdir()
        .unwrap();
    let _scope = ambient::test_guard();

    // The segment lives in a uuid subdirectory of `dir`.
    let mut segment = build_simple_segment(dir.path(), DIM, Distance::Dot).unwrap();
    let segment_path = segment.data_path();
    assert!(segment.append_only_mutations, "forced by the storages flag");

    // The payload storage was created in the append-only mode.
    assert_eq!(
        storage_mode(&segment_path.join("payload_storage")),
        "append_only"
    );

    // Inserts, and an update of an existing point.
    for id in 1..=5u64 {
        let vector: Vec<f32> = vec![id as f32; DIM];
        segment
            .upsert_point(1, id.into(), only_default_vector(&vector))
            .unwrap();
    }
    segment
        .set_full_payload(2, 3.into(), &payload_json! { "kind": "updated" })
        .unwrap();

    // A multi-step write within one operation: a slot per step.
    segment
        .upsert_point(3, 6.into(), only_default_vector(&[6.0; DIM]))
        .unwrap();
    segment
        .set_full_payload(3, 6.into(), &payload_json! { "kind": "multi-step" })
        .unwrap();

    // A delete is a tombstone.
    segment.delete_point(4, 5.into()).unwrap();

    // The index storage comes out append-only too.
    let key = "kind".parse().unwrap();
    segment
        .create_field_index(
            5,
            &key,
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
        )
        .unwrap();
    // The index directory name carries a hash prefix, locate it by suffix.
    let index_dir = fs_err::read_dir(segment_path.join("payload_index"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with("-kind-map"))
        })
        .expect("keyword index directory exists");
    assert_eq!(storage_mode(&index_dir), "append_only");

    // An integer index over a field only some points hold: indexing points
    // without the field, then giving one the field, drives the numeric index's
    // remove-before-add and empty-value paths against the append-only storage.
    let num_key = "num".parse().unwrap();
    segment
        .create_field_index(
            6,
            &num_key,
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)),
        )
        .unwrap();
    segment
        .set_payload(7, 4.into(), &payload_json! { "num": 7 }, &None)
        .unwrap();

    segment.flush(true).unwrap();
    drop(segment);

    // Everything survives a reload through the ordinary loader.
    let mut segment = load_segment(
        &segment_path,
        Uuid::nil(),
        None,
        &AtomicBool::new(false),
        false,
    )
    .unwrap();
    assert!(segment.append_only_mutations);

    // The repair every shard loader runs; deleting the leftovers it finds would
    // fail here, leaving the segment permanently unopenable.
    segment
        .check_consistency_and_repair()
        .expect("append-only leftovers must not be reported as inconsistencies");

    assert_eq!(segment.available_point_count(), 5);
    let payload = segment.payload(3.into()).unwrap();
    assert_eq!(payload, payload_json! { "kind": "updated" });
    let payload = segment.payload(6.into()).unwrap();
    assert_eq!(payload, payload_json! { "kind": "multi-step" });
    assert!(!segment.has_point(5.into(), DeferredBehavior::WithDeferred));
}

/// Element maps must follow their inner indexes into Logstore, including when
/// an index is built over existing points and when writes resume after a reload.
#[rstest::rstest]
#[case(false)]
#[case(true)]
fn nested_indexes_use_append_only_storage(#[case] build_before_points: bool) {
    let flags: FeatureFlags = serde_json::from_str(r#"{ "serverless_compatible": true }"#).unwrap();
    init_feature_flags(flags);
    let _scope = ambient::test_guard();
    let dir = Builder::new()
        .prefix("append_only_nested")
        .tempdir()
        .unwrap();
    let mut segment = build_simple_segment(dir.path(), DIM, Distance::Dot).unwrap();
    let segment_path = segment.data_path();

    let create_indexes = |segment: &mut Segment, op| {
        for (field, kind) in [("offers[].shop", "keyword"), ("offers[].price", "integer")] {
            let schema: PayloadFieldSchema = serde_json::from_value(json!({
                "type": kind, "nested": "offers[]",
            }))
            .unwrap();
            segment
                .create_field_index(op, &field.parse().unwrap(), Some(&schema))
                .unwrap();
        }
    };
    if build_before_points {
        create_indexes(&mut segment, 1);
    }
    for (id, payload) in [
        (
            1u64,
            payload_json! { "offers": [{"shop": "a", "price": 10}, {}] },
        ),
        (2, payload_json! { "offers": [{"shop": "b", "price": 20}] }),
        (3, payload_json! { "offers": [{}] }),
    ] {
        segment
            .upsert_point(2, id.into(), only_default_vector(&[1.0; DIM]))
            .unwrap();
        segment.set_full_payload(2, id.into(), &payload).unwrap();
    }
    if !build_before_points {
        create_indexes(&mut segment, 3);
    }
    segment.flush(true).unwrap();

    let map_dirs: Vec<_> = fs_err::read_dir(segment_path.join("payload_index"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with("-elements")
        })
        .collect();
    // Keyword, integer lookup, and integer range each have their own map.
    assert_eq!(map_dirs.len(), 3);
    for path in &map_dirs {
        assert_eq!(storage_mode(path), "append_only");
    }
    let snapshot = || -> Vec<(PathBuf, Vec<u8>)> {
        map_dirs
            .iter()
            .flat_map(|dir| {
                walkdir::WalkDir::new(dir)
                    .into_iter()
                    .map(Result::unwrap)
                    .filter(|entry| entry.file_type().is_file())
                    .map(|entry| (entry.path().to_owned(), fs_err::read(entry.path()).unwrap()))
            })
            .collect()
    };
    let check_queries = |segment: &Segment| {
        let payload_index = segment.payload_index.borrow();
        let id_tracker = segment.id_tracker.borrow();
        for (condition, expected) in [
            (
                json!({"key": "offers[].shop", "match": {"value": "a"}}),
                vec![1.into()],
            ),
            (
                json!({"key": "offers[].shop", "match": {"value": "b"}}),
                vec![],
            ),
            (
                json!({"key": "offers[].price", "range": {"gte": 10, "lte": 10}}),
                vec![1.into()],
            ),
        ] {
            let condition: FieldCondition = serde_json::from_value(condition).unwrap();
            let mut checked = false;
            for index in &payload_index.field_indexes[&condition.key] {
                if let Some(points) = index.filter(&condition).unwrap() {
                    let mut matches: Vec<_> = points
                        .filter_map(|point| id_tracker.external_id(point))
                        .collect();
                    matches.sort_unstable();
                    matches.dedup();
                    assert_eq!(matches, expected, "{condition:?}");
                    checked = true;
                }
            }
            assert!(checked, "no index supports {condition:?}");
        }
    };

    // Repeat after reopening: old element offsets must remain owned, including
    // those of tombstoned points, so new elements keep increasing their offsets.
    for round in 0..2 {
        let before = snapshot();
        let op = 10 + round * 10;
        let id = 4 + round;
        segment
            .upsert_point(op, id.into(), only_default_vector(&[2.0; DIM]))
            .unwrap();
        segment
            .set_full_payload(
                op,
                id.into(),
                &payload_json! {
                    "offers": [{"shop": "a", "price": 10}],
                },
            )
            .unwrap();
        // Grow, edit one element, shrink, and remove just the indexed field.
        segment
            .set_full_payload(
                op + 1,
                1.into(),
                &payload_json! {
                    "offers": [{"shop": "b", "price": 20}, {}, {"shop": "b", "price": 30}],
                },
            )
            .unwrap();
        segment
            .set_payload(
                op + 2,
                1.into(),
                &payload_json! {
                    "shop": "a", "price": 10,
                },
                &Some("offers[0]".parse().unwrap()),
            )
            .unwrap();
        segment
            .set_full_payload(
                op + 3,
                1.into(),
                &payload_json! {
                    "offers": [{"shop": "a", "price": 10}],
                },
            )
            .unwrap();
        segment
            .delete_payload(op + 4, 1.into(), &"offers[].shop".parse().unwrap())
            .unwrap();
        segment.clear_payload(op + 5, 2.into()).unwrap();
        segment.flush(true).unwrap();

        let before_delete = snapshot();
        segment.delete_point(op + 6, id.into()).unwrap();
        segment.delete_point(op + 6, 3.into()).unwrap();
        segment.flush(true).unwrap();
        for (path, bytes) in before_delete {
            assert_eq!(
                fs_err::read(&path).unwrap(),
                bytes,
                "delete changed {}",
                path.display()
            );
        }

        segment
            .set_full_payload(
                op + 7,
                1.into(),
                &payload_json! {
                    "offers": [{"shop": "a", "price": 10}, {}],
                },
            )
            .unwrap();
        segment.flush(true).unwrap();
        check_queries(&segment);
        for (path, bytes) in before {
            assert!(
                fs_err::read(&path).unwrap().starts_with(&bytes),
                "rewrote {}",
                path.display()
            );
        }

        drop(segment);
        segment = load_segment(
            &segment_path,
            Uuid::nil(),
            None,
            &AtomicBool::new(false),
            false,
        )
        .unwrap();
        segment.check_consistency_and_repair().unwrap();
        check_queries(&segment);
    }
}

/// The sparse vector storage created under the flag is append-only, deleting
/// where nothing was stored is a no-op, and deleting a stored vector fails.
#[test]
fn sparse_storage_is_append_only() {
    let flags: FeatureFlags = serde_json::from_str(r#"{ "serverless_compatible": true }"#).unwrap();
    init_feature_flags(flags);

    let dir = Builder::new()
        .prefix("append_only_sparse")
        .tempdir()
        .unwrap();
    let _scope = ambient::test_guard();

    let mut storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
    assert_eq!(storage_mode(&dir.path().join("store")), "append_only");

    let vector = SparseVector::new(vec![1, 3], vec![1.0, 2.0]).unwrap();
    storage.insert_vector(0, VectorRef::from(&vector)).unwrap();

    // Nothing was ever stored at key 5.
    storage.delete_vector(5).unwrap();

    // Key 0 holds a vector; an append-only storage cannot remove it.
    assert!(storage.delete_vector(0).is_err());
}

/// Tracker files under `dir` with the given name, at any depth.
fn count_files(dir: &Path, name: &str) -> usize {
    fs_err::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                count_files(&entry.path(), name)
            } else {
                usize::from(entry.file_name() == name)
            }
        })
        .sum()
}

/// The optimizer makes the append-only payload storage of a segment it builds non-appendable
/// immutable, which compacts its mapping file. Sparse vector storages, and every storage of an
/// appendable segment, keep the appendable layout.
#[test]
fn optimizer_makes_non_appendable_payload_storage_immutable() {
    use std::collections::HashMap;

    use segment::common::memory_usage::MemoryReporter as _;
    use segment::data_types::named_vectors::NamedVectors;
    use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
    use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
    use segment::segment_constructor::build_segment;
    use segment::segment_constructor::segment_builder::SegmentBuilder;
    use segment::types::{
        HnswGlobalConfig, Indexes, SegmentConfig, SparseVectorDataConfig, SparseVectorStorageType,
        VectorDataConfig, VectorStorageType,
    };

    let flags: FeatureFlags = serde_json::from_str(r#"{ "serverless_compatible": true }"#).unwrap();
    init_feature_flags(flags);
    assert!(common::flags::feature_flags().compact_logstore_tracker);

    let dir = Builder::new().prefix("make_immutable").tempdir().unwrap();
    let temp_dir = Builder::new()
        .prefix("make_immutable_temp")
        .tempdir()
        .unwrap();
    let _scope = ambient::test_guard();
    let stopped = AtomicBool::new(false);

    let config = |storage_type, sparse_index_type| SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Dot,
                storage_type,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: HashMap::from([(
            "sparse".to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig::new(None, sparse_index_type, None, None),
                storage_type: SparseVectorStorageType::default(),
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };
    let appendable = config(VectorStorageType::default(), SparseIndexType::MutableRam);
    let non_appendable = config(VectorStorageType::Mmap, SparseIndexType::Mmap);
    assert!(appendable.is_appendable());
    assert!(!non_appendable.is_appendable());

    let (mut source, _token) = build_segment(dir.path(), &appendable, None, true).unwrap();
    for id in 1..=5u64 {
        let dense = vec![id as f32; DIM];
        let sparse = SparseVector::new(vec![id as u32], vec![id as f32]).unwrap();
        let mut vectors = NamedVectors::from_ref(DEFAULT_VECTOR_NAME, VectorRef::from(&dense));
        vectors.insert_ref("sparse", VectorRef::Sparse(&sparse));
        source.upsert_point(1, id.into(), vectors).unwrap();
        source
            .set_full_payload(2, id.into(), &payload_json! { "id": id })
            .unwrap();
    }

    let build = |config: &SegmentConfig| {
        let mut builder = SegmentBuilder::new(
            temp_dir.path(),
            config,
            &HnswGlobalConfig::default(),
            common::flags::feature_flags(),
        )
        .unwrap();
        builder.update(&[&source], &stopped).unwrap();
        builder.build_for_test(dir.path())
    };

    let built = build(&non_appendable);
    let built_path = built.data_path();
    // The compacted mappings are reported as RAM, beyond the storage files
    let payload_ram = built
        .payload_storage
        .borrow()
        .memory_usage()
        .extra_ram_bytes;
    assert!(payload_ram.unwrap() >= 5 * 16, "{payload_ram:?}");
    assert_eq!(count_files(&built_path, "compacted_tracker.dat"), 1);
    assert_eq!(count_files(&built_path, "log_tracker.dat"), 1);
    assert!(
        built_path
            .join("payload_storage/compacted_tracker.dat")
            .exists()
    );
    assert!(built_path.join("id_tracker.compact_versions").exists());
    assert!(!built_path.join("id_tracker.versions").exists());
    drop(built);

    let reloaded = load_segment(
        &built_path,
        Uuid::nil(),
        None,
        &AtomicBool::new(false),
        false,
    )
    .unwrap();
    assert_eq!(reloaded.available_point_count(), 5);
    for id in 1..=5u64 {
        assert_eq!(
            reloaded.payload(id.into()).unwrap(),
            payload_json! { "id": id },
        );
        let sparse = reloaded.vector("sparse", id.into()).unwrap();
        assert!(sparse.is_some(), "sparse vector of point {id}");
        assert_eq!(reloaded.point_version(id.into()), Some(2));
    }

    let built = build(&appendable);
    let payload_ram = built
        .payload_storage
        .borrow()
        .memory_usage()
        .extra_ram_bytes;
    assert_eq!(payload_ram, Some(0));
    assert_eq!(count_files(&built.data_path(), "compacted_tracker.dat"), 0);
    assert_eq!(count_files(&built.data_path(), "log_tracker.dat"), 2);
}
