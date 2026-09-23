//! End-to-end check of the `append_only_storages` feature flag: a segment
//! built with it holds Logstore-mode storages, and the ordinary write paths —
//! upserts, payload updates, multi-step same-operation writes, deletes, index
//! builds — run against them.
//!
//! A standalone test binary, because feature flags are process-global: this
//! process runs with `serverless_compatible` on from the start, the way a
//! serverless deployment would.

use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::{FeatureFlags, init_feature_flags};
use common::types::DeferredBehavior;
use segment::data_types::vectors::{VectorRef, only_default_vector};
use segment::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _,
    StorageSegmentEntry as _,
};
use segment::payload_json;
use segment::segment_constructor::load_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadFieldSchema, PayloadSchemaType};
use segment::vector_storage::VectorStorage as _;
use segment::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
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
    let hw_counter = HardwareCounterCell::new();

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
            .upsert_point(1, id.into(), only_default_vector(&vector), &hw_counter)
            .unwrap();
    }
    segment
        .set_full_payload(
            2,
            3.into(),
            &payload_json! { "kind": "updated" },
            &hw_counter,
        )
        .unwrap();

    // A multi-step write within one operation: a slot per step.
    segment
        .upsert_point(3, 6.into(), only_default_vector(&[6.0; DIM]), &hw_counter)
        .unwrap();
    segment
        .set_full_payload(
            3,
            6.into(),
            &payload_json! { "kind": "multi-step" },
            &hw_counter,
        )
        .unwrap();

    // A delete is a tombstone.
    segment.delete_point(4, 5.into(), &hw_counter).unwrap();

    // The index storage comes out append-only too.
    let key = "kind".parse().unwrap();
    segment
        .create_field_index(
            5,
            &key,
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
            &hw_counter,
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
            &hw_counter,
        )
        .unwrap();
    segment
        .set_payload(7, 4.into(), &payload_json! { "num": 7 }, &None, &hw_counter)
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
    let payload = segment.payload(3.into(), &hw_counter).unwrap();
    assert_eq!(payload, payload_json! { "kind": "updated" });
    let payload = segment.payload(6.into(), &hw_counter).unwrap();
    assert_eq!(payload, payload_json! { "kind": "multi-step" });
    assert!(!segment.has_point(5.into(), DeferredBehavior::WithDeferred));
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
    let hw_counter = HardwareCounterCell::new();

    let mut storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
    assert_eq!(storage_mode(&dir.path().join("store")), "append_only");

    let vector = SparseVector::new(vec![1, 3], vec![1.0, 2.0]).unwrap();
    storage
        .insert_vector(0, VectorRef::from(&vector), &hw_counter)
        .unwrap();

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
    let hw_counter = HardwareCounterCell::new();
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
        source
            .upsert_point(1, id.into(), vectors, &hw_counter)
            .unwrap();
        source
            .set_full_payload(2, id.into(), &payload_json! { "id": id }, &hw_counter)
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
        builder.update(&[&source], &stopped, &hw_counter).unwrap();
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
            reloaded.payload(id.into(), &hw_counter).unwrap(),
            payload_json! { "id": id },
        );
        let sparse = reloaded.vector("sparse", id.into(), &hw_counter).unwrap();
        assert!(sparse.is_some(), "sparse vector of point {id}");
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
