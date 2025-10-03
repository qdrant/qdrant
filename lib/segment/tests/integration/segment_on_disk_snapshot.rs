use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::budget::ResourcePermit;
use common::tar_ext;
use fs_err as fs;
use fs_err::File;
use rstest::rstest;
use segment::data_types::index::{IntegerIndexParams, KeywordIndexParams};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::entry::snapshot_entry::SnapshotEntry as _;
use segment::index::hnsw_index::num_rayon_threads;
use segment::json_path::JsonPath;
use segment::segment::Segment;
use segment::segment_constructor::load_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Distance, HnswConfig, Indexes, PayloadFieldSchema, PayloadSchemaParams, PayloadStorageType,
    SegmentConfig, SnapshotFormat, VectorDataConfig, VectorStorageType,
};
use tempfile::Builder;

/// This test tests snapshotting and restoring a segment with all on-disk components.
#[rstest]
#[case::regular(SnapshotFormat::Regular)]
#[case::streamable(SnapshotFormat::Streamable)]
fn test_on_disk_segment_snapshot(#[case] format: SnapshotFormat) {
    use common::counter::hardware_counter::HardwareCounterCell;
    use segment::types::HnswGlobalConfig;

    let _ = env_logger::builder().is_test(true).try_init();

    let data = r#"
    {
        "names": ["John Doe", "Bill Murray"],
        "ages": [43, 51],
        "metadata": {
            "height": 50,
            "width": 60
        }
    }"#;

    let segment_builder_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment = build_simple_segment(segment_builder_dir.path(), 2, Distance::Dot).unwrap();

    let hw_counter = HardwareCounterCell::new();

    segment
        .upsert_point(0, 0.into(), only_default_vector(&[1.0, 1.0]), &hw_counter)
        .unwrap();
    segment
        .upsert_point(1, 1.into(), only_default_vector(&[2.0, 2.0]), &hw_counter)
        .unwrap();

    segment
        .set_full_payload(
            2,
            0.into(),
            &serde_json::from_str(data).unwrap(),
            &hw_counter,
        )
        .unwrap();
    segment
        .set_full_payload(
            3,
            0.into(),
            &serde_json::from_str(data).unwrap(),
            &hw_counter,
        )
        .unwrap();

    segment
        .create_field_index(
            4,
            &JsonPath::new("names"),
            Some(&PayloadFieldSchema::FieldParams(
                PayloadSchemaParams::Keyword(KeywordIndexParams {
                    r#type: segment::data_types::index::KeywordIndexType::Keyword,
                    is_tenant: None,
                    on_disk: Some(true),
                }),
            )),
            &hw_counter,
        )
        .unwrap();
    segment
        .create_field_index(
            5,
            &JsonPath::new("ages"),
            Some(&PayloadFieldSchema::FieldParams(
                PayloadSchemaParams::Integer(IntegerIndexParams {
                    r#type: segment::data_types::index::IntegerIndexType::Integer,
                    lookup: Some(true),
                    range: Some(true),
                    is_principal: None,
                    on_disk: Some(true),
                }),
            )),
            &hw_counter,
        )
        .unwrap();

    let segment_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: 2,
                distance: Distance::Dot,
                storage_type: VectorStorageType::Mmap, // mmap vectors
                index: Indexes::Hnsw(HnswConfig {
                    m: 4,
                    ef_construct: 16,
                    full_scan_threshold: 8,
                    max_indexing_threads: 2,
                    on_disk: Some(true), // mmap index
                    payload_m: None,
                    copy_vectors: None,
                }),
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: PayloadStorageType::Mmap,
    };

    let segment_base_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment_builder_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment_builder = SegmentBuilder::new(
        segment_base_dir.path(),
        segment_builder_dir.path(),
        &segment_config,
        &HnswGlobalConfig::default(),
    )
    .unwrap();
    segment_builder.update(&[&segment], &false.into()).unwrap();
    let mut rng = rand::rng();
    let segment = segment_builder
        .build(
            ResourcePermit::dummy(num_rayon_threads(0) as u32),
            &false.into(),
            &mut rng,
            &HardwareCounterCell::new(),
        )
        .unwrap();

    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    // The segment snapshot is a part of a parent collection/shard snapshot.
    let parent_snapshot_tar = Builder::new()
        .prefix("parent_snapshot")
        .suffix(".tar")
        .tempfile()
        .unwrap();
    let segment_id = segment
        .current_path
        .file_stem()
        .and_then(|f| f.to_str())
        .unwrap();

    // snapshotting!
    let tar =
        tar_ext::BuilderExt::new_seekable_owned(File::create(parent_snapshot_tar.path()).unwrap());
    segment
        .take_snapshot(temp_dir.path(), &tar, format, None)
        .unwrap();
    tar.blocking_finish().unwrap();

    let parent_snapshot_unpacked = Builder::new().prefix("parent_snapshot").tempdir().unwrap();
    tar::Archive::new(File::open(parent_snapshot_tar.path()).unwrap())
        .unpack(parent_snapshot_unpacked.path())
        .unwrap();

    // Should be exactly one entry in the snapshot.
    let mut entries = fs::read_dir(parent_snapshot_unpacked.path()).unwrap();
    let entry = entries.next().unwrap().unwrap();
    assert!(entries.next().is_none());

    match format {
        SnapshotFormat::Ancient => unreachable!("The old days are gone"),
        SnapshotFormat::Regular => {
            assert_eq!(entry.file_name(), format!("{segment_id}.tar").as_str());
            assert!(entry.path().is_file());
        }
        SnapshotFormat::Streamable => {
            assert_eq!(entry.file_name(), segment_id);
            assert!(entry.path().is_dir());
        }
    }

    // restore snapshot
    Segment::restore_snapshot_in_place(&entry.path()).unwrap();

    // Should be exactly one entry in the snapshot.
    let mut entries = fs::read_dir(parent_snapshot_unpacked.path()).unwrap();
    let entry = entries.next().unwrap().unwrap();
    assert!(entries.next().is_none());

    // It should be unpacked entry, not tar archive.
    assert!(entry.path().is_dir());
    assert_eq!(entry.file_name(), segment_id);

    let restored_segment = load_segment(&entry.path(), &AtomicBool::new(false))
        .unwrap()
        .unwrap();

    // validate restored snapshot is the same as original segment
    assert_eq!(
        segment.total_point_count(),
        restored_segment.total_point_count(),
    );
    assert_eq!(
        segment.available_point_count(),
        restored_segment.available_point_count(),
    );
    assert_eq!(
        segment.deleted_point_count(),
        restored_segment.deleted_point_count(),
    );

    let hw_counter = HardwareCounterCell::new();

    for id in segment.iter_points() {
        let vectors = segment.all_vectors(id, &hw_counter).unwrap();
        let restored_vectors = restored_segment.all_vectors(id, &hw_counter).unwrap();
        assert_eq!(vectors, restored_vectors);

        let payload = segment.payload(id, &hw_counter).unwrap();
        let restored_payload = restored_segment.payload(id, &hw_counter).unwrap();
        assert_eq!(payload, restored_payload);
    }
}
