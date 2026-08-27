use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use fs_err as fs;
use tempfile::Builder;

use super::*;
use crate::data_types::vectors::only_default_vector;
use crate::entry::ReadSegmentEntry as _;
use crate::entry::entry_point::SegmentEntry as _;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{Distance, PayloadFieldSchema, PayloadSchemaType};

fn keyword_schema() -> PayloadFieldSchema {
    PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)
}

fn field(name: &str) -> PayloadKeyType {
    name.parse().unwrap()
}

fn delete_change(point_id: u64, version: SeqNumberType) -> PendingChange {
    PendingChange::DeletePoint {
        point_id: point_id.into(),
        versions: ProxyDeletedPoint {
            local_version: version,
            operation_version: version,
        },
    }
}

/// Build a segment with points 1..=5 at versions 1..=5, flushed to disk.
fn build_segment(path: &Path) -> Segment {
    let hw_counter = HardwareCounterCell::new();
    let mut segment = build_simple_segment(path, 4, Distance::Dot).unwrap();
    for point_id in 1..=5u64 {
        segment
            .upsert_point(
                point_id,
                point_id.into(),
                only_default_vector(&[1.0, 0.0, 1.0, 1.0]),
                &hw_counter,
            )
            .unwrap();
    }
    segment.flush(true).unwrap();
    segment
}

#[test]
fn test_log_path_levels() {
    let segment_path = Path::new("/some/segment");
    assert_eq!(
        pending_changes_log_path(segment_path, 0),
        segment_path.join("pending_changes.log"),
    );
    assert_eq!(
        pending_changes_log_path(segment_path, 1),
        segment_path.join("pending_changes.log.1"),
    );
    assert_eq!(
        pending_changes_log_path(segment_path, 2),
        segment_path.join("pending_changes.log.2"),
    );
}

#[test]
fn test_list_log_files_ordered_and_gap_tolerant() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    assert!(list_pending_changes_log_files(dir.path()).is_empty());

    // A proxy layer that never persisted anything leaves no file; levels may have gaps
    fs::write(pending_changes_log_path(dir.path(), 2), b"").unwrap();
    fs::write(pending_changes_log_path(dir.path(), 0), b"").unwrap();
    // Unrelated files are not picked up
    fs::write(dir.path().join("pending_changes.log.bak"), b"").unwrap();
    fs::write(dir.path().join("segment.json"), b"").unwrap();

    let files = list_pending_changes_log_files(dir.path());
    assert_eq!(
        files,
        vec![
            pending_changes_log_path(dir.path(), 0),
            pending_changes_log_path(dir.path(), 2),
        ],
    );
}

#[test]
fn test_register_flush_load_roundtrip() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let segment_config = segment.config().clone();

    let mut pending_changes = PendingChanges::open(&segment_dir, 0).unwrap();
    assert_eq!(pending_changes.persisted_version(), 0);

    // Register one operation of each type
    pending_changes.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 10,
        },
    );
    pending_changes.register_index_change(
        field("color"),
        ProxyIndexChange::Create(keyword_schema(), 11),
    );
    pending_changes.register_index_change(field("price"), ProxyIndexChange::Delete(12));
    // An existing vector name with a different schema must supersede the wrapped data
    pending_changes.register_vector_name_create(
        "".into(),
        VectorNameConfig::dense(crate::data_types::vector_name_config::DenseVectorConfig {
            size: 8,
            distance: Distance::Cosine,
            multivector_config: None,
            datatype: None,
        }),
        13,
        &segment_config,
    );
    pending_changes.register_vector_name_delete("other".into(), 14);

    // Nothing is persisted yet
    assert!(!pending_changes.log_path().is_file());

    let flusher = pending_changes.flusher(14).unwrap();
    flusher().unwrap();

    assert!(pending_changes.log_path().is_file());
    assert_eq!(pending_changes.persisted_version(), 14);

    // The pending buffer is drained, a new flusher has nothing to do
    assert!(pending_changes.flusher(14).is_none());

    // Reconstruct the in-memory state from the log file
    let loaded = PendingChanges::load(&segment_dir, 0).unwrap();
    assert_eq!(loaded.persisted_version(), 14);
    assert_eq!(loaded.deleted_points(), pending_changes.deleted_points());
    assert_eq!(loaded.index_changes().len(), 2);
    assert_eq!(
        loaded
            .index_changes()
            .iter_ordered()
            .map(|(field_name, change)| (field_name.clone(), change.clone()))
            .collect::<Vec<_>>(),
        pending_changes
            .index_changes()
            .iter_ordered()
            .map(|(field_name, change)| (field_name.clone(), change.clone()))
            .collect::<Vec<_>>(),
    );
    let intent = loaded.vector_name_changes().get("").unwrap();
    assert!(
        matches!(
            intent,
            IntendedVector::Present {
                version: 13,
                supersedes_wrapped: true,
                ..
            },
        ),
        "existing vector name with different schema must supersede wrapped data: {intent:?}",
    );
    assert_eq!(
        loaded.vector_name_changes().get("other").unwrap(),
        &IntendedVector::Absent { version: 14 },
    );

    // Plain open adopts the file without loading the buffers
    let adopted = PendingChanges::open(&segment_dir, 0).unwrap();
    assert_eq!(adopted.persisted_version(), 14);
    assert!(adopted.deleted_points().is_empty());
    assert!(adopted.index_changes().is_empty());
    assert!(adopted.vector_name_changes().is_empty());
}

#[test]
fn test_flusher_covers_version_without_changes() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let pending_changes = PendingChanges::open(dir.path(), 0).unwrap();

    // Nothing registered and version 0 already covered
    assert!(pending_changes.flusher(0).is_none());

    // An operation that buffered nothing (e.g. a delete for an absent point) must still be
    // covered by the persisted version once flushed, without creating a log file
    let flusher = pending_changes.flusher(7).unwrap();
    flusher().unwrap();
    assert_eq!(pending_changes.persisted_version(), 7);
    assert!(!pending_changes.log_path().is_file());

    assert!(pending_changes.flusher(7).is_none());
}

#[test]
fn test_register_during_flush_is_not_lost_nor_covered() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::open(dir.path(), 0).unwrap();
    pending_changes.register_delete_point(
        1.into(),
        ProxyDeletedPoint {
            local_version: 1,
            operation_version: 10,
        },
    );

    // Capture a flusher, then register another operation before it runs
    let flusher = pending_changes.flusher(10).unwrap();
    pending_changes.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 11,
        },
    );
    flusher().unwrap();

    // The captured operation is persisted and covered, the raced-in one is neither
    assert_eq!(pending_changes.persisted_version(), 10);

    let flusher = pending_changes.flusher(11).unwrap();
    flusher().unwrap();
    assert_eq!(pending_changes.persisted_version(), 11);

    let loaded = PendingChanges::load(dir.path(), 0).unwrap();
    assert_eq!(loaded.deleted_points().len(), 2);
}

#[test]
fn test_flusher_skipped_after_drop() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::open(dir.path(), 0).unwrap();
    pending_changes.register_delete_point(
        1.into(),
        ProxyDeletedPoint {
            local_version: 1,
            operation_version: 10,
        },
    );

    let log_path = pending_changes.log_path().to_path_buf();
    let flusher = pending_changes.flusher(10).unwrap();
    drop(pending_changes);

    // A flusher captured before the component was dropped must be a no-op
    flusher().unwrap();
    assert!(!log_path.is_file());
}

#[test]
fn test_torn_tail_is_truncated() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::open(dir.path(), 0).unwrap();
    pending_changes.register_delete_point(
        1.into(),
        ProxyDeletedPoint {
            local_version: 1,
            operation_version: 10,
        },
    );
    pending_changes.flusher(10).unwrap()().unwrap();

    let log_path = pending_changes.log_path().to_path_buf();
    let intact_len = fs::metadata(&log_path).unwrap().len();
    drop(pending_changes);

    // A partially written length prefix
    let mut mangled = fs::read(&log_path).unwrap();
    mangled.extend_from_slice(&[0xAB, 0xCD]);
    fs::write(&log_path, &mangled).unwrap();

    let loaded = PendingChanges::load(dir.path(), 0).unwrap();
    assert_eq!(loaded.deleted_points().len(), 1);
    assert_eq!(loaded.persisted_version(), 10);
    assert_eq!(fs::metadata(&log_path).unwrap().len(), intact_len);

    // A full length prefix whose payload did not make it to disk
    let mut mangled = fs::read(&log_path).unwrap();
    mangled.extend_from_slice(&100u32.to_le_bytes());
    mangled.extend_from_slice(b"partial");
    fs::write(&log_path, &mangled).unwrap();

    let loaded = PendingChanges::load(dir.path(), 0).unwrap();
    assert_eq!(loaded.deleted_points().len(), 1);
    assert_eq!(fs::metadata(&log_path).unwrap().len(), intact_len);

    // Appending after truncation must work and keep the intact entry
    let mut adopted = PendingChanges::open(dir.path(), 0).unwrap();
    adopted.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 11,
        },
    );
    adopted.flusher(11).unwrap()().unwrap();

    let loaded = PendingChanges::load(dir.path(), 0).unwrap();
    assert_eq!(loaded.deleted_points().len(), 2);
    assert_eq!(loaded.persisted_version(), 11);
}

#[test]
fn test_corruption_in_middle_is_error() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::open(dir.path(), 0).unwrap();
    pending_changes.register_delete_point(
        1.into(),
        ProxyDeletedPoint {
            local_version: 1,
            operation_version: 10,
        },
    );
    pending_changes.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 11,
        },
    );
    pending_changes.flusher(11).unwrap()().unwrap();

    let log_path = pending_changes.log_path().to_path_buf();
    drop(pending_changes);

    // Corrupt the payload of the first entry; entries after it may have been acknowledged in the
    // WAL, so this must not be silently truncated away
    let mut mangled = fs::read(&log_path).unwrap();
    mangled[4] = b'X';
    fs::write(&log_path, &mangled).unwrap();

    assert!(PendingChanges::load(dir.path(), 0).is_err());
}

#[test]
fn test_adopted_log_is_appended() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::open(dir.path(), 0).unwrap();
    pending_changes.register_delete_point(
        1.into(),
        ProxyDeletedPoint {
            local_version: 1,
            operation_version: 10,
        },
    );
    pending_changes.flusher(10).unwrap()().unwrap();
    drop(pending_changes);

    // A new proxy on the same segment adopts the log file and appends to it
    let mut adopted = PendingChanges::open(dir.path(), 0).unwrap();
    assert_eq!(adopted.persisted_version(), 10);
    assert!(adopted.deleted_points().is_empty());

    adopted.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 20,
        },
    );
    adopted.flusher(20).unwrap()().unwrap();
    assert_eq!(adopted.persisted_version(), 20);

    let loaded = PendingChanges::load(dir.path(), 0).unwrap();
    assert_eq!(loaded.deleted_points().len(), 2);
    assert_eq!(loaded.persisted_version(), 20);
}

#[test]
fn test_recover_pending_changes() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let mut segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let segment_version = segment.version();

    let mut pending_changes = PendingChanges::open(&segment_dir, 0).unwrap();
    pending_changes.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: segment_version + 1,
        },
    );
    pending_changes.register_index_change(
        field("color"),
        ProxyIndexChange::Create(keyword_schema(), segment_version + 2),
    );
    pending_changes.flusher(segment_version + 2).unwrap()().unwrap();
    drop(pending_changes);

    // The segment itself never saw the operations
    assert!(segment.has_point(2.into(), common::types::DeferredBehavior::VisibleOnly));

    let replayed = recover_pending_changes(&mut segment).unwrap();
    assert_eq!(replayed, 2);

    assert!(!segment.has_point(2.into(), common::types::DeferredBehavior::VisibleOnly));
    assert!(segment.get_indexed_fields().contains_key(&field("color")));
    assert_eq!(segment.version(), segment_version + 2);
    // The segment was flushed before the log was removed
    assert_eq!(segment.persistent_version(), segment_version + 2);
    assert!(list_pending_changes_log_files(&segment_dir).is_empty());

    // Running again is a no-op
    assert_eq!(recover_pending_changes(&mut segment).unwrap(), 0);

    // Deleting a point again with the same version is silently skipped
    assert!(
        !segment
            .delete_point(segment_version + 1, 2.into(), &hw_counter)
            .unwrap()
    );
}

#[test]
fn test_recover_stale_log_is_noop() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let mut segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let op_version = segment.version() + 1;

    // The operation is already applied to the segment, e.g. because a proxy propagated its
    // changes before unwrapping and left the log file behind
    let mut pending_changes = PendingChanges::open(&segment_dir, 0).unwrap();
    pending_changes.register_delete_point(
        3.into(),
        ProxyDeletedPoint {
            local_version: 3,
            operation_version: op_version,
        },
    );
    pending_changes.flusher(op_version).unwrap()().unwrap();
    drop(pending_changes);

    segment
        .delete_point(op_version, 3.into(), &hw_counter)
        .unwrap();
    let point_count = segment.available_point_count();

    // Replaying the stale log must not change anything, and must clean up the file
    recover_pending_changes(&mut segment).unwrap();
    assert_eq!(segment.available_point_count(), point_count);
    assert_eq!(segment.version(), op_version);
    assert!(list_pending_changes_log_files(&segment_dir).is_empty());
}

#[test]
fn test_recover_multiple_levels_in_order() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let segment_version = segment.version();

    // Inner most proxy layer deleted point 1, the layer above later deleted point 2 and
    // re-created the index the inner layer deleted
    let mut inner = PendingChanges::open(&segment_dir, 0).unwrap();
    inner.register_delete_point(
        1.into(),
        ProxyDeletedPoint {
            local_version: 1,
            operation_version: segment_version + 1,
        },
    );
    inner.register_index_change(
        field("color"),
        ProxyIndexChange::Delete(segment_version + 2),
    );
    inner.flusher(segment_version + 2).unwrap()().unwrap();
    drop(inner);

    let mut outer = PendingChanges::open(&segment_dir, 1).unwrap();
    outer.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: segment_version + 3,
        },
    );
    outer.register_index_change(
        field("color"),
        ProxyIndexChange::Create(keyword_schema(), segment_version + 4),
    );
    outer.flusher(segment_version + 4).unwrap()().unwrap();
    drop(outer);

    let replayed = recover_pending_changes(&mut segment).unwrap();
    assert_eq!(replayed, 4);

    assert!(!segment.has_point(1.into(), common::types::DeferredBehavior::VisibleOnly));
    assert!(!segment.has_point(2.into(), common::types::DeferredBehavior::VisibleOnly));
    // The outer (newer) index create must win over the inner delete
    assert!(segment.get_indexed_fields().contains_key(&field("color")));
    assert_eq!(segment.version(), segment_version + 4);
    assert!(list_pending_changes_log_files(&segment_dir).is_empty());
}

#[test]
fn test_recover_vector_name_changes() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let segment_version = segment.version();
    let segment_config = segment.config().clone();

    let mut pending_changes = PendingChanges::open(&segment_dir, 0).unwrap();
    // Create a brand new sparse vector name
    pending_changes.register_vector_name_create(
        "sparse_new".into(),
        VectorNameConfig::sparse(crate::data_types::vector_name_config::SparseVectorConfig {
            modifier: None,
            datatype: None,
        }),
        segment_version + 1,
        &segment_config,
    );
    pending_changes.flusher(segment_version + 1).unwrap()().unwrap();
    drop(pending_changes);

    recover_pending_changes(&mut segment).unwrap();

    assert!(
        segment
            .vector_names()
            .iter()
            .any(|name| name == "sparse_new"),
        "replayed vector name create must be applied: {:?}",
        segment.vector_names(),
    );
    assert!(list_pending_changes_log_files(&segment_dir).is_empty());
}

#[test]
fn test_pending_change_version() {
    assert_eq!(delete_change(1, 42).version(), 42);
    assert_eq!(
        PendingChange::IndexChange {
            field_name: field("color"),
            change: ProxyIndexChange::Create(keyword_schema(), 43),
        }
        .version(),
        43,
    );
    assert_eq!(
        PendingChange::VectorNameChange {
            vector_name: "v".into(),
            intent: IntendedVector::Absent { version: 44 },
        }
        .version(),
        44,
    );
}
