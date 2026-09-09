use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use fs_err as fs;
use tempfile::Builder;
use uuid::Uuid;

use super::*;
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vector_name_config::DenseVectorConfig;
use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use crate::entry::ReadSegmentEntry as _;
use crate::entry::entry_point::SegmentEntry as _;
use crate::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::types::{Distance, PayloadFieldSchema, PayloadSchemaType};

fn keyword_schema() -> PayloadFieldSchema {
    PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)
}

fn integer_schema() -> PayloadFieldSchema {
    PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)
}

fn dense_config(size: usize, distance: Distance) -> VectorNameConfig {
    VectorNameConfig::dense(DenseVectorConfig {
        size,
        distance,
        multivector_config: None,
        datatype: None,
    })
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
    let id = Uuid::nil();
    for level in 0..3 {
        assert_eq!(
            pending_changes_log_path(segment_path, level, id),
            segment_path.join(format!("proxy_changes.{level}.{id}.dat")),
        );
    }
}

#[test]
fn test_log_path_unique_id_per_call() {
    let segment_path = Path::new("/some/segment");
    assert_ne!(
        pending_changes_log_path(segment_path, 0, Uuid::new_v4()),
        pending_changes_log_path(segment_path, 0, Uuid::new_v4()),
        "two different ids at the same level must not collide",
    );
}

#[test]
fn test_list_log_files_ordered_and_gap_tolerant() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    assert!(list_pending_changes_log_files(dir.path()).is_empty());

    // A proxy layer that never persisted anything leaves no file; levels may have gaps
    let level_2 = pending_changes_log_path(dir.path(), 2, Uuid::new_v4());
    let level_0 = pending_changes_log_path(dir.path(), 0, Uuid::new_v4());
    fs::write(&level_2, b"").unwrap();
    fs::write(&level_0, b"").unwrap();
    // Unrelated files are not picked up
    fs::write(dir.path().join("proxy_changes.bak"), b"").unwrap();
    fs::write(dir.path().join("segment.json"), b"").unwrap();

    let files = list_pending_changes_log_files(dir.path());
    assert_eq!(files, vec![level_0, level_2]);
}

#[test]
fn test_list_log_files_multiple_generations_same_level() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // An unwrapped proxy's log file not cleaned up yet, and a new proxy at the same level, both
    // exist side by side under their own unique names
    let older = pending_changes_log_path(dir.path(), 0, Uuid::new_v4());
    let newer = pending_changes_log_path(dir.path(), 0, Uuid::new_v4());
    fs::write(&older, b"").unwrap();
    fs::write(&newer, b"").unwrap();

    let mut files = list_pending_changes_log_files(dir.path());
    files.sort();
    let mut expected = vec![older, newer];
    expected.sort();
    assert_eq!(files, expected);
}

#[test]
fn test_register_flush_load_roundtrip() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let segment_config = segment.config().clone();

    let mut pending_changes = PendingChanges::new(&segment_dir, 0).unwrap();
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
    let loaded = PendingChanges::load(pending_changes.log_path()).unwrap();
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

    // A further plain open never adopts the file: it always starts a new, uniquely named one
    let fresh = PendingChanges::new(&segment_dir, 0).unwrap();
    assert_ne!(fresh.log_path(), pending_changes.log_path());
    assert_eq!(fresh.persisted_version(), 0);
    assert!(fresh.deleted_points().is_empty());
    assert!(fresh.index_changes().is_empty());
    assert!(fresh.vector_name_changes().is_empty());
}

#[test]
fn test_flusher_covers_version_without_changes() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let pending_changes = PendingChanges::new(dir.path(), 0).unwrap();

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

    let mut pending_changes = PendingChanges::new(dir.path(), 0).unwrap();
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

    let loaded = PendingChanges::load(pending_changes.log_path()).unwrap();
    assert_eq!(loaded.deleted_points().len(), 2);
}

#[test]
fn test_flusher_skipped_after_drop() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::new(dir.path(), 0).unwrap();
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

    let mut pending_changes = PendingChanges::new(dir.path(), 0).unwrap();
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

    let loaded = PendingChanges::load(&log_path).unwrap();
    assert_eq!(loaded.deleted_points().len(), 1);
    assert_eq!(loaded.persisted_version(), 10);
    assert_eq!(fs::metadata(&log_path).unwrap().len(), intact_len);

    // A full length prefix whose payload did not make it to disk
    let mut mangled = fs::read(&log_path).unwrap();
    mangled.extend_from_slice(&100u32.to_le_bytes());
    mangled.extend_from_slice(b"partial");
    fs::write(&log_path, &mangled).unwrap();

    let loaded = PendingChanges::load(&log_path).unwrap();
    assert_eq!(loaded.deleted_points().len(), 1);
    assert_eq!(fs::metadata(&log_path).unwrap().len(), intact_len);

    // Appending after truncation must work and keep the intact entry. This resumes the exact
    // same file rather than opening a new one, unlike a brand new proxy would.
    let mut resumed = PendingChanges::load(&log_path).unwrap();
    resumed.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 11,
        },
    );
    resumed.flusher(11).unwrap()().unwrap();

    let loaded = PendingChanges::load(&log_path).unwrap();
    assert_eq!(loaded.deleted_points().len(), 2);
    assert_eq!(loaded.persisted_version(), 11);
}

#[test]
fn test_corruption_in_middle_is_error() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::new(dir.path(), 0).unwrap();
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

    assert!(PendingChanges::load(&log_path).is_err());
}

#[test]
fn test_new_proxy_never_adopts_old_log() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut first = PendingChanges::new(dir.path(), 0).unwrap();
    first.register_delete_point(
        1.into(),
        ProxyDeletedPoint {
            local_version: 1,
            operation_version: 10,
        },
    );
    first.flusher(10).unwrap()().unwrap();
    let first_log_path = first.log_path().to_path_buf();
    drop(first);

    // A new proxy on the same segment (e.g. after the first one unwrapped) never adopts the old
    // log file: it starts a new, uniquely named one and leaves the old file untouched
    let mut second = PendingChanges::new(dir.path(), 0).unwrap();
    assert_ne!(second.log_path(), first_log_path);
    assert_eq!(second.persisted_version(), 0);
    assert!(second.deleted_points().is_empty());

    second.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 20,
        },
    );
    second.flusher(20).unwrap()().unwrap();
    assert_eq!(second.persisted_version(), 20);

    // Both log files coexist, each still holding just its own proxy's entries
    let loaded_first = PendingChanges::load(&first_log_path).unwrap();
    assert_eq!(loaded_first.deleted_points().len(), 1);
    assert_eq!(loaded_first.persisted_version(), 10);

    let loaded_second = PendingChanges::load(second.log_path()).unwrap();
    assert_eq!(loaded_second.deleted_points().len(), 1);
    assert_eq!(loaded_second.persisted_version(), 20);

    assert_eq!(list_pending_changes_log_files(dir.path()).len(), 2);
}

#[test]
fn test_load_resumes_same_log_name() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::new(dir.path(), 0).unwrap();
    pending_changes.register_delete_point(
        1.into(),
        ProxyDeletedPoint {
            local_version: 1,
            operation_version: 10,
        },
    );
    pending_changes.flusher(10).unwrap()().unwrap();
    let log_path = pending_changes.log_path().to_path_buf();
    drop(pending_changes);

    // Loading the same proxy's exact file and storing again must keep using that same file name
    let mut resumed = PendingChanges::load(&log_path).unwrap();
    assert_eq!(resumed.log_path(), log_path);
    resumed.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 20,
        },
    );
    resumed.flusher(20).unwrap()().unwrap();

    assert_eq!(resumed.log_path(), log_path);
    assert_eq!(list_pending_changes_log_files(dir.path()), vec![log_path]);

    let loaded = PendingChanges::load(resumed.log_path()).unwrap();
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

    let mut pending_changes = PendingChanges::new(&segment_dir, 0).unwrap();
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

    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();
    assert_eq!(recovered.replayed, 2);
    assert_eq!(recovered.ready_at, segment_version + 2);

    assert!(!segment.has_point(2.into(), common::types::DeferredBehavior::VisibleOnly));
    assert!(segment.get_indexed_fields().contains_key(&field("color")));
    assert_eq!(segment.version(), segment_version + 2);
    // Recovery does not flush; the log files must survive until the segment durably persists
    // past `ready_at`
    assert_eq!(segment.persistent_version(), segment_version);
    assert_eq!(
        list_pending_changes_log_files(&segment_dir),
        recovered.log_files,
    );

    // Once the segment durably persists past `ready_at`, the log files are safe to remove
    segment.flush(true).unwrap();
    assert_eq!(segment.persistent_version(), segment_version + 2);
    for path in &recovered.log_files {
        fs::remove_file(path).unwrap();
    }
    assert!(list_pending_changes_log_files(&segment_dir).is_empty());

    // Running again is a no-op
    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();
    assert_eq!(recovered.replayed, 0);
    assert!(recovered.log_files.is_empty());

    // Deleting a point again with the same version is silently skipped
    assert!(
        !segment
            .delete_point(segment_version + 1, 2.into(), &hw_counter)
            .unwrap()
    );
}

#[test]
fn test_recover_ignore_leaves_log_untouched() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let segment_version = segment.version();

    let mut pending_changes = PendingChanges::new(&segment_dir, 0).unwrap();
    pending_changes.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: segment_version + 1,
        },
    );
    pending_changes.flusher(segment_version + 1).unwrap()().unwrap();
    let log_path = pending_changes.log_path().to_path_buf();
    let log_len = fs::metadata(&log_path).unwrap().len();
    drop(pending_changes);

    // Ignoring must neither touch the segment nor the log file
    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Ignore).unwrap();
    assert_eq!(recovered.replayed, 0);
    assert!(recovered.log_files.is_empty());
    assert!(segment.has_point(2.into(), common::types::DeferredBehavior::VisibleOnly));
    assert_eq!(segment.version(), segment_version);
    assert_eq!(fs::metadata(&log_path).unwrap().len(), log_len);

    // A later replaying load still recovers the change
    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();
    assert_eq!(recovered.replayed, 1);
    assert!(!segment.has_point(2.into(), common::types::DeferredBehavior::VisibleOnly));
    assert_eq!(recovered.log_files, vec![log_path.clone()]);
    // The log file removal is deferred to the caller, not done here
    assert!(log_path.is_file());

    segment.flush(true).unwrap();
    fs::remove_file(&log_path).unwrap();
    assert!(!log_path.is_file());
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
    let mut pending_changes = PendingChanges::new(&segment_dir, 0).unwrap();
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

    // Replaying the stale log must not change anything; the file is left for the caller to
    // remove once the segment is durable past `ready_at`
    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();
    assert_eq!(segment.available_point_count(), point_count);
    assert_eq!(segment.version(), op_version);
    assert_eq!(
        list_pending_changes_log_files(&segment_dir),
        recovered.log_files,
    );

    segment.flush(true).unwrap();
    for path in &recovered.log_files {
        fs::remove_file(path).unwrap();
    }
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
    let mut inner = PendingChanges::new(&segment_dir, 0).unwrap();
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

    let mut outer = PendingChanges::new(&segment_dir, 1).unwrap();
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

    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();
    assert_eq!(recovered.replayed, 4);

    assert!(!segment.has_point(1.into(), common::types::DeferredBehavior::VisibleOnly));
    assert!(!segment.has_point(2.into(), common::types::DeferredBehavior::VisibleOnly));
    // The outer (newer) index create must win over the inner delete
    assert!(segment.get_indexed_fields().contains_key(&field("color")));
    assert_eq!(segment.version(), segment_version + 4);
    assert_eq!(recovered.ready_at, segment_version + 4);

    segment.flush(true).unwrap();
    for path in &recovered.log_files {
        fs::remove_file(path).unwrap();
    }
    assert!(list_pending_changes_log_files(&segment_dir).is_empty());
}

/// Two proxy generations at the *same* level (e.g. a proxy that unwrapped without its log being
/// cleaned up yet, followed by a new proxy that never adopts it) are not chronologically ordered
/// by file discovery. Recovery must still replay them in the correct order.
#[test]
fn test_recover_same_level_multiple_generations_replayed_in_version_order() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let segment_version = segment.version();

    // First proxy generation at level 0 creates the "color" index, then unwraps without the
    // segment ever flushing, leaving its log file behind
    let mut first = PendingChanges::new(&segment_dir, 0).unwrap();
    first.register_index_change(
        field("color"),
        ProxyIndexChange::Create(keyword_schema(), segment_version + 1),
    );
    first.flusher(segment_version + 1).unwrap()().unwrap();
    drop(first);

    // A second proxy generation at the very same level starts fresh (it does not adopt the first
    // one's file) and later creates a different index
    let mut second = PendingChanges::new(&segment_dir, 0).unwrap();
    second.register_index_change(
        field("size"),
        ProxyIndexChange::Create(keyword_schema(), segment_version + 2),
    );
    second.flusher(segment_version + 2).unwrap()().unwrap();
    drop(second);

    assert_eq!(list_pending_changes_log_files(&segment_dir).len(), 2);

    // Both changes must be applied. Replaying the newer (second generation) change before the
    // older (first generation) one would bump the segment's global version past it, silently and
    // permanently skipping it, even though it targets a different field entirely.
    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();
    assert_eq!(recovered.replayed, 2);
    assert!(segment.get_indexed_fields().contains_key(&field("color")));
    assert!(segment.get_indexed_fields().contains_key(&field("size")));
    assert_eq!(segment.version(), segment_version + 2);

    segment.flush(true).unwrap();
    for path in &recovered.log_files {
        fs::remove_file(path).unwrap();
    }
    assert!(list_pending_changes_log_files(&segment_dir).is_empty());
}

#[test]
fn test_recover_vector_name_changes() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_segment(dir.path());
    let segment_dir = segment.data_path();
    let segment_version = segment.version();
    let segment_config = segment.config().clone();

    let mut pending_changes = PendingChanges::new(&segment_dir, 0).unwrap();
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

    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();

    assert!(
        segment
            .vector_names()
            .iter()
            .any(|name| name == "sparse_new"),
        "replayed vector name create must be applied: {:?}",
        segment.vector_names(),
    );

    segment.flush(true).unwrap();
    for path in &recovered.log_files {
        fs::remove_file(path).unwrap();
    }
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

#[test]
fn test_recover_delete_if_incompatible_index_change() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let mut segment = build_segment(dir.path());
    let segment_dir = segment.data_path();

    segment
        .create_field_index(6, &field("color"), Some(&keyword_schema()), &hw_counter)
        .unwrap();
    assert!(segment.get_indexed_fields().contains_key(&field("color")));

    let mut pending_changes = PendingChanges::open(&segment_dir, 0).unwrap();
    pending_changes.register_index_change(
        field("color"),
        ProxyIndexChange::DeleteIfIncompatible(7, keyword_schema()),
    );
    pending_changes.flusher(7).unwrap()().unwrap();
    drop(pending_changes);

    recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();
    assert!(
        segment.get_indexed_fields().contains_key(&field("color")),
        "a compatible schema must keep the index",
    );

    let mut pending_changes = PendingChanges::open(&segment_dir, 0).unwrap();
    pending_changes.register_index_change(
        field("color"),
        ProxyIndexChange::DeleteIfIncompatible(8, integer_schema()),
    );
    pending_changes.flusher(8).unwrap()().unwrap();
    drop(pending_changes);

    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();
    assert!(
        !segment.get_indexed_fields().contains_key(&field("color")),
        "an incompatible schema must drop the index",
    );

    // This normally happens after a full segments flush cycle
    for path in &recovered.log_files {
        fs::remove_file(path).unwrap();
    }

    assert!(list_pending_changes_log_files(&segment_dir).is_empty());
}

/// Replaying a create that supersedes the segment's own schema must clear the stale vector data
/// first, mirroring what `propagate_to_wrapped` does. A plain create is idempotent and would
/// silently keep it.
#[test]
fn test_recover_superseding_vector_name_change() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let mut segment = build_segment(dir.path());

    segment
        .create_vector_name(6, "v2", &dense_config(4, Distance::Dot))
        .unwrap();
    let mut vectors = NamedVectors::default();
    vectors.insert(DEFAULT_VECTOR_NAME.to_owned(), vec![1.0f32; 4].into());
    vectors.insert("v2".to_owned(), vec![2.0f32; 4].into());
    segment
        .upsert_point(7, 1.into(), vectors, &hw_counter)
        .unwrap();
    segment.flush(true).unwrap();
    assert!(
        segment
            .vector("v2", 1.into(), &hw_counter)
            .unwrap()
            .is_some()
    );

    let segment_dir = segment.data_path();
    let segment_config = segment.config().clone();
    let segment_version = segment.version();

    let mut pending_changes = PendingChanges::open(&segment_dir, 0).unwrap();
    pending_changes.register_vector_name_create(
        "v2".into(),
        dense_config(8, Distance::Cosine),
        segment_version + 1,
        &segment_config,
    );
    pending_changes.flusher(segment_version + 1).unwrap()().unwrap();
    drop(pending_changes);

    let recovered = recover_pending_changes(&mut segment, PersistedProxyChanges::Replay).unwrap();

    let vector_config = segment.config().vector_data.get("v2").unwrap();
    assert_eq!(vector_config.size, 8);
    assert_eq!(vector_config.distance, Distance::Cosine);
    assert!(
        segment
            .vector("v2", 1.into(), &hw_counter)
            .unwrap()
            .is_none(),
        "superseded vector data must be cleared on replay",
    );

    // This normally happens after a full segments flush cycle
    for path in &recovered.log_files {
        fs::remove_file(path).unwrap();
    }

    assert!(list_pending_changes_log_files(&segment_dir).is_empty());
}

/// An append that failed half way leaves bytes past the last durable entry. The next flush must
/// truncate them rather than append behind them, which would corrupt the log.
#[test]
fn test_partial_append_is_truncated_on_next_flush() {
    use std::io::Write as _;

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

    let mut file = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
    file.write_all(&[0xFF; 7]).unwrap();
    drop(file);
    assert_eq!(fs::metadata(&log_path).unwrap().len(), intact_len + 7);

    pending_changes.register_delete_point(
        2.into(),
        ProxyDeletedPoint {
            local_version: 2,
            operation_version: 11,
        },
    );
    pending_changes.flusher(11).unwrap()().unwrap();

    let loaded = PendingChanges::load(dir.path(), 0).unwrap();
    assert_eq!(loaded.deleted_points().len(), 2);
    assert_eq!(loaded.persisted_version(), 11);
}

/// Every prefix of the log must load as the entries that fully fit in it, and the torn tail past
/// them must be truncated away.
#[test]
fn test_torn_tail_truncation_sweep() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut pending_changes = PendingChanges::open(dir.path(), 0).unwrap();
    for point_id in 1..=4u64 {
        pending_changes.register_delete_point(
            point_id.into(),
            ProxyDeletedPoint {
                local_version: point_id,
                operation_version: 10 + point_id,
            },
        );
    }
    pending_changes.flusher(14).unwrap()().unwrap();
    let log_path = pending_changes.log_path().to_path_buf();
    let pristine = fs::read(&log_path).unwrap();
    drop(pending_changes);

    let mut boundaries = Vec::new();
    let mut offset = 0;
    while offset < pristine.len() {
        let entry_len = u32::from_le_bytes(pristine[offset..offset + 4].try_into().unwrap());
        offset += size_of::<u32>() + entry_len as usize;
        boundaries.push(offset);
    }
    assert_eq!(boundaries.len(), 4);

    for prefix_len in 0..=pristine.len() {
        fs::write(&log_path, &pristine[..prefix_len]).unwrap();

        let expected_entries = boundaries.iter().filter(|end| **end <= prefix_len).count();
        let expected_len = boundaries
            .iter()
            .rfind(|end| **end <= prefix_len)
            .copied()
            .unwrap_or(0);

        let loaded = PendingChanges::load(dir.path(), 0)
            .unwrap_or_else(|err| panic!("prefix of {prefix_len} bytes must load: {err}"));
        assert_eq!(
            loaded.deleted_points().len(),
            expected_entries,
            "prefix of {prefix_len} bytes",
        );
        assert_eq!(
            fs::metadata(&log_path).unwrap().len(),
            expected_len as u64,
            "torn tail must be truncated for a prefix of {prefix_len} bytes",
        );
    }
}
