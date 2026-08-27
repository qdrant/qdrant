use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::tar_ext;
use common::types::DeferredBehavior;
use fs_err::File;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, SegmentEntry as _, SnapshotEntry as _,
    StorageSegmentEntry as _,
};
use segment::types::{FieldCondition, PayloadSchemaType};
use tempfile::Builder;

use super::*;
use crate::fixtures::*;

impl ProxySegment {
    /// This function is a simplified version of `search_batch` intended for testing purposes.
    #[allow(clippy::too_many_arguments)]
    pub fn search(
        &self,
        vector_name: &VectorName,
        vector: &QueryVector,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        use segment::data_types::query_context::QueryContext;

        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();
        let result = self.search_batch(
            vector_name,
            &[vector],
            with_payload,
            with_vector,
            filter,
            top,
            params,
            &segment_query_context,
        )?;

        Ok(result.into_iter().next().unwrap())
    }
}

/// Regression test for the proxy `deleted_mask` race that drops a live point from scored
/// search (catalog: "exact dense search + payload filter drops a candidate", optimizer-on).
///
/// `deleted_mask` is a snapshot of the wrapped segment's deleted bitvec. If it is synced while
/// the wrapped segment is still appendable, an upsert can still land afterwards at an internal
/// offset past the snapshot; the scored search consults `deleted_mask` and treats every
/// out-of-range offset as deleted (`NotDeletedChecker` → `unwrap_or(true)`), silently
/// excluding the live point even though scroll/retrieve still see it.
///
/// [`UnsyncedProxySegment::finalize`] is what reads the mask, so the fix is timing: finalize
/// only once the wrapped segment is frozen, so the mask covers its full final point range. This
/// test exercises both orderings on two parallel segments — finalize-before-race (buggy) vs
/// finalize-after-race (fixed) — entirely at the proxy level, no model-testing harness involved.
#[test]
fn test_proxy_deleted_mask_resync_after_race_window_write() {
    let hw_counter = HardwareCounterCell::new();
    let query_vector: QueryVector = [1.0, 1.0, 1.0, 1.0].into();

    // Build a wrapped segment with 2 points (internal offsets 0 and 1) and an unsynced proxy
    // around it. Returns `(unsynced_proxy, wrapped_handle)` so the caller controls when the proxy
    // is finalized (mask synced) relative to the race-window write.
    let build_unsynced_proxy = |dir: &std::path::Path| -> (UnsyncedProxySegment, LockedSegment) {
        let original_segment = LockedSegment::new(empty_segment(dir));
        original_segment
            .get()
            .write()
            .upsert_point(
                1,
                1.into(),
                only_default_vector(&[1.0, 0.0, 0.0, 0.0]),
                &hw_counter,
            )
            .unwrap();
        original_segment
            .get()
            .write()
            .upsert_point(
                2,
                2.into(),
                only_default_vector(&[0.0, 1.0, 0.0, 0.0]),
                &hw_counter,
            )
            .unwrap();

        // Keep a handle so we can write to the wrapped segment around the proxy lifecycle.
        let wrapped_handle = original_segment.clone();
        let proxy = UnsyncedProxySegment::new(original_segment).unwrap();
        (proxy, wrapped_handle)
    };

    // Race-window write: a brand-new point lands at offset 2, past a length-2 `deleted_mask`.
    let race_window_write = |wrapped: &LockedSegment| {
        wrapped
            .get()
            .write()
            .upsert_point(
                11,
                3.into(),
                only_default_vector(&[1.0, 1.0, 1.0, 1.0]),
                &hw_counter,
            )
            .unwrap();
    };

    let search_ids = |proxy: &ProxySegment| -> Vec<PointIdType> {
        proxy
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap()
            .into_iter()
            .map(|scored| scored.id)
            .collect()
    };

    // --- Buggy ordering: finalize BEFORE the race write, so the mask snapshot stops at len 2 ---
    let buggy_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (buggy_unsynced, buggy_wrapped) = build_unsynced_proxy(buggy_dir.path());
    let mut buggy_proxy = buggy_unsynced.finalize();
    race_window_write(&buggy_wrapped);
    // A proxy-level delete makes `deleted_points` non-empty, which is what makes the search
    // path consult `deleted_mask` instead of the wrapped segment's live deleted state.
    buggy_proxy.delete_point(10, 1.into(), &hw_counter).unwrap();
    let buggy_ids = search_ids(&buggy_proxy);
    assert!(
        !buggy_ids.contains(&3.into()),
        "finalizing before the race the point should be (buggily) dropped, got {buggy_ids:?}",
    );

    // --- Fixed ordering: finalize AFTER the race write (segment frozen), so the mask covers it ---
    let fixed_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (fixed_unsynced, fixed_wrapped) = build_unsynced_proxy(fixed_dir.path());
    race_window_write(&fixed_wrapped);
    let mut fixed_proxy = fixed_unsynced.finalize();
    fixed_proxy.delete_point(10, 1.into(), &hw_counter).unwrap();
    let fixed_ids = search_ids(&fixed_proxy);
    assert!(
        fixed_ids.contains(&3.into()),
        "finalizing after the race the point must be searchable, got {fixed_ids:?}",
    );
    assert!(
        !fixed_ids.contains(&1.into()),
        "the proxy-deleted point must still be excluded after finalize, got {fixed_ids:?}",
    );
}

#[test]
fn test_search_batch_equivalence_single() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));
    let hw_counter = HardwareCounterCell::new();

    let vec4 = vec![1.1, 1.0, 0.0, 1.0];
    original_segment
        .get()
        .write()
        .upsert_point(100, 4.into(), only_default_vector(&vec4), &hw_counter)
        .unwrap();
    let vec6 = vec![1.0, 1.0, 0.5, 1.0];
    original_segment
        .get()
        .write()
        .upsert_point(101, 6.into(), only_default_vector(&vec6), &hw_counter)
        .unwrap();

    let mut proxy_segment = ProxySegment::new(original_segment);

    proxy_segment
        .delete_point(102, 1.into(), &hw_counter)
        .unwrap();

    let query_vector = [1.0, 1.0, 1.0, 1.0].into();
    let search_result = proxy_segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query_vector,
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
        )
        .unwrap();

    eprintln!("search_result = {search_result:#?}");

    let hardware_accumulator = HwMeasurementAcc::new();
    let query_context = QueryContext::new(10000, hardware_accumulator.clone());
    let segment_query_context = query_context.get_segment_query_context();

    let search_batch_result = proxy_segment
        .search_batch(
            DEFAULT_VECTOR_NAME,
            &[&query_vector],
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
            &segment_query_context,
        )
        .unwrap();

    eprintln!("search_batch_result = {search_batch_result:#?}");

    assert!(!search_result.is_empty());
    assert_eq!(search_result, search_batch_result[0].clone());
    assert!(hardware_accumulator.get_cpu() > 0);
}

#[test]
fn test_search_batch_equivalence_single_random() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(random_segment(dir.path(), 100, 200, 4));

    let proxy_segment = ProxySegment::new(original_segment);

    let query_vector = [1.0, 1.0, 1.0, 1.0].into();
    let search_result = proxy_segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query_vector,
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
        )
        .unwrap();

    eprintln!("search_result = {search_result:#?}");

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    let search_batch_result = proxy_segment
        .search_batch(
            DEFAULT_VECTOR_NAME,
            &[&query_vector],
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
            &segment_query_context,
        )
        .unwrap();

    eprintln!("search_batch_result = {search_batch_result:#?}");

    assert!(!search_result.is_empty());
    assert_eq!(search_result, search_batch_result[0].clone())
}

#[test]
fn test_search_batch_equivalence_multi_random() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(random_segment(dir.path(), 100, 200, 4));

    let proxy_segment = ProxySegment::new(original_segment);

    let q1 = [1.0, 1.0, 1.0, 0.1];
    let q2 = [1.0, 1.0, 0.1, 0.1];
    let q3 = [1.0, 0.1, 1.0, 0.1];
    let q4 = [0.1, 1.0, 1.0, 0.1];

    let query_vectors: &[&QueryVector] = &[&q1.into(), &q2.into(), &q3.into(), &q4.into()];

    let mut all_single_results = Vec::with_capacity(query_vectors.len());
    for query_vector in query_vectors {
        let res = proxy_segment
            .search(
                DEFAULT_VECTOR_NAME,
                query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();
        all_single_results.push(res);
    }

    eprintln!("search_result = {all_single_results:#?}");

    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();

    let search_batch_result = proxy_segment
        .search_batch(
            DEFAULT_VECTOR_NAME,
            query_vectors,
            &WithPayload::default(),
            &false.into(),
            None,
            10,
            None,
            &segment_query_context,
        )
        .unwrap();

    eprintln!("search_batch_result = {search_batch_result:#?}");

    assert_eq!(all_single_results, search_batch_result)
}

fn wrap_proxy(original_segment: LockedSegment) -> ProxySegment {
    ProxySegment::new(original_segment)
}

#[test]
fn test_read_filter() {
    let is_stopped = AtomicBool::new(false);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));

    let hw_counter = HardwareCounterCell::new();

    let filter = Filter::new_must_not(Condition::Field(FieldCondition::new_match(
        "color".parse().unwrap(),
        "blue".to_string().into(),
    )));

    let original_points = original_segment
        .get()
        .read()
        .read_filtered(
            None,
            Some(100),
            None,
            &is_stopped,
            &hw_counter,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();

    let original_points_filtered = original_segment
        .get()
        .read()
        .read_filtered(
            None,
            Some(100),
            Some(&filter),
            &is_stopped,
            &hw_counter,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();

    let mut proxy_segment = wrap_proxy(original_segment);

    let hw_counter = HardwareCounterCell::new();

    proxy_segment
        .delete_point(100, 2.into(), &hw_counter)
        .unwrap();

    let proxy_res = proxy_segment
        .read_filtered(
            None,
            Some(100),
            None,
            &is_stopped,
            &hw_counter,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();
    let proxy_res_filtered = proxy_segment
        .read_filtered(
            None,
            Some(100),
            Some(&filter),
            &is_stopped,
            &hw_counter,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();

    assert_eq!(original_points_filtered.len() - 1, proxy_res_filtered.len());
    assert_eq!(original_points.len() - 1, proxy_res.len());
}

#[test]
fn test_read_range() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));

    let original_points = original_segment
        .get()
        .read()
        .read_range(None, Some(10.into()));

    let mut proxy_segment = wrap_proxy(original_segment);

    let hw_cell = HardwareCounterCell::new();

    proxy_segment.delete_point(100, 2.into(), &hw_cell).unwrap();

    let proxy_res = proxy_segment.read_range(None, Some(10.into()));

    assert_eq!(original_points.len() - 1, proxy_res.len());
}

#[test]
fn test_sync_indexes() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));
    let write_segment = LockedSegment::new(empty_segment(dir.path()));

    original_segment
        .get()
        .write()
        .create_field_index(
            10,
            &"color".parse().unwrap(),
            Some(&PayloadSchemaType::Keyword.into()),
            &HardwareCounterCell::new(),
        )
        .unwrap();

    let proxy_segment = ProxySegment::new(original_segment.clone());

    let hw_cell = HardwareCounterCell::new();

    proxy_segment
        .replicate_field_indexes(0, &hw_cell, &write_segment)
        .unwrap();

    assert!(
        write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"color".parse().unwrap()),
    );

    original_segment
        .get()
        .write()
        .create_field_index(
            11,
            &"location".parse().unwrap(),
            Some(&PayloadSchemaType::Geo.into()),
            &hw_cell,
        )
        .unwrap();

    original_segment
        .get()
        .write()
        .delete_field_index(12, &"color".parse().unwrap())
        .unwrap();

    proxy_segment
        .replicate_field_indexes(0, &hw_cell, &write_segment)
        .unwrap();

    assert!(
        write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"location".parse().unwrap()),
    );
    assert!(
        !write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"color".parse().unwrap()),
    );
}

#[test]
fn test_take_snapshot() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));
    let original_segment_2 = LockedSegment::new(build_segment_2(dir.path()));

    let hw_cell = HardwareCounterCell::new();

    let mut proxy_segment = ProxySegment::new(original_segment);

    let proxy_segment2 = ProxySegment::new(original_segment_2);

    proxy_segment.delete_point(102, 1.into(), &hw_cell).unwrap();

    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    eprintln!("Snapshot into {:?}", snapshot_file.path());
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(snapshot_file.path()).unwrap());
    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let temp_dir2 = Builder::new().prefix("temp_dir").tempdir().unwrap();
    proxy_segment
        .take_snapshot(temp_dir.path(), &tar, SnapshotFormat::Regular, None)
        .unwrap();
    proxy_segment2
        .take_snapshot(temp_dir2.path(), &tar, SnapshotFormat::Regular, None)
        .unwrap();
    tar.blocking_finish().unwrap();

    // validate that 2 archives were created:
    // wrapped_segment1, wrapped_segment2
    let mut tar = tar::Archive::new(File::open(snapshot_file.path()).unwrap());
    let archive_count = tar.entries_with_seek().unwrap().count();
    assert_eq!(archive_count, 2);

    let mut tar = tar::Archive::new(File::open(snapshot_file.path()).unwrap());
    for entry in tar.entries_with_seek().unwrap() {
        let archive_path = entry.unwrap().path().unwrap().into_owned();
        let archive_extension = archive_path.extension().unwrap();
        // correct file extension
        assert_eq!(archive_extension, "tar");
    }
}

/// A persisted pending changes log is part of the segment snapshot and manifest, so full,
/// partial and streamed snapshots all carry it and recovery can replay it.
#[test]
fn test_take_snapshot_includes_pending_changes_log() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));

    let hw_cell = HardwareCounterCell::new();

    let mut proxy_segment = ProxySegment::new(original_segment);
    proxy_segment.delete_point(102, 1.into(), &hw_cell).unwrap();
    // Persist the pending delete into the pending changes log
    proxy_segment.flush(false).unwrap();

    // The pending changes log is part of the segment manifest; it is registered unversioned, so
    // its effective version resolves to the wrapped segment version
    let manifest = proxy_segment.get_segment_manifest().unwrap();
    let file_version = manifest
        .file_version(std::path::Path::new(
            segment::pending_changes::PENDING_CHANGES_LOG_FILE,
        ))
        .expect("pending changes log must be listed in the segment manifest");
    assert_eq!(file_version, manifest.segment_version);

    // The pending changes log is included in the snapshot files
    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(snapshot_file.path()).unwrap());
    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    proxy_segment
        .take_snapshot(temp_dir.path(), &tar, SnapshotFormat::Streamable, None)
        .unwrap();
    tar.blocking_finish().unwrap();

    let mut tar = tar::Archive::new(File::open(snapshot_file.path()).unwrap());
    let has_pending_changes_log = tar.entries_with_seek().unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .unwrap()
            .ends_with(std::path::Path::new(&format!(
                "files/{}",
                segment::pending_changes::PENDING_CHANGES_LOG_FILE,
            )))
    });
    assert!(
        has_pending_changes_log,
        "snapshot must carry the pending changes log",
    );
}

#[test]
fn test_point_vector_count() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let original_segment = LockedSegment::new(build_segment_1(dir.path()));

    let hw_cell = HardwareCounterCell::new();

    let mut proxy_segment = ProxySegment::new(original_segment);

    // We have 5 points by default, assert counts
    let segment_info = proxy_segment.info().unwrap();
    assert_eq!(segment_info.num_points, 5);
    assert_eq!(segment_info.num_vectors, 5);

    // Delete nonexistent point, counts should remain the same
    proxy_segment
        .delete_point(101, 99999.into(), &hw_cell)
        .unwrap();
    let segment_info = proxy_segment.info().unwrap();
    assert_eq!(segment_info.num_points, 5);
    assert_eq!(segment_info.num_vectors, 5);

    // Delete point 1, counts should decrease by 1
    proxy_segment.delete_point(102, 4.into(), &hw_cell).unwrap();
    let segment_info = proxy_segment.info().unwrap();
    assert_eq!(segment_info.num_points, 4);
    assert_eq!(segment_info.num_vectors, 4);
}

#[test]
fn test_point_vector_count_multivec() {
    use segment::segment_constructor::simple_segment_constructor::{
        VECTOR1_NAME, VECTOR2_NAME, build_segment_with_two_named_vecs,
    };
    use segment::types::Distance;

    // Create proxied multivec segment
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 1;

    let mut original_segment =
        build_segment_with_two_named_vecs(dir.path(), dim, dim, Distance::Dot).unwrap();

    let hw_cell = HardwareCounterCell::new();

    original_segment
        .upsert_point(
            100,
            4.into(),
            NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0.4]),
                (VECTOR2_NAME.into(), vec![0.5]),
            ]),
            &hw_cell,
        )
        .unwrap();
    original_segment
        .upsert_point(
            101,
            6.into(),
            NamedVectors::from_pairs([
                (VECTOR1_NAME.into(), vec![0.6]),
                (VECTOR2_NAME.into(), vec![0.7]),
            ]),
            &hw_cell,
        )
        .unwrap();

    let original_segment = LockedSegment::new(original_segment);

    let mut proxy_segment = ProxySegment::new(original_segment);

    // Assert counts from original segment
    let segment_info = proxy_segment.info().unwrap();
    assert_eq!(segment_info.num_points, 2);
    assert_eq!(segment_info.num_vectors, 4);

    // Delete nonexistent point, counts should remain the same
    proxy_segment.delete_point(104, 1.into(), &hw_cell).unwrap();
    let segment_info = proxy_segment.info().unwrap();
    assert_eq!(segment_info.num_points, 2);
    assert_eq!(segment_info.num_vectors, 4);

    // Delete point 4, counts should decrease by 1
    proxy_segment.delete_point(105, 4.into(), &hw_cell).unwrap();
    let segment_info = proxy_segment.info().unwrap();
    assert_eq!(segment_info.num_points, 1);
    assert_eq!(segment_info.num_vectors, 2);
}

#[test]
fn test_proxy_segment_flush() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("segment_dir")
        .tempdir()
        .unwrap();

    let locked_wrapped_segment = LockedSegment::new(build_segment_1(tmp_dir.path()));
    let wrapped_segment_dir = locked_wrapped_segment.get().read().data_path();

    let mut proxy_segment = ProxySegment::new(locked_wrapped_segment.clone());

    let flushed_version_1 = proxy_segment.flush(false).unwrap();
    assert_eq!(flushed_version_1, proxy_segment.version());

    proxy_segment
        .delete_point(100, 2.into(), &HardwareCounterCell::new())
        .unwrap();

    // The pending delete is not persisted yet, so it caps the persistent version
    assert_eq!(proxy_segment.persistent_version(), flushed_version_1);

    let flushed_version_2 = proxy_segment.flush(false).unwrap();

    // Flushing persisted the pending delete into the pending changes log, so the proxy is fully
    // persisted and does not hold back acknowledging the WAL
    assert_eq!(flushed_version_2, 100);
    assert_eq!(flushed_version_2, proxy_segment.version());
    assert!(
        segment::pending_changes::pending_changes_log_path(&wrapped_segment_dir, 0).is_file(),
        "flush must persist pending changes log into the wrapped segment directory",
    );

    // An operation that buffers nothing (delete of an absent point) must not cap the persistent
    // version either
    proxy_segment
        .delete_point(101, 12345.into(), &HardwareCounterCell::new())
        .unwrap();
    let flushed_version_3 = proxy_segment.flush(false).unwrap();
    assert_eq!(flushed_version_3, 101);
    assert_eq!(flushed_version_3, proxy_segment.version());
}

/// Pending proxy changes are persisted on flush, survive dropping the proxy without propagation
/// (e.g. a crash) and are replayed onto the actual segment when it is loaded again.
#[test]
fn test_pending_changes_recovered_on_restart() {
    let hw_counter = HardwareCounterCell::new();
    let tmp_dir = tempfile::Builder::new()
        .prefix("segment_dir")
        .tempdir()
        .unwrap();

    let locked_wrapped_segment = LockedSegment::new(build_segment_1(tmp_dir.path()));
    let wrapped_segment_dir = locked_wrapped_segment.get().read().data_path();

    let mut proxy_segment = ProxySegment::new(locked_wrapped_segment.clone());

    proxy_segment
        .delete_point(100, 2.into(), &hw_counter)
        .unwrap();
    proxy_segment
        .apply_field_index(
            101,
            "color".parse().unwrap(),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
            vec![],
        )
        .unwrap();

    // Flush persists the pending changes log; the wrapped segment itself never sees the changes
    let flushed_version = proxy_segment.flush(false).unwrap();
    assert_eq!(flushed_version, 101);

    // "Crash": drop the proxy without propagating to the wrapped segment
    drop(proxy_segment);
    drop(locked_wrapped_segment);
    assert!(segment::pending_changes::pending_changes_log_path(&wrapped_segment_dir, 0).is_file());

    // "Restart": load the segment from disk and recover the pending changes, as done on start-up
    let mut segment = segment::segment_constructor::load_segment(
        &wrapped_segment_dir,
        uuid::Uuid::nil(),
        None,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();

    // Before recovery the segment does not know about the buffered operations
    assert!(segment.has_point(2.into(), DeferredBehavior::VisibleOnly));

    let replayed = segment::pending_changes::recover_pending_changes(&mut segment).unwrap();
    assert_eq!(replayed, 2);

    assert!(!segment.has_point(2.into(), DeferredBehavior::VisibleOnly));
    assert!(
        segment
            .get_indexed_fields()
            .contains_key(&"color".parse().unwrap())
    );
    assert_eq!(segment.version(), 101);
    // Recovery flushed the segment and removed the log file
    assert_eq!(segment.persistent_version(), 101);
    assert!(!segment::pending_changes::pending_changes_log_path(&wrapped_segment_dir, 0).is_file());
}

/// Unwrapping a proxy leaves the pending changes log in place (deleting it before the wrapped
/// segment flushed the propagated changes would not be crash safe), and a new proxy on the same
/// segment adopts and appends to it.
#[test]
fn test_unproxy_leaves_pending_changes_log_for_adoption() {
    let hw_counter = HardwareCounterCell::new();
    let tmp_dir = tempfile::Builder::new()
        .prefix("segment_dir")
        .tempdir()
        .unwrap();

    let locked_wrapped_segment = LockedSegment::new(build_segment_1(tmp_dir.path()));
    let wrapped_segment_dir = locked_wrapped_segment.get().read().data_path();
    let log_path = segment::pending_changes::pending_changes_log_path(&wrapped_segment_dir, 0);

    let mut proxy_segment = ProxySegment::new(locked_wrapped_segment.clone());
    proxy_segment
        .delete_point(100, 2.into(), &hw_counter)
        .unwrap();
    proxy_segment.flush(false).unwrap();

    // Unproxy: propagate pending changes into the wrapped segment, then drop the proxy
    proxy_segment.propagate_to_wrapped().unwrap();
    assert!(proxy_segment.get_deleted_points().is_empty());
    drop(proxy_segment);

    assert!(
        !locked_wrapped_segment
            .get()
            .read()
            .has_point(2.into(), DeferredBehavior::VisibleOnly)
    );
    assert!(
        log_path.is_file(),
        "unproxying must leave the pending changes log in place",
    );

    // A new proxy on the same segment adopts the log: its persisted version covers the old
    // operations, and new operations are appended after them
    let mut proxy_segment = ProxySegment::new(locked_wrapped_segment.clone());
    assert_eq!(proxy_segment.persistent_version(), 100);
    assert!(proxy_segment.get_deleted_points().is_empty());

    proxy_segment
        .delete_point(110, 3.into(), &hw_counter)
        .unwrap();
    proxy_segment.flush(false).unwrap();

    let loaded = segment::pending_changes::PendingChanges::load(&wrapped_segment_dir, 0).unwrap();
    assert_eq!(loaded.deleted_points().len(), 2);
    assert_eq!(loaded.persisted_version(), 110);
}

/// Each proxy layer persists its pending changes into a dedicated log file; on restart all files
/// are replayed onto the segment, inner most layer first.
#[test]
fn test_double_proxy_pending_changes_levels() {
    let hw_counter = HardwareCounterCell::new();
    let tmp_dir = tempfile::Builder::new()
        .prefix("segment_dir")
        .tempdir()
        .unwrap();

    let locked_wrapped_segment = LockedSegment::new(build_segment_1(tmp_dir.path()));
    let wrapped_segment_dir = locked_wrapped_segment.get().read().data_path();

    // Inner proxy (e.g. an ongoing optimization) buffers a delete of point 2
    let mut inner_proxy = ProxySegment::new(locked_wrapped_segment.clone());
    inner_proxy
        .delete_point(100, 2.into(), &hw_counter)
        .unwrap();

    // Outer proxy (e.g. an ongoing snapshot) wraps the inner proxy and buffers a delete of
    // point 3
    let locked_inner_proxy = LockedSegment::from(inner_proxy);
    let mut outer_proxy = ProxySegment::new(locked_inner_proxy.clone());
    outer_proxy
        .delete_point(101, 3.into(), &hw_counter)
        .unwrap();

    // Flushing the outer proxy persists its own pending changes and passes the flush along to
    // the inner proxy, which persists its own as well
    let flushed_version = outer_proxy.flush(false).unwrap();
    assert_eq!(flushed_version, 101);

    let log_files = segment::pending_changes::list_pending_changes_log_files(&wrapped_segment_dir);
    assert_eq!(
        log_files,
        vec![
            segment::pending_changes::pending_changes_log_path(&wrapped_segment_dir, 0),
            segment::pending_changes::pending_changes_log_path(&wrapped_segment_dir, 1),
        ],
        "each proxy layer must persist into its own log file",
    );

    // "Crash": drop both proxies without propagating, then load the segment and recover
    drop(outer_proxy);
    drop(locked_inner_proxy);
    drop(locked_wrapped_segment);

    let mut segment = segment::segment_constructor::load_segment(
        &wrapped_segment_dir,
        uuid::Uuid::nil(),
        None,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();

    let replayed = segment::pending_changes::recover_pending_changes(&mut segment).unwrap();
    assert_eq!(replayed, 2);

    assert!(!segment.has_point(2.into(), DeferredBehavior::VisibleOnly));
    assert!(!segment.has_point(3.into(), DeferredBehavior::VisibleOnly));
    assert_eq!(segment.version(), 101);
    assert!(
        segment::pending_changes::list_pending_changes_log_files(&wrapped_segment_dir).is_empty()
    );
}

#[test]
fn test_proxy_deferred() {
    let hw_counter = HardwareCounterCell::new();

    let tmp_dir = tempfile::Builder::new()
        .prefix("segment_dir")
        .tempdir()
        .unwrap();

    let mut wrapped_segment = build_segment_with_deferred_1(tmp_dir.path());

    let initial_estimation = wrapped_segment.estimate_point_count(None, &hw_counter);

    let initial_deferred_point_count = wrapped_segment.size_info().num_deferred_points.unwrap();

    wrapped_segment
        .delete_point_internal(3, None, &hw_counter)
        .unwrap();

    assert_eq!(
        wrapped_segment.size_info().num_deferred_points.unwrap(),
        initial_deferred_point_count - 1
    );

    let mut proxy_segment = ProxySegment::new(LockedSegment::new(wrapped_segment));

    assert_eq!(
        proxy_segment.size_info().num_deferred_points.unwrap(),
        initial_deferred_point_count - 1
    );

    assert_eq!(proxy_segment.available_point_count_without_deferred(), 3);

    proxy_segment
        .delete_point(7, 5.into(), &hw_counter)
        .unwrap();

    assert_eq!(
        proxy_segment.size_info().num_deferred_points.unwrap(),
        initial_deferred_point_count - 2
    );

    assert_eq!(proxy_segment.available_point_count_without_deferred(), 3);

    // We didn't touch normal points so estimation should not change.
    assert_eq!(
        proxy_segment.estimate_point_count(None, &hw_counter),
        initial_estimation
    );

    // Touch normal points
    proxy_segment
        .delete_point(6, 1.into(), &hw_counter)
        .unwrap();

    // Now we must see a difference in estimation.
    assert_ne!(
        proxy_segment.estimate_point_count(None, &hw_counter),
        initial_estimation
    );

    assert_eq!(proxy_segment.available_point_count_without_deferred(), 2);
}
