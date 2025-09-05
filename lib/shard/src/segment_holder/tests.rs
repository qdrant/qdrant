use std::collections::HashMap;
use std::fs::File;
use std::str::FromStr;

use rand::Rng;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorInternal, only_default_vector};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadContainer};
use serde_json::Value;
use tempfile::Builder;

use super::*;
use crate::fixtures::*;
use crate::proxy_segment::ProxyDeletedPoint;

#[test]
fn test_add_and_swap() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    assert_ne!(sid1, sid2);

    let segment3 = build_simple_segment(dir.path(), 4, Distance::Dot).unwrap();

    let (_sid3, replaced_segments) = holder.swap_new(segment3, &[sid1, sid2]);
    replaced_segments
        .into_iter()
        .for_each(|s| s.drop_data().unwrap());
}

#[rstest::rstest]
#[case::do_update_nonappendable(true)]
#[case::dont_update_nonappendable(false)]
fn test_apply_to_appendable(#[case] update_nonappendable: bool) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment1 = build_segment_1(dir.path());
    let mut segment2 = build_segment_2(dir.path());

    segment2.appendable_flag = false;

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    let op_num = 100;
    let mut processed_points: Vec<PointIdType> = vec![];
    let mut processed_points2: Vec<PointIdType> = vec![];
    holder
        .apply_points_with_conditional_move(
            op_num,
            &[1.into(), 2.into(), 11.into(), 12.into()],
            |point_id, segment| {
                processed_points.push(point_id);
                assert!(segment.has_point(point_id));
                Ok(true)
            },
            |point_id, _, _| processed_points2.push(point_id),
            |_| update_nonappendable,
            &HardwareCounterCell::new(),
        )
        .unwrap();

    assert_eq!(4, processed_points.len() + processed_points2.len());

    let locked_segment_1 = holder.get(sid1).unwrap().get();
    let read_segment_1 = locked_segment_1.read();
    let locked_segment_2 = holder.get(sid2).unwrap().get();
    let read_segment_2 = locked_segment_2.read();

    for i in processed_points2.iter() {
        assert!(read_segment_1.has_point(*i));
    }

    assert!(read_segment_1.has_point(1.into()));
    assert!(read_segment_1.has_point(2.into()));

    // Points moved or not moved on apply based on appendable flag
    assert_eq!(read_segment_1.has_point(11.into()), !update_nonappendable);
    assert_eq!(read_segment_1.has_point(12.into()), !update_nonappendable);
    assert_eq!(read_segment_2.has_point(11.into()), update_nonappendable);
    assert_eq!(read_segment_2.has_point(12.into()), update_nonappendable);
}

/// Test applying points and conditionally moving them if operation versions are off
///
/// More specifically, this tests the move is still applied correctly even if segments already
/// have a newer version. That very situation can happen when replaying the WAL after a crash
/// where only some of the segments have been flushed properly.
///
/// Before <https://github.com/qdrant/qdrant/pull/4060> the copy and delete operation to move a
/// point to another segment may only be partially executed if an operation ID was given that
/// is older than the current segment version. It resulted in missing points. This test asserts
/// this cannot happen anymore.
#[rstest::rstest]
#[case::segments_older(false, false)]
#[case::non_appendable_newer_appendable_older(true, false)]
#[case::non_appendable_older_appendable_newer(false, true)]
#[case::segments_newer(true, true)]
fn test_apply_and_move_old_versions(
    #[case] segment_1_high_version: bool,
    #[case] segment_2_high_version: bool,
) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment1 = build_segment_1(dir.path());
    let mut segment2 = build_segment_2(dir.path());

    let hw_counter = HardwareCounterCell::new();

    // Insert operation 100 with point 123 and 456 into segment 1, and 789 into segment 2
    segment1
        .upsert_point(
            100,
            123.into(),
            segment::data_types::vectors::only_default_vector(&[0.0, 1.0, 2.0, 3.0]),
            &hw_counter,
        )
        .unwrap();
    segment1
        .upsert_point(
            100,
            456.into(),
            segment::data_types::vectors::only_default_vector(&[0.0, 1.0, 2.0, 3.0]),
            &hw_counter,
        )
        .unwrap();
    segment2
        .upsert_point(
            100,
            789.into(),
            segment::data_types::vectors::only_default_vector(&[0.0, 1.0, 2.0, 3.0]),
            &hw_counter,
        )
        .unwrap();

    // Bump segment version of segment 1 and/or 2 to a high value
    // Here we insert a random point to achieve this, normally this could happen on restart if
    // segments are not all flushed at the same time
    if segment_1_high_version {
        segment1
            .upsert_point(
                99999,
                99999.into(),
                segment::data_types::vectors::only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
                &hw_counter,
            )
            .unwrap();
    }
    if segment_2_high_version {
        segment2
            .upsert_point(
                99999,
                99999.into(),
                segment::data_types::vectors::only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
                &hw_counter,
            )
            .unwrap();
    }

    // Segment 1 is non-appendable, segment 2 is appendable
    segment1.appendable_flag = false;

    let mut holder = SegmentHolder::default();
    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    // Update point 123, 456 and 789 in the non-appendable segment to move it to segment 2
    let op_num = 101;
    let mut processed_points: Vec<PointIdType> = vec![];
    let mut processed_points2: Vec<PointIdType> = vec![];
    holder
        .apply_points_with_conditional_move(
            op_num,
            &[123.into(), 456.into(), 789.into()],
            |point_id, segment| {
                processed_points.push(point_id);
                assert!(segment.has_point(point_id));
                Ok(true)
            },
            |point_id, _, _| processed_points2.push(point_id),
            |_| false,
            &hw_counter,
        )
        .unwrap();
    assert_eq!(3, processed_points.len() + processed_points2.len());

    let locked_segment_1 = holder.get(sid1).unwrap().get();
    let read_segment_1 = locked_segment_1.read();
    let locked_segment_2 = holder.get(sid2).unwrap().get();
    let read_segment_2 = locked_segment_2.read();

    for i in processed_points2.iter() {
        assert!(read_segment_2.has_point(*i));
    }

    // Point 123 and 456 should have moved from segment 1 into 2
    assert!(!read_segment_1.has_point(123.into()));
    assert!(!read_segment_1.has_point(456.into()));
    assert!(!read_segment_1.has_point(789.into()));
    assert!(read_segment_2.has_point(123.into()));
    assert!(read_segment_2.has_point(456.into()));
    assert!(read_segment_2.has_point(789.into()));
}

#[test]
fn test_cow_operation() {
    const PAYLOAD_KEY: &str = "test-value";

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment1 = build_segment_1(dir.path());
    let mut segment2 = build_segment_1(dir.path());

    let hw_counter = HardwareCounterCell::new();

    segment2
        .upsert_point(
            100,
            123.into(),
            segment::data_types::vectors::only_default_vector(&[0.0, 1.0, 2.0, 3.0]),
            &hw_counter,
        )
        .unwrap();
    let mut payload = Payload::default();
    payload.0.insert(PAYLOAD_KEY.to_string(), 42.into());
    segment2
        .set_full_payload(100, 123.into(), &payload, &hw_counter)
        .unwrap();
    segment2.appendable_flag = false;

    let mut holder = SegmentHolder::default();
    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    {
        let locked_segment_1 = holder.get(sid1).unwrap().get();
        let read_segment_1 = locked_segment_1.read();
        assert!(!read_segment_1.has_point(123.into()));

        let locked_segment_2 = holder.get(sid2).unwrap().get();
        let read_segment_2 = locked_segment_2.read();
        assert!(read_segment_2.has_point(123.into()));
        let vector = read_segment_2
            .vector(DEFAULT_VECTOR_NAME, 123.into(), &hw_counter)
            .unwrap()
            .unwrap();
        assert_ne!(vector, VectorInternal::Dense(vec![9.0; 4]));
        assert_eq!(
            read_segment_2
                .payload(123.into(), &hw_counter)
                .unwrap()
                .get_value(&JsonPath::from_str(PAYLOAD_KEY).unwrap())[0],
            &Value::from(42)
        );
    }

    holder
        .apply_points_with_conditional_move(
            1010,
            &[123.into()],
            |_, _| unreachable!(),
            |_point_id, vectors, payload| {
                vectors.insert(
                    DEFAULT_VECTOR_NAME.to_owned(),
                    VectorInternal::Dense(vec![9.0; 4]),
                );
                payload.0.insert(PAYLOAD_KEY.to_string(), 2.into());
            },
            |_| false,
            &hw_counter,
        )
        .unwrap();

    let locked_segment_1 = holder.get(sid1).unwrap().get();
    let read_segment_1 = locked_segment_1.read();

    assert!(read_segment_1.has_point(123.into()));

    let new_vector = read_segment_1
        .vector(DEFAULT_VECTOR_NAME, 123.into(), &hw_counter)
        .unwrap()
        .unwrap();
    assert_eq!(new_vector, VectorInternal::Dense(vec![9.0; 4]));
    let new_payload_value = read_segment_1.payload(123.into(), &hw_counter).unwrap();
    assert_eq!(
        new_payload_value.get_value(&JsonPath::from_str(PAYLOAD_KEY).unwrap())[0],
        &Value::from(2)
    );
}

#[test]
fn test_points_deduplication() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment1 = build_segment_1(dir.path());
    let mut segment2 = build_segment_1(dir.path());

    let hw_counter = HardwareCounterCell::new();

    segment1
        .set_payload(100, 1.into(), &payload_json! {}, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(100, 2.into(), &payload_json! {}, &None, &hw_counter)
        .unwrap();

    segment2
        .set_payload(200, 4.into(), &payload_json! {}, &None, &hw_counter)
        .unwrap();
    segment2
        .set_payload(200, 5.into(), &payload_json! {}, &None, &hw_counter)
        .unwrap();

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    let res = deduplicate_points_sync(&holder).unwrap();

    assert_eq!(5, res);

    assert!(holder.get(sid1).unwrap().get().read().has_point(1.into()));
    assert!(holder.get(sid1).unwrap().get().read().has_point(2.into()));
    assert!(!holder.get(sid2).unwrap().get().read().has_point(1.into()));
    assert!(!holder.get(sid2).unwrap().get().read().has_point(2.into()));

    assert!(holder.get(sid2).unwrap().get().read().has_point(4.into()));
    assert!(holder.get(sid2).unwrap().get().read().has_point(5.into()));
    assert!(!holder.get(sid1).unwrap().get().read().has_point(4.into()));
    assert!(!holder.get(sid1).unwrap().get().read().has_point(5.into()));
}

/// Unit test for a specific bug we caught before.
///
/// See: <https://github.com/qdrant/qdrant/pull/5585>
#[test]
fn test_points_deduplication_bug() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment1 = empty_segment(dir.path());
    let mut segment2 = empty_segment(dir.path());

    let hw_counter = HardwareCounterCell::new();

    segment1
        .upsert_point(
            2,
            10.into(),
            segment::data_types::vectors::only_default_vector(&[0.0; 4]),
            &hw_counter,
        )
        .unwrap();
    segment2
        .upsert_point(
            3,
            10.into(),
            segment::data_types::vectors::only_default_vector(&[0.0; 4]),
            &hw_counter,
        )
        .unwrap();

    segment1
        .upsert_point(
            1,
            11.into(),
            segment::data_types::vectors::only_default_vector(&[0.0; 4]),
            &hw_counter,
        )
        .unwrap();
    segment2
        .upsert_point(
            2,
            11.into(),
            segment::data_types::vectors::only_default_vector(&[0.0; 4]),
            &hw_counter,
        )
        .unwrap();

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    let duplicate_count = holder
        .find_duplicated_points()
        .values()
        .map(|ids| ids.len())
        .sum::<usize>();
    assert_eq!(2, duplicate_count);

    let removed_count = deduplicate_points_sync(&holder).unwrap();
    assert_eq!(2, removed_count);

    assert!(!holder.get(sid1).unwrap().get().read().has_point(10.into()));
    assert!(holder.get(sid2).unwrap().get().read().has_point(10.into()));

    assert!(!holder.get(sid1).unwrap().get().read().has_point(11.into()));
    assert!(holder.get(sid2).unwrap().get().read().has_point(11.into()));

    assert_eq!(
        holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .available_point_count(),
        0,
    );
    assert_eq!(
        holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .available_point_count(),
        2,
    );
}

#[test]
fn test_points_deduplication_randomized() {
    const POINT_COUNT: usize = 1000;

    let mut rand = rand::rng();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let vector = segment::data_types::vectors::only_default_vector(&[0.0; 4]);

    let hw_counter = HardwareCounterCell::new();

    let mut segments = [
        empty_segment(dir.path()),
        empty_segment(dir.path()),
        empty_segment(dir.path()),
        empty_segment(dir.path()),
        empty_segment(dir.path()),
    ];

    // Insert points into all segments with random versions
    let mut highest_point_version = HashMap::new();
    for id in 0..POINT_COUNT {
        let mut max_version = 0;
        let point_id = PointIdType::from(id as u64);

        for segment in &mut segments {
            let version = rand.random_range(1..10);
            segment
                .upsert_point(version, point_id, vector.clone(), &hw_counter)
                .unwrap();
            max_version = version.max(max_version);
        }

        highest_point_version.insert(id, max_version);
    }

    // Put segments into holder
    let mut holder = SegmentHolder::default();
    let segment_ids = segments
        .into_iter()
        .map(|segment| holder.add_new(segment))
        .collect::<Vec<_>>();

    let duplicate_count = holder
        .find_duplicated_points()
        .values()
        .map(|ids| ids.len())
        .sum::<usize>();
    assert_eq!(POINT_COUNT * (segment_ids.len() - 1), duplicate_count);

    let removed_count = deduplicate_points_sync(&holder).unwrap();
    assert_eq!(POINT_COUNT * (segment_ids.len() - 1), removed_count);

    // Assert points after deduplication
    for id in 0..POINT_COUNT {
        let point_id = PointIdType::from(id as u64);
        let max_version = highest_point_version[&id];

        let found_versions = segment_ids
            .iter()
            .filter_map(|segment_id| {
                holder
                    .get(*segment_id)
                    .unwrap()
                    .get()
                    .read()
                    .point_version(point_id)
            })
            .collect::<Vec<_>>();

        // We must have exactly one version, and it must be the highest we inserted
        assert_eq!(
            found_versions.len(),
            1,
            "point version must be maximum known version",
        );
        assert_eq!(
            found_versions[0], max_version,
            "point version must be maximum known version",
        );
    }
}

fn deduplicate_points_sync(holder: &SegmentHolder) -> OperationResult<usize> {
    let mut removed_points = 0;

    for task in holder.deduplicate_points_tasks() {
        removed_points += task()?;
    }

    Ok(removed_points)
}

#[test]
fn test_snapshot_all() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);
    assert_ne!(sid1, sid2);

    let holder = Arc::new(RwLock::new(holder));

    let before_ids = holder
        .read()
        .iter()
        .map(|(id, _)| *id)
        .collect::<HashSet<_>>();

    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
    let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(&snapshot_file).unwrap());

    let payload_schema_file = dir.path().join("payload.schema");
    let schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    SegmentHolder::snapshot_all_segments(
        holder.clone(),
        segments_dir.path(),
        None,
        schema,
        temp_dir.path(),
        &tar,
        SnapshotFormat::Regular,
        None,
    )
    .unwrap();

    let after_ids = holder
        .read()
        .iter()
        .map(|(id, _)| *id)
        .collect::<HashSet<_>>();

    assert_eq!(
        before_ids, after_ids,
        "segment holder IDs before and after snapshotting must be equal",
    );

    let mut tar = tar::Archive::new(File::open(&snapshot_file).unwrap());
    let archive_count = tar.entries_with_seek().unwrap().count();
    // one archive produced per concrete segment in the SegmentHolder
    assert_eq!(archive_count, 2);
}

#[test]
fn test_double_proxies() {
    let hw_counter = HardwareCounterCell::disposable();

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = build_segment_1(dir.path());

    let mut holder = SegmentHolder::default();

    let _sid1 = holder.add_new(segment1);

    let holder = Arc::new(RwLock::new(holder));

    let before_segment_ids = holder
        .read()
        .iter()
        .map(|(id, _)| *id)
        .collect::<HashSet<_>>();

    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
    let payload_schema_file = dir.path().join("payload.schema");
    let schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    let (inner_proxies, inner_tmp_segment, inner_segments_lock) =
        SegmentHolder::proxy_all_segments(
            holder.upgradable_read(),
            segments_dir.path(),
            None,
            schema.clone(),
        )
        .unwrap();

    // check inner proxy contains points
    let points = inner_proxies[0]
        .1
        .get()
        .read()
        .read_range(Some(1.into()), None);
    assert_eq!(&points, &[1.into(), 2.into(), 3.into(), 4.into(), 5.into()]);

    // Writing to inner proxy segment
    inner_proxies[0]
        .1
        .get()
        .write()
        .delete_point(10, 1.into(), &hw_counter)
        .unwrap();

    let (outer_proxies, outer_tmp_segment, outer_segments_lock) =
        SegmentHolder::proxy_all_segments(inner_segments_lock, segments_dir.path(), None, schema)
            .unwrap();

    // Writing to outer proxy segment
    outer_proxies[0]
        .1
        .get()
        .write()
        .upsert_point(
            100,
            1.into(),
            only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
            &hw_counter,
        )
        .unwrap();

    // Unproxy once
    SegmentHolder::unproxy_all_segments(outer_segments_lock, outer_proxies, outer_tmp_segment)
        .unwrap();

    // Unproxy twice
    SegmentHolder::unproxy_all_segments(holder.upgradable_read(), inner_proxies, inner_tmp_segment)
        .unwrap();

    let after_segment_ids = holder
        .read()
        .iter()
        .map(|(id, _)| *id)
        .collect::<HashSet<_>>();

    // Check that we have one new segment
    let diff: HashSet<_> = after_segment_ids.difference(&before_segment_ids).collect();
    assert_eq!(
        diff.len(),
        1,
        "There should be one new segment after unproxying"
    );
}

/// Test proxy propagating older delete to wrapped segment is sound
///
/// A specific scenario that asserts the delete propagation logic is sound. It tries to propagate
/// an older delete version to a wrapped segment that already has a newer point version. It
/// previously had a faulty debug assertion in place that triggers in this case.
///
/// This scenario is possible if:
/// - there are at least two layers of two proxies
/// - one of the outer proxies is unproxied half way
/// - a new point version is upserted through the unproxied segment (now being the inner proxy)
///
/// See: <https://github.com/qdrant/qdrant/pull/7208>
#[test]
fn test_proxy_propagate_older_delete_to_wrapped() {
    let hw_counter = HardwareCounterCell::disposable();

    let pid = ExtendedPointId::from(1);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = empty_segment(dir.path());
    let segment2 = empty_segment(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    let holder = Arc::new(RwLock::new(holder));

    let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
    let payload_schema_file = dir.path().join("payload.schema");
    let schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    // Create inner proxies, we have two of them
    let (inner_proxies, inner_tmp_segment, inner_segments_lock) =
        SegmentHolder::proxy_all_segments(
            holder.upgradable_read(),
            segments_dir.path(),
            None,
            schema.clone(),
        )
        .unwrap();
    assert_eq!(inner_proxies.len(), 2);

    // Insert version 100 in first inner proxy
    inner_proxies[0]
        .1
        .get()
        .write()
        .upsert_point(
            100,
            pid,
            only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
            &hw_counter,
        )
        .unwrap();

    // Create outer proxies, we have two of them
    let (mut outer_proxies, outer_tmp_segment, mut outer_segments_lock) =
        SegmentHolder::proxy_all_segments(inner_segments_lock, segments_dir.path(), None, schema)
            .unwrap();
    assert_eq!(outer_proxies.len(), 2);

    // Insert version 200 then delete version 210 in first outer proxy
    outer_proxies[0]
        .1
        .get()
        .write()
        .upsert_point(
            200,
            pid,
            only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
            &hw_counter,
        )
        .unwrap();
    outer_proxies[0]
        .1
        .get()
        .write()
        .delete_point(210, pid, &hw_counter)
        .unwrap();

    // First outer proxy has point deleted, second outer proxy still has it
    assert!(
        !outer_proxies[0].1.get().read().has_point(pid),
        "first proxy must not have point",
    );
    assert_eq!(
        outer_proxies[1].1.get().read().point_version(pid),
        Some(100),
        "second outer proxy must have point version 100 through shared inner tmp segment",
    );

    // Insert version 300 in second outer proxy
    outer_proxies[1]
        .1
        .get()
        .write()
        .upsert_point(
            300,
            pid,
            only_default_vector(&[1.0, 1.0, 1.0, 1.0]),
            &hw_counter,
        )
        .unwrap();

    // Both outer proxies must have point
    assert_eq!(
        outer_proxies[0].1.get().read().point_version(pid),
        Some(300),
        "second outer proxy must have point version 300, latest upsert through shared outer tmp segment",
    );
    assert_eq!(
        outer_proxies[1].1.get().read().point_version(pid),
        Some(300),
        "second outer proxy must have point version 300, latest upsert through shared outer tmp segment",
    );

    // Both the outer and inner tmp segment must have point
    assert_eq!(
        outer_tmp_segment.get().read().point_version(pid),
        Some(300),
        "shared outer tmp segment has point version 300 from latest upsert",
    );
    assert_eq!(
        inner_tmp_segment.get().read().point_version(pid),
        Some(100),
        "shared inner tmp segment has point version 100 from first upsert",
    );

    // Assert both outer proxies have delete markers we expect
    match (&outer_proxies[0].1, &outer_proxies[1].1) {
        (LockedSegment::Proxy(first), LockedSegment::Proxy(second)) => {
            assert_eq!(
                first.read().get_deleted_points().read().deref(),
                &AHashMap::from_iter([(
                    pid,
                    ProxyDeletedPoint {
                        // Delete version 210 in first outer proxy
                        operation_version: 210,
                        // Delete version 210 in first outer proxy
                        local_version: 210,
                    }
                )]),
            );
            assert_eq!(
                second.read().get_deleted_points().read().deref(),
                &AHashMap::from_iter([(
                    pid,
                    ProxyDeletedPoint {
                        // Initial upsert version 100 through shared inner tmp segment
                        local_version: 100,
                        // Latest upsert version 300 did CoW to shared outer tmp segment
                        operation_version: 300,
                    }
                )]),
            );
        }
        _ => unreachable!(),
    }

    // Unproxy only first outer proxy
    // Happens in `proxy_all_segments_and_apply` function that immediately tries to unproxy
    // proxy segments it has applied an operation on, without waiting until all proxy segments
    // have applied the operation.
    let (unproxy_id, unproxy_segment) = outer_proxies.remove(0);
    outer_segments_lock =
        SegmentHolder::try_unproxy_segment(outer_segments_lock, unproxy_id, unproxy_segment)
            .expect("should always succeed");

    // Delete propagation at unproxy deleted version 210 from shared inner tmp segment which had version 100
    assert!(
        !inner_tmp_segment.get().read().has_point(pid),
        "inner tmp segment must not have point",
    );

    // Insert point version 400 into now unproxied segment (which now became the first inner proxy)
    outer_segments_lock
        .get(unproxy_id)
        .unwrap()
        .get()
        .write()
        .upsert_point(
            400,
            pid,
            only_default_vector(&[1.0, 1.0, 1.0, 1.0]),
            &hw_counter,
        )
        .unwrap();

    // Last outer proxy still views point version 300
    // Shared inner tmp segment has point version 400 from latest upsert
    assert_eq!(
        outer_proxies[0].1.get().read().point_version(pid),
        Some(300),
        "last outer proxy has point version 300 through shared outer tmp segment",
    );
    assert_eq!(
        outer_tmp_segment.get().read().point_version(pid),
        Some(300),
        "outer tmp segment has point version 300",
    );
    assert_eq!(
        inner_tmp_segment.get().read().point_version(pid),
        Some(400),
        "inner tmp segment has point version 400 from latest upsert",
    );

    // Unproxy last outer proxy
    //
    // The proxy will propagate its queued deletes to wrapped segment:
    // It will try to delete point version 300, but it's wrapped segment already has point version
    // 400 which is shared through the inner tmp segment. Because the delete version is older, it
    // will not delete the newer point.
    // The faulty debug assertion assumed the wrapped segment could not change and therefore delete
    // version must always be newer. This is not true, because if the wrapped segment is also a
    // proxy it can share state with other segments. If these other segments receive updates, the
    // wrapped segment might view newer point versions.
    SegmentHolder::unproxy_all_segments(outer_segments_lock, outer_proxies, outer_tmp_segment)
        .unwrap();

    // Inner proxies and shared inner tmp segment all see point version 400
    assert_eq!(
        inner_proxies[0].1.get().read().point_version(pid),
        Some(400),
        "last outer proxy has point version 300 through shared outer tmp segment",
    );
    assert_eq!(
        inner_proxies[1].1.get().read().point_version(pid),
        Some(400),
        "last outer proxy has point version 300 through shared outer tmp segment",
    );
    assert_eq!(
        inner_tmp_segment.get().read().point_version(pid),
        Some(400),
        "inner tmp segment has point version 400 from latest upsert",
    );

    // Unproxy inner proxies
    // Note: in our current codebase we don't unproxy like this twice
    eprintln!("\n\n\nUNPROXY INNER PROXIES");
    SegmentHolder::unproxy_all_segments(holder.upgradable_read(), inner_proxies, inner_tmp_segment)
        .unwrap();

    // Original segments should not have point, newly added tmp segment does have the point
    assert_eq!(
        holder.read().len(),
        4,
        "segment holder must have 4 segments, 2 original plus 2 temporary",
    );
    assert!(
        !holder.read().get(sid1).unwrap().get().read().has_point(pid),
        "first original segment must not have the point",
    );
    assert!(
        !holder.read().get(sid2).unwrap().get().read().has_point(pid),
        "second original segment must not have the point",
    );
    assert_eq!(
        holder
            .read()
            .get(sid2 + 1)
            .unwrap()
            .get()
            .read()
            .point_version(pid),
        Some(300),
        "outer tmp segment still has point version 300",
    );
    assert_eq!(
        holder
            .read()
            .get(sid2 + 2)
            .unwrap()
            .get()
            .read()
            .point_version(pid),
        Some(400),
        "inner tmp segment must have latest point version 400",
    );
}
