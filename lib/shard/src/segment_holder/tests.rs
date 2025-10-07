use std::collections::HashMap;
use std::str::FromStr;

use rand::Rng;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorInternal};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadContainer};
use serde_json::Value;
use tempfile::Builder;

use super::*;
use crate::fixtures::*;

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
fn test_double_proxies() {
    let hw_counter = HardwareCounterCell::disposable();

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment1 = build_segment_1(dir.path());

    let mut holder = SegmentHolder::default();

    let locked_segment1 = LockedSegment::from(segment1);

    let _sid1 = holder.add_new_locked(locked_segment1.clone());

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

    let mut has_point = false;
    for (_proxy_id, proxy) in &outer_proxies {
        let proxy_read = proxy.get().read();

        if proxy_read.has_point(2.into()) {
            has_point = true;
            let payload = proxy_read.payload(2.into(), &hw_counter).unwrap();

            assert!(
                payload.0.get("color").is_some(),
                "Payload should be readable in double proxy"
            );
            drop(proxy_read);

            proxy
                .get()
                .write()
                .delete_point(11, 2.into(), &hw_counter)
                .unwrap();

            break;
        }
    }

    assert!(has_point, "Point should be present in double proxy");

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
        0,
        "There should be no new segment after unproxying"
    );

    let has_point_1 = locked_segment1.get().read().has_point(1.into()); // Deleted in inner proxy
    let has_point_2 = locked_segment1.get().read().has_point(2.into()); // Deleted in outer proxy
    let has_point_3 = locked_segment1.get().read().has_point(3.into()); // Not deleted

    assert!(!has_point_1, "Point 1 should be deleted");
    assert!(!has_point_2, "Point 2 should be deleted");
    assert!(has_point_3, "Point 3 should be present");
}
