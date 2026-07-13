use std::collections::HashMap;
use std::str::FromStr;

use rand::RngExt;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorInternal};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadContainer};
use serde_json::Value;
use tempfile::Builder;

use super::*;
use crate::fixtures::*;
use crate::segment_holder::locked::LockedSegmentHolder;

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

#[test]
fn test_apply_to_appendable() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let appendable_segment = build_segment_1(dir.path());

    let mut immutable_segment = build_segment_2(dir.path());
    immutable_segment.appendable_flag = false;

    let mut holder = SegmentHolder::default();
    let appendable_id = holder.add_new(appendable_segment);
    let immutable_id = holder.add_new(immutable_segment);

    let point_ids = [1.into(), 2.into(), 11.into(), 12.into()];
    let mut updated_in_place = Vec::new();
    let mut moved_to_appendable = Vec::new();

    holder
        .apply_points_with_conditional_move(
            100,
            &point_ids,
            |point_id, segment| {
                updated_in_place.push(point_id);
                assert!(segment.has_point(point_id, common::types::DeferredBehavior::WithDeferred));
                Ok(true)
            },
            |point_id, _, _, _| {
                moved_to_appendable.push(point_id);
            },
            &HardwareCounterCell::new(),
        )
        .unwrap();

    // All points were updated
    assert_eq!(
        point_ids.len(),
        updated_in_place.len() + moved_to_appendable.len()
    );

    let appendable_segment = holder.get(appendable_id).unwrap().get();
    let appendable_segment = appendable_segment.read();

    let immutable_segment = holder.get(immutable_id).unwrap().get();
    let immutable_segment = immutable_segment.read();

    // All points were moved from immutable into appendable segment
    for point_id in point_ids {
        assert!(
            appendable_segment.has_point(point_id, common::types::DeferredBehavior::WithDeferred)
        );
    }

    assert!(!immutable_segment.has_point(11.into(), common::types::DeferredBehavior::WithDeferred));
    assert!(!immutable_segment.has_point(12.into(), common::types::DeferredBehavior::WithDeferred));
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
                assert!(segment.has_point(point_id, common::types::DeferredBehavior::WithDeferred));
                Ok(true)
            },
            |point_id, _, _, _| processed_points2.push(point_id),
            &hw_counter,
        )
        .unwrap();
    assert_eq!(3, processed_points.len() + processed_points2.len());

    let locked_segment_1 = holder.get(sid1).unwrap().get();
    let read_segment_1 = locked_segment_1.read();
    let locked_segment_2 = holder.get(sid2).unwrap().get();
    let read_segment_2 = locked_segment_2.read();

    for i in &processed_points2 {
        assert!(read_segment_2.has_point(*i, common::types::DeferredBehavior::WithDeferred));
    }

    // Point 123 and 456 should have moved from segment 1 into 2
    assert!(!read_segment_1.has_point(123.into(), common::types::DeferredBehavior::WithDeferred));
    assert!(!read_segment_1.has_point(456.into(), common::types::DeferredBehavior::WithDeferred));
    assert!(!read_segment_1.has_point(789.into(), common::types::DeferredBehavior::WithDeferred));
    assert!(read_segment_2.has_point(123.into(), common::types::DeferredBehavior::WithDeferred));
    assert!(read_segment_2.has_point(456.into(), common::types::DeferredBehavior::WithDeferred));
    assert!(read_segment_2.has_point(789.into(), common::types::DeferredBehavior::WithDeferred));
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
        assert!(
            !read_segment_1.has_point(123.into(), common::types::DeferredBehavior::WithDeferred)
        );

        let locked_segment_2 = holder.get(sid2).unwrap().get();
        let read_segment_2 = locked_segment_2.read();
        assert!(
            read_segment_2.has_point(123.into(), common::types::DeferredBehavior::WithDeferred)
        );
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
            |_point_id, _raw_vectors, vectors, payload| {
                vectors.insert(
                    DEFAULT_VECTOR_NAME.to_owned(),
                    VectorInternal::Dense(vec![9.0; 4]),
                );
                payload.0.insert(PAYLOAD_KEY.to_string(), 2.into());
            },
            &hw_counter,
        )
        .unwrap();

    let locked_segment_1 = holder.get(sid1).unwrap().get();
    let read_segment_1 = locked_segment_1.read();

    assert!(read_segment_1.has_point(123.into(), common::types::DeferredBehavior::WithDeferred));

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

/// A CoW move into an append-only destination must allocate exactly one
/// internal slot: raw vectors, updated vectors and payload travel in one
/// fused write (`upsert_moved_point`), not one clone per part.
#[test]
fn test_cow_move_append_only_single_slot() {
    use segment::id_tracker::IdTrackerRead as _;

    const PAYLOAD_KEY: &str = "test-value";

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut destination = empty_segment(dir.path());
    destination.append_only_mutations = true;
    let destination_id_tracker = destination.id_tracker.clone();

    let mut source = build_segment_1(dir.path());
    let hw_counter = HardwareCounterCell::new();
    source
        .upsert_point(
            100,
            123.into(),
            segment::data_types::vectors::only_default_vector(&[0.0, 1.0, 2.0, 3.0]),
            &hw_counter,
        )
        .unwrap();
    source.appendable_flag = false;

    let mut holder = SegmentHolder::default();
    let dst_sid = holder.add_new(destination);
    holder.add_new(source);

    holder
        .apply_points_with_conditional_move(
            1010,
            &[123.into()],
            |_, _| unreachable!("the point's segment is non-appendable, it must be moved"),
            |_point_id, _raw_vectors, vectors, payload| {
                vectors.insert(
                    DEFAULT_VECTOR_NAME.to_owned(),
                    VectorInternal::Dense(vec![9.0; 4]),
                );
                payload.0.insert(PAYLOAD_KEY.to_string(), 2.into());
            },
            &hw_counter,
        )
        .unwrap();

    // The moved point occupies exactly one slot in the destination, with the
    // overlaid vector and payload.
    assert_eq!(destination_id_tracker.borrow().total_point_count(), 1);
    let locked_destination = holder.get(dst_sid).unwrap().get();
    let read_destination = locked_destination.read();
    assert_eq!(
        read_destination
            .vector(DEFAULT_VECTOR_NAME, 123.into(), &hw_counter)
            .unwrap(),
        Some(VectorInternal::Dense(vec![9.0; 4])),
    );
    assert_eq!(
        read_destination
            .payload(123.into(), &hw_counter)
            .unwrap()
            .get_value(&JsonPath::from_str(PAYLOAD_KEY).unwrap())[0],
        &Value::from(2)
    );
}

/// TurboQuant-as-datatype vectors must survive repeated CoW moves between
/// segments without degrading.
///
/// The CoW arm of [`SegmentHolder::apply_points_with_conditional_move`] reads
/// the point's vectors decoded to `f32` and re-inserts them with
/// `upsert_point`, so a TQ-datatype vector is dequantized and requantized on
/// every move. TurboQuant requantization is not idempotent — for Dot/L2 the
/// stored per-vector scale factor picks up the centroid-norm bias on every
/// cycle — so each move degrades the vector a little further beyond the
/// initial (expected, one-off) quantization loss.
///
/// This test ping-pongs one point between two TQ segments, and asserts the
/// decoded vector never drifts from its first-generation value (the read-back
/// right after initial ingestion). The holder classifies segments as
/// (non-)appendable once, at insertion, so each round uses a fresh holder with
/// the segment roles swapped instead of flipping flags under a live holder.
#[test]
fn test_cow_move_does_not_degrade_turbo_vectors() {
    use segment::data_types::vectors::only_default_vector;
    use segment::types::{Indexes, VectorDataConfig, VectorStorageDatatype, VectorStorageType};

    const DIM: usize = 128;
    const ROUNDTRIPS: u64 = 32;

    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Dot,
                storage_type: VectorStorageType::ChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype: Some(VectorStorageDatatype::Turbo4),
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (mut segment_a, _) = build_segment(dir.path(), &config, None, true).unwrap();
    let (segment_b, _) = build_segment(dir.path(), &config, None, true).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let point_id: PointIdType = 7.into();
    let original: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.37).sin()).collect();
    segment_a
        .upsert_point(100, point_id, only_default_vector(&original), &hw_counter)
        .unwrap();

    let read_dense = |segment: &dyn SegmentEntry| -> Vec<f32> {
        match segment
            .vector(DEFAULT_VECTOR_NAME, point_id, &hw_counter)
            .unwrap()
            .unwrap()
        {
            VectorInternal::Dense(vector) => vector,
            VectorInternal::Sparse(_) | VectorInternal::MultiDense(_) => {
                panic!("expected a dense vector")
            }
        }
    };

    // First-generation read-back: the original vector after its initial
    // (expected, one-off) quantization. Moves must preserve it exactly.
    let first_generation = read_dense(&segment_a);

    let segment_a = Arc::new(RwLock::new(segment_a));
    let segment_b = Arc::new(RwLock::new(segment_b));

    // L2 distance of each round's read-back from the first generation.
    let mut drift = Vec::new();
    for round in 0..ROUNDTRIPS {
        let (source, destination) = if round % 2 == 0 {
            (&segment_a, &segment_b)
        } else {
            (&segment_b, &segment_a)
        };

        // The segment holding the point is non-appendable and the other one is
        // the only appendable destination, so the call must take the CoW-move
        // arm.
        source.write().appendable_flag = false;
        destination.write().appendable_flag = true;
        let mut holder = SegmentHolder::default();
        holder.add_new_locked(LockedSegment::Original(source.clone()));
        holder.add_new_locked(LockedSegment::Original(destination.clone()));

        holder
            .apply_points_with_conditional_move(
                101 + round,
                &[point_id],
                |_, _| unreachable!("the point's segment is non-appendable, it must be moved"),
                |_, _, _, _| {}, // no-op: a pure move
                &hw_counter,
            )
            .unwrap();

        let source = source.read();
        let destination = destination.read();
        assert!(!source.has_point(point_id, DeferredBehavior::WithDeferred));
        assert!(destination.has_point(point_id, DeferredBehavior::WithDeferred));

        let read_back = read_dense(&*destination);
        drift.push(
            first_generation
                .iter()
                .zip(&read_back)
                .map(|(&a, &b)| (a - b).powi(2))
                .sum::<f32>()
                .sqrt(),
        );
    }

    assert!(
        drift.iter().all(|&distance| distance == 0.0),
        "TurboQuant vector degraded across CoW moves; L2 distance from the \
         first-generation read-back after each move: {drift:?}",
    );
}

/// A CoW move where the operation overlays one named vector must re-encode
/// only that name: the untouched sibling TQ vector travels as verbatim bytes
/// (bit-identical read-back), and the overlaid name reads back exactly like a
/// fresh direct ingest of the same data.
#[test]
fn test_cow_move_overlay_preserves_untouched_turbo_vector() {
    use segment::types::{Indexes, VectorDataConfig, VectorStorageDatatype, VectorStorageType};

    const DIM: usize = 128;
    const KEEP: &str = "keep";
    const REPLACE: &str = "replace";

    let vector_config = VectorDataConfig {
        size: DIM,
        distance: Distance::Dot,
        storage_type: VectorStorageType::ChunkedMmap,
        index: Indexes::Plain {},
        quantization_config: None,
        multivector_config: None,
        datatype: Some(VectorStorageDatatype::Turbo4),
    };
    let config = SegmentConfig {
        vector_data: HashMap::from([
            (KEEP.to_owned(), vector_config.clone()),
            (REPLACE.to_owned(), vector_config),
        ]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (mut source, _) = build_segment(dir.path(), &config, None, true).unwrap();
    let (destination, _) = build_segment(dir.path(), &config, None, true).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let point_id: PointIdType = 7.into();
    let keep_vec: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.37).sin()).collect();
    let old_replace: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.11).cos()).collect();
    let fresh_replace: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.73).sin()).collect();

    source
        .upsert_point(
            100,
            point_id,
            NamedVectors::from_pairs([
                (KEEP.to_owned(), keep_vec.clone()),
                (REPLACE.to_owned(), old_replace.clone()),
            ]),
            &hw_counter,
        )
        .unwrap();

    let read_dense = |segment: &dyn SegmentEntry, name: &str| -> Vec<f32> {
        match segment
            .vector(name, point_id, &hw_counter)
            .unwrap()
            .unwrap()
        {
            VectorInternal::Dense(vector) => vector,
            VectorInternal::Sparse(_) | VectorInternal::MultiDense(_) => {
                panic!("expected a dense vector")
            }
        }
    };

    // First-generation read-back of the untouched name: must survive the move
    // bit-identically.
    let first_generation_keep = read_dense(&source, KEEP);

    // Oracle for the overlaid name: a fresh direct ingest into a same-config
    // segment (TQ encoding is deterministic for equal configs).
    let (mut oracle, _) = build_segment(dir.path(), &config, None, true).unwrap();
    oracle
        .upsert_point(
            100,
            point_id,
            NamedVectors::from_pairs([
                (KEEP.to_owned(), keep_vec.clone()),
                (REPLACE.to_owned(), fresh_replace.clone()),
            ]),
            &hw_counter,
        )
        .unwrap();
    let expected_replace = read_dense(&oracle, REPLACE);

    // Same pattern as test_cow_move_does_not_degrade_turbo_vectors: keep own
    // Arc handles so the read-back guard derefs to &Segment (→ &dyn SegmentEntry).
    source.appendable_flag = false;
    let source = Arc::new(RwLock::new(source));
    let destination = Arc::new(RwLock::new(destination));
    let mut holder = SegmentHolder::default();
    holder.add_new_locked(LockedSegment::Original(source.clone()));
    holder.add_new_locked(LockedSegment::Original(destination.clone()));

    holder
        .apply_points_with_conditional_move(
            101,
            &[point_id],
            |_, _| unreachable!("the point's segment is non-appendable, it must be moved"),
            |_, _raw_vectors, updated_vectors, _| {
                updated_vectors.insert(
                    REPLACE.to_owned(),
                    VectorInternal::Dense(fresh_replace.clone()),
                );
            },
            &hw_counter,
        )
        .unwrap();

    let destination = destination.read();
    assert!(destination.has_point(point_id, DeferredBehavior::WithDeferred));

    assert_eq!(
        read_dense(&*destination, KEEP),
        first_generation_keep,
        "untouched named vector must travel as verbatim bytes",
    );
    assert_eq!(
        read_dense(&*destination, REPLACE),
        expected_replace,
        "overlaid named vector must equal a fresh direct ingest",
    );
}

/// A CoW move where the operation deletes one named vector must drop exactly
/// that name at the destination while the surviving TQ vector travels as
/// verbatim bytes (bit-identical read-back).
#[test]
fn test_cow_move_delete_name_preserves_survivor() {
    use segment::types::{Indexes, VectorDataConfig, VectorStorageDatatype, VectorStorageType};

    const DIM: usize = 128;
    const KEEP: &str = "keep";
    const DROP: &str = "drop";

    let vector_config = VectorDataConfig {
        size: DIM,
        distance: Distance::Dot,
        storage_type: VectorStorageType::ChunkedMmap,
        index: Indexes::Plain {},
        quantization_config: None,
        multivector_config: None,
        datatype: Some(VectorStorageDatatype::Turbo4),
    };
    let config = SegmentConfig {
        vector_data: HashMap::from([
            (KEEP.to_owned(), vector_config.clone()),
            (DROP.to_owned(), vector_config),
        ]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let (mut source, _) = build_segment(dir.path(), &config, None, true).unwrap();
    let (destination, _) = build_segment(dir.path(), &config, None, true).unwrap();

    let hw_counter = HardwareCounterCell::new();
    let point_id: PointIdType = 7.into();
    let keep_vec: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.37).sin()).collect();
    let drop_vec: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.11).cos()).collect();

    source
        .upsert_point(
            100,
            point_id,
            NamedVectors::from_pairs([
                (KEEP.to_owned(), keep_vec.clone()),
                (DROP.to_owned(), drop_vec.clone()),
            ]),
            &hw_counter,
        )
        .unwrap();

    let read_dense = |segment: &dyn SegmentEntry, name: &str| -> Vec<f32> {
        match segment
            .vector(name, point_id, &hw_counter)
            .unwrap()
            .unwrap()
        {
            VectorInternal::Dense(vector) => vector,
            VectorInternal::Sparse(_) | VectorInternal::MultiDense(_) => {
                panic!("expected a dense vector")
            }
        }
    };
    let first_generation_keep = read_dense(&source, KEEP);

    // Own Arc handles, as in test_cow_move_does_not_degrade_turbo_vectors.
    source.appendable_flag = false;
    let source = Arc::new(RwLock::new(source));
    let destination = Arc::new(RwLock::new(destination));
    let mut holder = SegmentHolder::default();
    holder.add_new_locked(LockedSegment::Original(source.clone()));
    holder.add_new_locked(LockedSegment::Original(destination.clone()));

    holder
        .apply_points_with_conditional_move(
            101,
            &[point_id],
            |_, _| unreachable!("the point's segment is non-appendable, it must be moved"),
            |_, raw_vectors, _, _| {
                raw_vectors.retain(|(name, _)| name != DROP);
            },
            &hw_counter,
        )
        .unwrap();

    let destination = destination.read();
    assert!(destination.has_point(point_id, DeferredBehavior::WithDeferred));

    assert_eq!(
        read_dense(&*destination, KEEP),
        first_generation_keep,
        "surviving named vector must travel as verbatim bytes",
    );
    assert!(
        destination
            .vector(DROP, point_id, &hw_counter)
            .unwrap()
            .is_none(),
        "deleted named vector must not exist at the destination",
    );
}

/// A CoW move between segments whose vector configs differ only in segment
/// role fields (storage type, index, quantization) must succeed — that is the
/// normal shape of a move out of an optimizer-built segment into an appendable
/// one. Only encoding-relevant fields (size, distance, datatype, multivector
/// config) must match for raw bytes to be portable.
///
/// Regression test: the CoW arm's config guard compared full
/// `VectorDataConfig` structs and panicked (debug builds) on every move out of
/// an optimized segment.
#[test]
fn test_cow_move_allows_role_config_differences() {
    use segment::types::{Indexes, VectorDataConfig, VectorStorageType};

    const DIM: usize = 128;

    let make_config = |storage_type: VectorStorageType| SegmentConfig {
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
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    // Source and destination differ in storage type only; the byte encoding
    // (f32 dense) is identical on both sides.
    let (mut source, _) = build_segment(
        dir.path(),
        &make_config(VectorStorageType::InRamChunkedMmap),
        None,
        true,
    )
    .unwrap();
    let (destination, _) = build_segment(
        dir.path(),
        &make_config(VectorStorageType::ChunkedMmap),
        None,
        true,
    )
    .unwrap();

    let hw_counter = HardwareCounterCell::new();
    let point_id: PointIdType = 7.into();
    let original: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.37).sin()).collect();
    source
        .upsert_point(
            100,
            point_id,
            segment::data_types::vectors::only_default_vector(&original),
            &hw_counter,
        )
        .unwrap();

    // Own Arc handles, as in test_cow_move_does_not_degrade_turbo_vectors.
    source.appendable_flag = false;
    let source = Arc::new(RwLock::new(source));
    let destination = Arc::new(RwLock::new(destination));
    let mut holder = SegmentHolder::default();
    holder.add_new_locked(LockedSegment::Original(source.clone()));
    holder.add_new_locked(LockedSegment::Original(destination.clone()));

    holder
        .apply_points_with_conditional_move(
            101,
            &[point_id],
            |_, _| unreachable!("the point's segment is non-appendable, it must be moved"),
            |_, _, _, _| {}, // no-op: a pure move
            &hw_counter,
        )
        .unwrap();

    let destination = destination.read();
    assert!(destination.has_point(point_id, DeferredBehavior::WithDeferred));
    match destination
        .vector(DEFAULT_VECTOR_NAME, point_id, &hw_counter)
        .unwrap()
        .unwrap()
    {
        VectorInternal::Dense(vector) => assert_eq!(
            vector, original,
            "f32 dense vector must travel losslessly across role-config differences",
        ),
        VectorInternal::Sparse(_) | VectorInternal::MultiDense(_) => {
            panic!("expected a dense vector")
        }
    }
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

    assert!(
        holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(1.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(2.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        !holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(1.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        !holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(2.into(), common::types::DeferredBehavior::WithDeferred)
    );

    assert!(
        holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(4.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(5.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        !holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(4.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        !holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(5.into(), common::types::DeferredBehavior::WithDeferred)
    );
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

    assert!(
        !holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(10.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(10.into(), common::types::DeferredBehavior::WithDeferred)
    );

    assert!(
        !holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(11.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(11.into(), common::types::DeferredBehavior::WithDeferred)
    );

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

/// Base test without deferred points
#[test]
fn test_find_points_to_update_and_delete() {
    use std::collections::HashSet;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let vec4 = segment::data_types::vectors::only_default_vector(&[0.0, 0.0, 0.0, 0.0]);

    // Segment 1: point 1 (v1), point 2 (v2), point 3 (v5), point 6 (v7)
    let mut segment1 = empty_segment(dir.path());
    segment1
        .upsert_point(1, 1.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(2, 2.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(5, 3.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(7, 6.into(), vec4.clone(), &hw_counter)
        .unwrap();

    // Segment 2: point 2 (v3), point 3 (v4), point 4 (v6), point 6 (v7)
    let mut segment2 = empty_segment(dir.path());
    segment2
        .upsert_point(3, 2.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(4, 3.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(6, 4.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(7, 6.into(), vec4.clone(), &hw_counter)
        .unwrap();

    let mut holder = SegmentHolder::default();
    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    // Query for points 1, 2, 3, 4, 5, 6
    let ids: Vec<PointIdType> = vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into(), 6.into()];
    let (to_update, to_delete) = holder.find_points_to_update_and_delete(&ids);

    let update_set = |sid: &SegmentId| -> HashSet<PointIdType> {
        to_update
            .get(sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };
    let delete_set = |sid: &SegmentId| -> HashSet<PointIdType> {
        to_delete
            .get(sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };

    let update_1 = update_set(&sid1);
    let update_2 = update_set(&sid2);
    let delete_1 = delete_set(&sid1);
    let delete_2 = delete_set(&sid2);

    // Point 1 (v1): only in segment1 → update in seg1
    assert!(update_1.contains(&1.into()));

    // Point 2: seg1 v2, seg2 v3 → seg2 newer → update in seg2, delete from seg1
    assert!(update_2.contains(&2.into()));
    assert!(!update_1.contains(&2.into()));
    assert!(delete_1.contains(&2.into()));

    // Point 3: seg1 v5, seg2 v4 → seg1 newer → update in seg1, delete from seg2
    assert!(update_1.contains(&3.into()));
    assert!(!update_2.contains(&3.into()));
    assert!(delete_2.contains(&3.into()));

    // Point 4 (v6): only in segment2 → update in seg2
    assert!(update_2.contains(&4.into()));
    assert!(!update_1.contains(&4.into()));

    // Point 5: not in any segment → absent from both maps
    assert!(!update_1.contains(&5.into()));
    assert!(!update_2.contains(&5.into()));
    assert!(!delete_1.contains(&5.into()));
    assert!(!delete_2.contains(&5.into()));

    // Point 6: same version (v7) in both segments → both in to_update, neither in to_delete
    assert!(update_1.contains(&6.into()));
    assert!(update_2.contains(&6.into()));
    assert!(!delete_1.contains(&6.into()));
    assert!(!delete_2.contains(&6.into()));
}

/// Test that deferred points are properly handled by `find_points_to_update_and_delete`.
///
/// The logic keeps the latest non-deferred version and only keeps a deferred
/// version if it is strictly newer than all non-deferred versions.
#[test]
fn test_find_points_to_update_and_delete_with_deferred() {
    use std::collections::HashSet;

    use crate::fixtures::build_segment_with_deferred_1;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let vec4 = segment::data_types::vectors::only_default_vector(&[0.0, 0.0, 0.0, 0.0]);

    // Segment 1 (normal): points 3, 4, 5 at version 10
    let mut segment1 = empty_segment(dir.path());
    segment1
        .upsert_point(10, 3.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(10, 4.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(10, 5.into(), vec4.clone(), &hw_counter)
        .unwrap();

    // Segment 2 (with deferred): points 1-5 at version 6
    //   Points 1, 2, 3: NOT deferred
    //   Points 4, 5: deferred
    let segment2 = build_segment_with_deferred_1(dir.path());

    let mut holder = SegmentHolder::default();
    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    let ids: Vec<PointIdType> = vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()];
    let (to_update, to_delete) = holder.find_points_to_update_and_delete(&ids);

    let update_set = |sid: &SegmentId| -> HashSet<PointIdType> {
        to_update
            .get(sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };
    let delete_set = |sid: &SegmentId| -> HashSet<PointIdType> {
        to_delete
            .get(sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };

    let update_1 = update_set(&sid1);
    let update_2 = update_set(&sid2);
    let delete_1 = delete_set(&sid1);
    let delete_2 = delete_set(&sid2);

    // Point 1 (seg2 v6, not deferred): only in seg2 → update seg2
    assert!(update_2.contains(&1.into()));
    assert!(!update_1.contains(&1.into()));

    // Point 2 (seg2 v6, not deferred): only in seg2 → update seg2
    assert!(update_2.contains(&2.into()));
    assert!(!update_1.contains(&2.into()));

    // Point 3: seg1 v10 (non-deferred) vs seg2 v6 (non-deferred) → seg1 newer → update seg1, delete seg2
    assert!(update_1.contains(&3.into()));
    assert!(!update_2.contains(&3.into()));
    assert!(delete_2.contains(&3.into()));

    // Point 4: seg1 v10 (non-deferred) vs seg2 v6 (deferred)
    // seg1 v10 is latest → update seg1
    // seg2 v6 is deferred → never deleted (optimizer handles it)
    assert!(update_1.contains(&4.into()));
    assert!(!update_2.contains(&4.into()));
    assert!(!delete_2.contains(&4.into()));

    // Point 5: seg1 v10 (non-deferred) vs seg2 v6 (deferred) → same as point 4
    assert!(update_1.contains(&5.into()));
    assert!(!update_2.contains(&5.into()));
    assert!(!delete_2.contains(&5.into()));

    // No deletes at all — seg1 always had the latest, seg2 deferred copies are left alone
    assert!(delete_1.is_empty());
    assert!(!delete_2.contains(&4.into()));
    assert!(!delete_2.contains(&5.into()));
}

/// Test that a deferred-only point (no non-deferred copy anywhere) is kept for update.
#[test]
fn test_find_points_to_update_and_delete_with_deferred_only() {
    use std::collections::HashSet;

    use crate::fixtures::build_segment_with_deferred_1;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // Segment with deferred: points 1-5 at version 6
    //   Points 1, 2, 3: NOT deferred
    //   Points 4, 5: deferred
    let segment = build_segment_with_deferred_1(dir.path());

    let mut holder = SegmentHolder::default();
    let sid = holder.add_new(segment);

    // Query only deferred points — no non-deferred copy exists for these in any segment
    let ids: Vec<PointIdType> = vec![4.into(), 5.into()];
    let (to_update, to_delete) = holder.find_points_to_update_and_delete(&ids);

    let update_set: HashSet<PointIdType> = to_update
        .get(&sid)
        .map(|v| v.iter().cloned().collect())
        .unwrap_or_default();
    let delete_set: HashSet<PointIdType> = to_delete
        .get(&sid)
        .map(|v| v.iter().cloned().collect())
        .unwrap_or_default();

    // Points 4 and 5 are deferred but have no non-deferred copy → should be kept for update
    assert!(update_set.contains(&4.into()));
    assert!(update_set.contains(&5.into()));
    assert!(delete_set.is_empty());
}

/// Test that a deferred point with a strictly higher version than all non-deferred copies
/// is kept for update alongside the latest non-deferred copy.
#[test]
fn test_find_points_to_update_and_delete_with_deferred_winning() {
    use std::collections::HashSet;

    use crate::fixtures::build_segment_with_deferred_1;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let vec4 = segment::data_types::vectors::only_default_vector(&[0.0, 0.0, 0.0, 0.0]);

    // Segment 1 (normal): points 4, 5 at version 3 (lower than deferred v6)
    let mut segment1 = empty_segment(dir.path());
    segment1
        .upsert_point(3, 4.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(3, 5.into(), vec4.clone(), &hw_counter)
        .unwrap();

    // Segment 2 (with deferred): points 1-5 at version 6
    //   Points 4, 5: deferred (version 6 > non-deferred version 3)
    let segment2 = build_segment_with_deferred_1(dir.path());

    let mut holder = SegmentHolder::default();
    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);

    let ids: Vec<PointIdType> = vec![4.into(), 5.into()];
    let (to_update, to_delete) = holder.find_points_to_update_and_delete(&ids);

    let update_set = |sid: &SegmentId| -> HashSet<PointIdType> {
        to_update
            .get(sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };
    let delete_set = |sid: &SegmentId| -> HashSet<PointIdType> {
        to_delete
            .get(sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };

    let update_1 = update_set(&sid1);
    let update_2 = update_set(&sid2);
    let delete_1 = delete_set(&sid1);
    let delete_2 = delete_set(&sid2);

    // Point 4: seg1 v3 (non-deferred) vs seg2 v6 (deferred)
    // seg2 v6 is latest → update seg2
    // seg1 v3 is older non-deferred but best is deferred → keep seg1 (don't discard
    // the non-deferred copy in favor of a deferred one)
    assert!(!update_1.contains(&4.into()));
    assert!(update_2.contains(&4.into()));
    assert!(!delete_1.contains(&4.into()));

    // Point 5: same situation as point 4
    assert!(!update_1.contains(&5.into()));
    assert!(update_2.contains(&5.into()));
    assert!(!delete_1.contains(&5.into()));

    // No deletes at all — deferred copies never deleted, non-deferred kept as safety net
    assert!(delete_2.is_empty());
}

/// Test that stale non-deferred copies are deleted even when a same-version
/// non-deferred copy is discovered after a deferred copy at the latest version.
///
/// Scenario (3 segments, point P):
///   seg1: v5 non-deferred (older, should be deleted)
///   seg2: v10 deferred (latest version)
///   seg3: v10 non-deferred (latest version, makes it safe to delete seg1)
///
/// This is order-independent: regardless of which segment is visited first,
/// seg1's stale copy must be deleted.
#[test]
fn test_find_points_to_update_and_delete_with_deferred_three_segments() {
    use std::collections::HashSet;

    use crate::fixtures::build_segment_with_deferred_1;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let vec4 = segment::data_types::vectors::only_default_vector(&[0.0, 0.0, 0.0, 0.0]);

    // Segment 1 (normal): point 4 at version 5 (older non-deferred)
    let mut segment1 = empty_segment(dir.path());
    segment1
        .upsert_point(5, 4.into(), vec4.clone(), &hw_counter)
        .unwrap();

    // Segment 2 (with deferred): point 4 at version 6 (deferred)
    let segment2 = build_segment_with_deferred_1(dir.path());

    // Segment 3 (normal): point 4 at version 6 (non-deferred, same as deferred)
    let mut segment3 = empty_segment(dir.path());
    segment3
        .upsert_point(6, 4.into(), vec4.clone(), &hw_counter)
        .unwrap();

    let mut holder = SegmentHolder::default();
    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);
    let sid3 = holder.add_new(segment3);

    let ids: Vec<PointIdType> = vec![4.into()];
    let (to_update, to_delete) = holder.find_points_to_update_and_delete(&ids);

    let update_set = |sid: &SegmentId| -> HashSet<PointIdType> {
        to_update
            .get(sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };
    let delete_set = |sid: &SegmentId| -> HashSet<PointIdType> {
        to_delete
            .get(sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };

    // Latest version (v6) in seg2 (deferred) and seg3 (non-deferred) → both updated
    assert!(!update_set(&sid1).contains(&4.into()));
    assert!(update_set(&sid2).contains(&4.into()));
    assert!(update_set(&sid3).contains(&4.into()));

    // Seg1 v5 non-deferred is older and latest has a non-deferred copy (seg3) → deleted
    assert!(delete_set(&sid1).contains(&4.into()));
    // Deferred copies are never deleted
    assert!(!delete_set(&sid2).contains(&4.into()));
    assert!(!delete_set(&sid3).contains(&4.into()));
}

/// Test deduplication with deferred points.
///
/// Exercises all deduplication rules:
/// - Non-deferred duplicate with lower version is removed when latest has non-deferred
/// - Deferred points are never removed
/// - Older non-deferred copies are kept when the latest version is deferred-only
/// - Multiple deferred copies are reduced to one
#[test]
fn test_points_deduplication_with_deferred() {
    use crate::fixtures::empty_segment_with_deferred;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let vec4 = segment::data_types::vectors::only_default_vector(&[0.0; 4]);

    // Segment 1 (normal, no deferred):
    //   Point 3 at v10, Point 4 at v10, Point 5 at v3, Point 6 at v8
    let mut segment1 = empty_segment(dir.path());
    segment1
        .upsert_point(10, 3.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(10, 4.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(3, 5.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(8, 6.into(), vec4.clone(), &hw_counter)
        .unwrap();

    // Segment 2 (with deferred, internal IDs >= 3 are deferred):
    //   Insert order: point 1, 2, 3 (non-deferred), then 4, 5, 6 (deferred)
    //   All at version 6
    let mut segment2 = empty_segment_with_deferred(dir.path(), 3);
    for id in 1..=6u64 {
        segment2
            .upsert_point(6, id.into(), vec4.clone(), &hw_counter)
            .unwrap();
    }
    // Verify deferred status
    assert!(!segment2.point_is_deferred(1.into()));
    assert!(!segment2.point_is_deferred(2.into()));
    assert!(!segment2.point_is_deferred(3.into()));
    assert!(segment2.point_is_deferred(4.into()));
    assert!(segment2.point_is_deferred(5.into()));
    assert!(segment2.point_is_deferred(6.into()));

    // Segment 3 (also with deferred, internal IDs >= 3) — to test multiple deferred copies:
    //   To get point 6 deferred, we need 3 dummy points first to push internal IDs up.
    let mut segment3 = empty_segment_with_deferred(dir.path(), 3);
    // Insert dummy points to push internal IDs up
    segment3
        .upsert_point(1, 100.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment3
        .upsert_point(1, 101.into(), vec4.clone(), &hw_counter)
        .unwrap();
    segment3
        .upsert_point(1, 102.into(), vec4.clone(), &hw_counter)
        .unwrap();
    // Now internal_id 3 → deferred
    segment3
        .upsert_point(9, 6.into(), vec4.clone(), &hw_counter)
        .unwrap();
    assert!(segment3.point_is_deferred(6.into()));

    let mut holder = SegmentHolder::default();
    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);
    let sid3 = holder.add_new(segment3);

    let duplicates = holder.find_duplicated_points();

    let removed_from = |sid: SegmentId| -> HashSet<PointIdType> {
        duplicates
            .get(&sid)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    };

    let rem1 = removed_from(sid1);
    let rem2 = removed_from(sid2);
    let rem3 = removed_from(sid3);

    // Point 1: only in seg2 (non-deferred, v6) → no dedup
    assert!(!rem2.contains(&1.into()));

    // Point 2: only in seg2 (non-deferred, v6) → no dedup
    assert!(!rem2.contains(&2.into()));

    // Point 3: seg1 v10 (non-deferred) vs seg2 v6 (non-deferred)
    //   Latest v10 has non-deferred → remove older non-deferred from seg2
    assert!(!rem1.contains(&3.into()));
    assert!(rem2.contains(&3.into()));

    // Point 4: seg1 v10 (non-deferred) vs seg2 v6 (deferred)
    //   Latest v10 has non-deferred (seg1) → seg1 kept as winner
    //   Seg2 is deferred → never removed
    assert!(!rem1.contains(&4.into()));
    assert!(!rem2.contains(&4.into()));

    // Point 5: seg1 v3 (non-deferred) vs seg2 v6 (deferred)
    //   Latest v6 is deferred-only → keep older non-deferred (seg1)
    //   Seg2 is deferred → never removed
    assert!(!rem1.contains(&5.into()));
    assert!(!rem2.contains(&5.into()));

    // Point 6: seg1 v8 (non-deferred) vs seg2 v6 (deferred) vs seg3 v9 (deferred)
    //   Latest v9 is deferred-only → keep older non-deferred (seg1)
    //   Two deferred copies (seg2, seg3) → keep one, remove the other
    assert!(!rem1.contains(&6.into()));
    let deferred_6_removed =
        usize::from(rem2.contains(&6.into())) + usize::from(rem3.contains(&6.into()));
    assert_eq!(
        deferred_6_removed, 1,
        "exactly one of the two deferred copies of point 6 should be removed"
    );

    // Now run actual dedup and verify it completes
    let removed_count = deduplicate_points_sync(&holder).unwrap();
    // Point 3 from seg2 + 1 deferred copy of point 6 = 2
    assert_eq!(removed_count, 2);

    // After dedup: point 3 should only be in seg1
    assert!(
        holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(3.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        !holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(3.into(), common::types::DeferredBehavior::WithDeferred)
    );

    // Points 4 and 5 should still be in both seg1 and seg2
    assert!(
        holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(4.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(4.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        holder
            .get(sid1)
            .unwrap()
            .get()
            .read()
            .has_point(5.into(), common::types::DeferredBehavior::WithDeferred)
    );
    assert!(
        holder
            .get(sid2)
            .unwrap()
            .get()
            .read()
            .has_point(5.into(), common::types::DeferredBehavior::WithDeferred)
    );
}

/// Randomized test for deduplication with a mix of normal and deferred segments.
///
/// Verifies invariants after deduplication:
/// - Each point has at most one deferred copy remaining
/// - If a non-deferred copy was removed, a non-deferred copy with >= version remains
/// - Deferred copies are never removed (only extra deferred duplicates are)
/// - If the latest version is deferred-only, older non-deferred copies are preserved
#[test]
fn test_points_deduplication_with_deferred_randomized() {
    use crate::fixtures::empty_segment_with_deferred;

    const POINT_COUNT: u64 = 200;
    const SEGMENT_COUNT: usize = 5;

    let mut rng = rand::rng();
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let vec4 = segment::data_types::vectors::only_default_vector(&[0.0; 4]);
    let hw_counter = HardwareCounterCell::new();

    // Create segments: first 3 normal, last 2 with deferred points.
    // Deferred internal ID = 3 → internal IDs >= 3 are deferred.
    let segments: Vec<_> = (0..SEGMENT_COUNT)
        .map(|i| {
            if i < 3 {
                empty_segment(dir.path())
            } else {
                empty_segment_with_deferred(dir.path(), 3)
            }
        })
        .collect();

    // Track what we inserted: point_id → [(segment_idx, version)]
    let mut point_versions: HashMap<u64, Vec<(usize, u64)>> = HashMap::new();

    // Wrap in a mutable vec to insert points
    let mut segments = segments;
    for id in 0..POINT_COUNT {
        let point_id = PointIdType::from(id);
        // Insert into a random subset of segments (at least 1, to ensure the point exists)
        let insert_count = rng.random_range(1..=SEGMENT_COUNT);
        let mut segment_indices: Vec<usize> = (0..SEGMENT_COUNT).collect();
        // Shuffle and take first insert_count
        for i in (1..segment_indices.len()).rev() {
            let j = rng.random_range(0..=i);
            segment_indices.swap(i, j);
        }
        segment_indices.truncate(insert_count);

        for &seg_idx in &segment_indices {
            let version = rng.random_range(1..20u64);
            segments[seg_idx]
                .upsert_point(version, point_id, vec4.clone(), &hw_counter)
                .unwrap();
            point_versions
                .entry(id)
                .or_default()
                .push((seg_idx, version));
        }
    }

    // Record deferred status before putting into holder
    let mut is_deferred: HashMap<(u64, usize), bool> = HashMap::new();
    for id in 0..POINT_COUNT {
        let point_id = PointIdType::from(id);
        for (seg_idx, segment) in segments.iter().enumerate() {
            if segment.has_point(point_id, common::types::DeferredBehavior::WithDeferred) {
                is_deferred.insert((id, seg_idx), segment.point_is_deferred(point_id));
            }
        }
    }

    // Put segments into holder
    let mut holder = SegmentHolder::default();
    let segment_ids: Vec<_> = segments
        .into_iter()
        .map(|segment| holder.add_new(segment))
        .collect();

    // Record versions before dedup
    let mut versions_before: HashMap<(u64, usize), Option<u64>> = HashMap::new();
    for id in 0..POINT_COUNT {
        let point_id = PointIdType::from(id);
        for (seg_idx, &sid) in segment_ids.iter().enumerate() {
            let seg = holder.get(sid).unwrap().get();
            let seg = seg.read();
            versions_before.insert((id, seg_idx), seg.point_version(point_id));
        }
    }

    // Run dedup
    let duplicates = holder.find_duplicated_points();

    // Collect what will be removed: (point_id, segment_idx)
    let mut to_remove: HashSet<(u64, usize)> = HashSet::new();
    for (&sid, point_ids) in &duplicates {
        let seg_idx = segment_ids.iter().position(|&s| s == sid).unwrap();
        for &point_id in point_ids {
            if let PointIdType::NumId(num) = point_id {
                to_remove.insert((num, seg_idx));
            }
        }
    }

    // Verify invariants
    for id in 0..POINT_COUNT {
        // Collect all copies of this point
        let copies: Vec<(usize, u64, bool)> = (0..SEGMENT_COUNT)
            .filter_map(|seg_idx| {
                versions_before
                    .get(&(id, seg_idx))
                    .and_then(|v| *v)
                    .map(|version| {
                        let deferred = *is_deferred.get(&(id, seg_idx)).unwrap_or(&false);
                        (seg_idx, version, deferred)
                    })
            })
            .collect();

        if copies.len() <= 1 {
            continue; // No duplicates possible
        }

        let max_version = copies.iter().map(|c| c.1).max().unwrap();
        let latest_has_non_deferred = copies.iter().any(|c| c.1 == max_version && !c.2);

        // Copies remaining after dedup
        let remaining: Vec<_> = copies
            .iter()
            .filter(|(seg_idx, _, _)| !to_remove.contains(&(id, *seg_idx)))
            .collect();

        // Invariant 1: At most one deferred copy remains
        let remaining_deferred = remaining.iter().filter(|c| c.2).count();
        assert!(
            remaining_deferred <= 1,
            "Point {id}: {remaining_deferred} deferred copies remain, expected at most 1"
        );

        // Invariant 2: Deferred copies in the removal set must be extra duplicates
        // (there must be another deferred copy remaining)
        for &(seg_idx, _, deferred) in &copies {
            if deferred && to_remove.contains(&(id, seg_idx)) {
                assert!(
                    remaining_deferred == 1,
                    "Point {id}: deferred copy removed from seg {seg_idx} but no deferred copy remains"
                );
            }
        }

        // Invariant 3: If latest has non-deferred, exactly one non-deferred copy remains
        if latest_has_non_deferred {
            let remaining_non_deferred: Vec<_> = remaining.iter().filter(|c| !c.2).collect();
            assert_eq!(
                remaining_non_deferred.len(),
                1,
                "Point {id}: expected exactly 1 non-deferred copy when latest has non-deferred, got {}",
                remaining_non_deferred.len()
            );
            // And it should have the latest version
            assert_eq!(
                remaining_non_deferred[0].1, max_version,
                "Point {id}: remaining non-deferred copy should have latest version"
            );
        }

        // Invariant 4: If latest is deferred-only, all non-deferred copies should remain
        if !latest_has_non_deferred {
            let original_non_deferred = copies.iter().filter(|c| !c.2).count();
            let remaining_non_deferred = remaining.iter().filter(|c| !c.2).count();
            assert_eq!(
                remaining_non_deferred, original_non_deferred,
                "Point {id}: when latest is deferred-only, all non-deferred copies should be preserved"
            );
        }

        // Invariant 5: At least one copy remains
        assert!(
            !remaining.is_empty(),
            "Point {id}: all copies were removed!"
        );
    }

    // Actually apply the dedup and verify it succeeds
    let removed_count = deduplicate_points_sync(&holder).unwrap();
    assert_eq!(
        removed_count,
        to_remove.len(),
        "deduplicate_points_sync removed a different count than find_duplicated_points reported"
    );
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

    let holder = LockedSegmentHolder::new(holder);

    let before_segment_ids = holder
        .read()
        .iter()
        .map(|(id, _)| id)
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
            None,
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
        SegmentHolder::proxy_all_segments(
            inner_segments_lock,
            segments_dir.path(),
            None,
            schema,
            None,
        )
        .unwrap();

    let mut has_point = false;
    for (_proxy_id, proxy) in &outer_proxies {
        let proxy_read = proxy.get().read();

        if proxy_read.has_point(2.into(), common::types::DeferredBehavior::WithDeferred) {
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
    SegmentHolder::unproxy_all_segments(
        outer_segments_lock,
        outer_proxies,
        outer_tmp_segment,
        holder.acquire_updates_lock(),
    )
    .unwrap();

    // Unproxy twice
    SegmentHolder::unproxy_all_segments(
        holder.upgradable_read(),
        inner_proxies,
        inner_tmp_segment,
        holder.acquire_updates_lock(),
    )
    .unwrap();

    let after_segment_ids = holder
        .read()
        .iter()
        .map(|(id, _)| id)
        .collect::<HashSet<_>>();

    // Check that we have one new segment
    let diff: HashSet<_> = after_segment_ids.difference(&before_segment_ids).collect();
    assert_eq!(
        diff.len(),
        0,
        "There should be no new segment after unproxying"
    );

    let has_point_1 = locked_segment1
        .get()
        .read()
        .has_point(1.into(), common::types::DeferredBehavior::WithDeferred); // Deleted in inner proxy
    let has_point_2 = locked_segment1
        .get()
        .read()
        .has_point(2.into(), common::types::DeferredBehavior::WithDeferred); // Deleted in outer proxy
    let has_point_3 = locked_segment1
        .get()
        .read()
        .has_point(3.into(), common::types::DeferredBehavior::WithDeferred); // Not deleted

    assert!(!has_point_1, "Point 1 should be deleted");
    assert!(!has_point_2, "Point 2 should be deleted");
    assert!(has_point_3, "Point 3 should be present");
}

/// Test that CoW skips deleting the source point when the destination copy is deferred.
///
/// When a point is moved from a non-appendable segment to an appendable segment with a
/// deferred threshold, the point may become deferred in the destination (vectors not yet
/// fully materialized). In that case, the source must NOT be deleted to avoid data loss.
#[test]
fn test_cow_skips_delete_when_destination_is_deferred() {
    use crate::fixtures::build_segment_with_deferred_1;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let vec4 = segment::data_types::vectors::only_default_vector(&[0.0, 0.0, 0.0, 0.0]);

    // Appendable segment with deferred threshold: points 1-5, deferred_internal_id = 3
    // Any new point gets internal_id >= 5, which is >= 3, so it will be deferred.
    let appendable = build_segment_with_deferred_1(dir.path());

    // Non-appendable segment with point 100 at version 10
    let mut non_appendable = empty_segment(dir.path());
    non_appendable
        .upsert_point(10, 100.into(), vec4.clone(), &hw_counter)
        .unwrap();
    non_appendable.appendable_flag = false;

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    holder
        .apply_points_with_conditional_move(
            20,
            &[100.into()],
            |_, _| unreachable!("point is in non-appendable, should take CoW path"),
            |_, _, _, _| {},
            &hw_counter,
        )
        .unwrap();

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    // Point 100 was upserted into the appendable segment and became deferred
    assert!(
        app.point_is_deferred(100.into()),
        "Point 100 should be deferred in the destination"
    );
    assert!(
        app.point_version(100.into()).is_some(),
        "Point 100 should exist in the destination"
    );

    // Source should NOT be deleted because the destination copy is deferred
    assert!(
        non_app.point_version(100.into()).is_some(),
        "Point 100 should still exist in the source (delete skipped for deferred destination)"
    );
}

/// A post-flush action that does not complete keeps its ack pin in effect across flushes,
/// capping the returned WAL acknowledge until it finally completes.
#[test]
fn test_post_flush_action_retry_keeps_ack_pin() {
    use std::sync::atomic::AtomicUsize;

    const ACK_PIN: SeqNumberType = 5;
    const WATERLINE: SeqNumberType = 100;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut holder = SegmentHolder::default();
    holder.add_new(empty_segment(dir.path()));
    // Make the durable waterline observably higher than the ack pin so the cap is visible.
    holder.bump_max_segment_version_overwrite(WATERLINE);

    // An action that must run three times: Retry, Retry, then Done.
    let runs = Arc::new(AtomicUsize::new(0));
    let runs_in_action = Arc::clone(&runs);
    holder.register_post_flush_action(0, ACK_PIN, move || {
        let previous = runs_in_action.fetch_add(1, Ordering::SeqCst);
        if previous < 2 {
            Ok(PostFlushOutcome::Retry)
        } else {
            Ok(PostFlushOutcome::Done)
        }
    });

    // While the action is pending, the ack pin caps the returned version.
    let version = holder.flush_all(FlushMode::Sync, true).unwrap();
    assert_eq!(version, ACK_PIN, "ack pin must cap the WAL acknowledge");
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Still pending after the second retry: the pin remains in effect.
    let version = holder.flush_all(FlushMode::Sync, true).unwrap();
    assert_eq!(version, ACK_PIN);
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    // The third flush completes the action, lifting the cap.
    let version = holder.flush_all(FlushMode::Sync, true).unwrap();
    assert_eq!(
        version, WATERLINE,
        "cap is lifted once the action completes"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 3);

    // The completed action is gone and is not run again.
    let version = holder.flush_all(FlushMode::Sync, true).unwrap();
    assert_eq!(version, WATERLINE);
    assert_eq!(runs.load(Ordering::SeqCst), 3);
}

/// While an action is being run it is briefly removed from the queue, but its ack pin must stay
/// visible to `pending_post_flush_ack_cap` so a concurrent flush cannot over-acknowledge the WAL.
#[test]
fn test_post_flush_action_in_flight_pin_stays_visible() {
    const ACK_PIN: SeqNumberType = 5;
    const WATERLINE: SeqNumberType = 100;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut holder = SegmentHolder::default();
    holder.add_new(empty_segment(dir.path()));
    holder.bump_max_segment_version_overwrite(WATERLINE);
    let holder = Arc::new(holder);

    // From inside the action (when it is out of the queue) observe the cap a concurrent flush
    // would see.
    let observed = Arc::new(Mutex::new(None));
    let observed_in_action = Arc::clone(&observed);
    let holder_in_action = Arc::clone(&holder);
    holder.register_post_flush_action(0, ACK_PIN, move || {
        *observed_in_action.lock() = Some(holder_in_action.pending_post_flush_ack_cap());
        Ok(PostFlushOutcome::Done)
    });

    let version = holder.flush_all(FlushMode::Sync, true).unwrap();

    assert_eq!(
        *observed.lock(),
        Some(Some(ACK_PIN)),
        "in-flight action's ack pin must remain visible while it runs",
    );
    // Once it completed, the cap is lifted.
    assert_eq!(version, WATERLINE);
    assert_eq!(holder.pending_post_flush_ack_cap(), None);
}

/// A post-flush action that fails hard (not retryable) is dropped and surfaces its error; a
/// later flush is then unaffected.
#[test]
fn test_post_flush_action_hard_failure_is_dropped() {
    const WATERLINE: SeqNumberType = 100;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut holder = SegmentHolder::default();
    holder.add_new(empty_segment(dir.path()));
    holder.bump_max_segment_version_overwrite(WATERLINE);

    holder.register_post_flush_action(0, 5, move || Err(OperationError::service_error("boom")));

    // The flush surfaces the action's error.
    let err = holder.flush_all(FlushMode::Sync, true).unwrap_err();
    assert!(format!("{err}").contains("boom"));

    // The failed action is gone, so the next flush is clean and uncapped.
    let version = holder.flush_all(FlushMode::Sync, true).unwrap();
    assert_eq!(version, WATERLINE);
}

/// Test that CoW deletes the source point when the destination copy is NOT deferred.
///
/// This is the standard CoW behavior: after successfully moving a point to the appendable
/// segment, the old copy in the non-appendable segment is deleted.
#[test]
fn test_cow_deletes_source_when_destination_is_not_deferred() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let vec4 = segment::data_types::vectors::only_default_vector(&[0.0, 0.0, 0.0, 0.0]);

    // Regular appendable segment (no deferred threshold)
    let appendable = empty_segment(dir.path());

    // Non-appendable segment with point 100 at version 10
    let mut non_appendable = empty_segment(dir.path());
    non_appendable
        .upsert_point(10, 100.into(), vec4.clone(), &hw_counter)
        .unwrap();
    non_appendable.appendable_flag = false;

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    holder
        .apply_points_with_conditional_move(
            20,
            &[100.into()],
            |_, _| unreachable!("point is in non-appendable, should take CoW path"),
            |_, _, _, _| {},
            &hw_counter,
        )
        .unwrap();

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    // Point 100 should exist in the appendable segment, not deferred
    assert!(
        app.has_point(100.into(), common::types::DeferredBehavior::WithDeferred),
        "Point 100 should exist in the destination"
    );

    // Source should be deleted (standard CoW behavior)
    assert!(
        non_app.point_version(100.into()).is_none(),
        "Point 100 should be deleted from the source"
    );
}
