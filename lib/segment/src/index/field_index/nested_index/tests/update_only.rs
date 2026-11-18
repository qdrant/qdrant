//! Per-element indexes written by an update-only segment must read back like the indexes
//! a writable segment builds from the same points.

use common::bitvec::BitVec;
use common::universal_io::{MmapFile, MmapFs};

use super::*;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::struct_payload_index::update_only::UpdateOnlyStructPayloadIndex;
use crate::segment_constructor::get_payload_index_path;

/// Every read of `index` and `reference` agrees, for points `0..total`.
fn assert_reads_equal(
    reference: &FieldIndex,
    index: &ReadOnlyFieldIndex<MmapFile>,
    field: &JsonPath,
    total: PointOffsetType,
) {
    assert_eq!(
        reference.get_full_index_type().index_type,
        index.get_full_index_type().index_type,
    );
    let index_type = &reference.get_full_index_type().index_type;
    assert_eq!(
        reference.count_indexed_points().unwrap(),
        index.count_indexed_points().unwrap(),
        "{field} {index_type:?}: indexed points",
    );
    for point in 0..total {
        assert_eq!(
            reference.values_count(point).unwrap(),
            index.values_count(point).unwrap(),
            "{field} {index_type:?}: values of point {point}",
        );
        assert_eq!(
            reference.values_is_empty(point).unwrap(),
            index.values_is_empty(point).unwrap(),
            "{field} {index_type:?}: emptiness of point {point}",
        );
    }

    let conditions = field_conditions()
        .into_iter()
        .filter(|condition| &condition.key == field)
        .chain([
            FieldCondition::new_is_empty(field.clone(), true),
            FieldCondition::new_is_null(field.clone(), true),
        ]);
    for condition in conditions {
        let sorted = |points: Option<Box<dyn Iterator<Item = PointOffsetType> + '_>>| {
            points.map(|points| {
                let mut points: Vec<_> = points.collect();
                points.sort_unstable();
                points
            })
        };
        assert_eq!(
            sorted(reference.filter(&condition).unwrap()),
            sorted(index.filter(&condition).unwrap()),
            "{field} {index_type:?}: filter {condition:?}",
        );

        let reference_checker = reference.condition_checker(&condition).unwrap();
        let checker = index.condition_checker(&condition).unwrap();
        assert_eq!(reference_checker.is_some(), checker.is_some());
        if let (Some(reference_checker), Some(checker)) = (reference_checker, checker) {
            for point in 0..total {
                assert_eq!(
                    reference_checker.check(point).unwrap(),
                    checker.check(point).unwrap(),
                    "{field} {index_type:?}: checker {condition:?} of point {point}",
                );
            }
        }
    }
}

/// Points written in several batches by the update-only writer read back like the
/// per-element indexes of a writable segment that got the same points.
#[test]
fn update_only_nested_indexes_match_writable() {
    let dir = Builder::new()
        .prefix("nested_update_only")
        .tempdir()
        .unwrap();
    let mut rng = StdRng::seed_from_u64(3);
    let _scope = ambient::test_guard();

    // Indexes first, so the points get internal offsets in insertion order
    let mut reference =
        build_simple_segment(&dir.path().join("reference"), 2, Distance::Dot).unwrap();
    let mut op_num = 0;
    for &(field, field_type) in FIELDS {
        op_num += 1;
        reference
            .create_field_index(
                op_num,
                &JsonPath::from_str(field).unwrap(),
                Some(&schema(field_type, true, None)),
            )
            .unwrap();
    }
    let payloads: Vec<Payload> = (0..NUM_POINTS).map(|_| random_payload(&mut rng)).collect();
    for (id, payload) in payloads.iter().enumerate() {
        op_num += 1;
        let id = ExtendedPointId::from(id as u64);
        reference
            .upsert_point(op_num, id, only_default_vector(&[1.0, 1.0]))
            .unwrap();
        reference.set_full_payload(op_num, id, payload).unwrap();
    }

    // The same fields and index types, written by update-only batches
    let segment_path = dir.path().join("update_only");
    let index_path = get_payload_index_path(&segment_path);
    fs_err::create_dir_all(&index_path).unwrap();
    let config = reference.payload_index.borrow().config().clone();
    config
        .save(&crate::index::payload_config::PayloadConfig::get_config_path(&index_path))
        .unwrap();
    let batch_size = payloads.len().div_ceil(3);
    // Reopened per batch: the next batch resumes after the offsets the last one gave out
    for (i, batch) in payloads.chunks(batch_size).enumerate() {
        let mut index =
            UpdateOnlyStructPayloadIndex::<MmapFile>::par_open(&MmapFs, &segment_path).unwrap();
        let start_slot = (i * batch_size) as PointOffsetType;
        index.append_many(&MmapFs, start_slot, batch).unwrap();
    }

    let total = NUM_POINTS as PointOffsetType;
    let deleted = BitVec::repeat(false, total as usize);
    let reference_index = reference.payload_index.borrow();
    for (field, indexed) in config.indices.iter() {
        let reference_indexes = &reference_index.field_indexes[field];
        assert_eq!(reference_indexes.len(), indexed.types.len());
        for (reference, index_type) in reference_indexes.iter().zip(&indexed.types) {
            assert!(reference.is_nested());
            let index = ReadOnlyFieldIndex::open(
                &MmapFs,
                &index_path,
                field,
                &indexed.schema,
                index_type,
                total as usize,
                &deleted,
                None,
            )
            .unwrap()
            .unwrap_or_else(|| panic!("{field} {index_type:?} must be written"));
            assert!(matches!(index, ReadOnlyFieldIndex::Nested(_)));
            assert_reads_equal(reference, &index, field, total);
        }
    }
}
