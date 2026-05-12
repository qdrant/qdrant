use std::borrow::Borrow;
use std::collections::HashSet;
use std::hint::black_box;
use std::path::Path;

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use ecow::EcoString;
use gridstore::Blob;
use rstest::rstest;
use serde_json::Value;
use tempfile::Builder;

use super::MapIndex;
use super::key::MapIndexKey;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadFieldIndex, PayloadFieldIndexRead,
    ValueIndexer,
};
use crate::types::{IntPayloadType, PayloadKeyType, UuidIntType};

/// Generous default size for the deleted-points bitslice used in tests.
///
/// Must be larger than the stored mmap deletion bitslice for any test in
/// this file (which is sized to the highest point id, rounded up to a
/// `usize` boundary). 4096 bits comfortably covers all current tests.
const TEST_DELETED_BITS: usize = 4096;

/// All-zero deletion bitslice for tests that don't care about deletions.
fn empty_deleted() -> BitVec {
    BitVec::repeat(false, TEST_DELETED_BITS)
}

/// Deletion bitslice with specific points marked as deleted.
fn deleted_with(points: &[PointOffsetType]) -> BitVec {
    let mut v = empty_deleted();
    for &p in points {
        v.set(p as usize, true);
    }
    v
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum IndexType {
    MutableGridstore,
    Mmap,
    RamMmap,
}

fn save_map_index<N>(
    data: &[Vec<<N as MapIndexKey>::Owned>],
    path: &Path,
    index_type: IndexType,
    into_value: impl Fn(&<N as MapIndexKey>::Owned) -> Value,
) where
    N: MapIndexKey + ?Sized,
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    <MapIndex<N> as ValueIndexer>::ValueType: Into<<N as MapIndexKey>::Owned>,
{
    let hw_counter = HardwareCounterCell::new();

    match index_type {
        IndexType::MutableGridstore => {
            let mut builder = MapIndex::<N>::builder_gridstore(path.to_path_buf());
            builder.init().unwrap();
            for (idx, values) in data.iter().enumerate() {
                let values: Vec<Value> = values.iter().map(&into_value).collect();
                let values: Vec<_> = values.iter().collect();
                builder
                    .add_point(idx as PointOffsetType, &values, &hw_counter)
                    .unwrap();
            }
            builder.finalize().unwrap();
        }
        IndexType::Mmap | IndexType::RamMmap => {
            let mut builder = MapIndex::<N>::builder_mmap(path, false, &empty_deleted());
            builder.init().unwrap();
            for (idx, values) in data.iter().enumerate() {
                let values: Vec<Value> = values.iter().map(&into_value).collect();
                let values: Vec<_> = values.iter().collect();
                builder
                    .add_point(idx as PointOffsetType, &values, &hw_counter)
                    .unwrap();
            }
            builder.finalize().unwrap();
        }
    }
}

fn load_map_index<N: MapIndexKey + ?Sized>(
    data: &[Vec<<N as MapIndexKey>::Owned>],
    path: &Path,
    index_type: IndexType,
) -> MapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    let index = match index_type {
        IndexType::MutableGridstore => MapIndex::<N>::new_gridstore(path.to_path_buf(), true)
            .unwrap()
            .unwrap(),
        IndexType::Mmap => MapIndex::<N>::new_mmap(path, true, &empty_deleted())
            .unwrap()
            .unwrap(),
        IndexType::RamMmap => MapIndex::<N>::new_mmap(path, false, &empty_deleted())
            .unwrap()
            .unwrap(),
    };
    let hw_counter = HardwareCounterCell::new();
    for (idx, values) in data.iter().enumerate() {
        let index_values: HashSet<<N as MapIndexKey>::Owned> = index
            .get_values(idx as PointOffsetType, &hw_counter)
            .unwrap()
            .map(|v| MapIndexKey::to_owned(v.as_ref()))
            .collect();
        let index_values: HashSet<&N> = index_values.iter().map(|v| v.borrow()).collect();
        let check_values: HashSet<&N> = values.iter().map(|v| v.borrow()).collect();
        assert_eq!(index_values, check_values);
    }

    index
}

#[test]
fn test_uuid_payload_index() {
    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    let mut builder =
        MapIndex::<UuidIntType>::builder_mmap(temp_dir.path(), false, &empty_deleted());

    builder.init().unwrap();

    let hw_counter = HardwareCounterCell::new();

    let uuid: Value = Value::String("baa56dfc-e746-4ec1-bf50-94822535a46c".to_string());

    for idx in 0..100 {
        builder
            .add_point(idx as PointOffsetType, &[&uuid], &hw_counter)
            .unwrap();
    }

    let index = builder.finalize().unwrap();

    index
        .for_each_payload_block(50, PayloadKeyType::new("test_uuid"), &mut |block| {
            black_box(block);
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_index_non_ascending_insertion() {
    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    let mut builder =
        MapIndex::<IntPayloadType>::builder_mmap(temp_dir.path(), false, &empty_deleted());
    builder.init().unwrap();

    let data = [vec![1, 2, 3, 4, 5, 6], vec![25], vec![10, 11]];

    let hw_counter = HardwareCounterCell::new();

    for (idx, values) in data.iter().enumerate().rev() {
        let values: Vec<Value> = values.iter().map(|i| (*i).into()).collect();
        let values: Vec<_> = values.iter().collect();
        builder
            .add_point(idx as PointOffsetType, &values, &hw_counter)
            .unwrap();
    }

    let index = builder.finalize().unwrap();
    let hw_counter = HardwareCounterCell::new();
    for (idx, values) in data.iter().enumerate().rev() {
        let res: Vec<_> = index
            .get_values(idx as u32, &hw_counter)
            .unwrap()
            .map(|i| *i as i32)
            .collect();
        assert_eq!(res, *values);
    }
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_int_disk_map_index(#[case] index_type: IndexType) {
    let data = vec![
        vec![1, 2, 3, 4, 5, 6],
        vec![1, 2, 3, 4, 5, 6],
        vec![13, 14, 15, 16, 17, 18],
        vec![19, 20, 21, 22, 23, 24],
        vec![25],
    ];

    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());
    let index = load_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type);

    let hw_counter = HardwareCounterCell::new();

    assert!(
        !index
            .except_cardinality(std::iter::empty(), &hw_counter)
            .equals_min_exp_max(&CardinalityEstimation::exact(0))
    );
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_string_disk_map_index(#[case] index_type: IndexType) {
    let data = vec![
        vec![
            EcoString::from("AABB"),
            EcoString::from("UUFF"),
            EcoString::from("IIBB"),
        ],
        vec![
            EcoString::from("PPMM"),
            EcoString::from("QQXX"),
            EcoString::from("YYBB"),
        ],
        vec![
            EcoString::from("FFMM"),
            EcoString::from("IICC"),
            EcoString::from("IIBB"),
        ],
        vec![
            EcoString::from("AABB"),
            EcoString::from("UUFF"),
            EcoString::from("IIBB"),
        ],
        vec![EcoString::from("PPGG")],
    ];

    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    save_map_index::<str>(&data, temp_dir.path(), index_type, |v| v.to_string().into());
    let index = load_map_index::<str>(&data, temp_dir.path(), index_type);

    let hw_counter = HardwareCounterCell::new();

    assert!(
        !index
            .except_cardinality(vec![].into_iter(), &hw_counter)
            .equals_min_exp_max(&CardinalityEstimation::exact(0))
    );
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_empty_index(#[case] index_type: IndexType) {
    let data: Vec<Vec<EcoString>> = vec![];

    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    save_map_index::<str>(&data, temp_dir.path(), index_type, |v| v.to_string().into());
    let index = load_map_index::<str>(&data, temp_dir.path(), index_type);

    let hw_counter = HardwareCounterCell::new();

    assert!(
        index
            .except_cardinality(std::iter::empty(), &hw_counter)
            .equals_min_exp_max(&CardinalityEstimation::exact(0))
    );
}

/// Test that `get_values` on an on-disk mmap index actually increments the hardware counter.
#[test]
fn test_mmap_get_values_hw_counter() {
    let data = vec![vec![1i64, 2, 3], vec![4, 5], vec![6]];

    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    save_map_index::<IntPayloadType>(&data, temp_dir.path(), IndexType::Mmap, |v| (*v).into());
    let index = load_map_index::<IntPayloadType>(&data, temp_dir.path(), IndexType::Mmap);

    let hw_counter = HardwareCounterCell::new();
    for idx in 0..data.len() {
        let _values: Vec<_> = index
            .get_values(idx as PointOffsetType, &hw_counter)
            .unwrap()
            .collect();
    }

    assert!(
        hw_counter.payload_index_io_read_counter().get() > 0,
        "Expected on-disk mmap get_values to track payload index IO reads, but counter was 0"
    );

    let temp_dir2 = Builder::new().prefix("store_dir").tempdir().unwrap();
    save_map_index::<IntPayloadType>(&data, temp_dir2.path(), IndexType::RamMmap, |v| (*v).into());
    let index2 = load_map_index::<IntPayloadType>(&data, temp_dir2.path(), IndexType::RamMmap);

    let hw_counter2 = HardwareCounterCell::new();
    for idx in 0..data.len() {
        let _values: Vec<_> = index2
            .get_values(idx as PointOffsetType, &hw_counter2)
            .unwrap()
            .collect();
    }

    assert_eq!(
        hw_counter2.payload_index_io_read_counter().get(),
        0,
        "Expected RAM mmap get_values NOT to track IO reads, but counter was non-zero"
    );
}

/// Reload contract: runtime deletions are not persisted by the mmap map
/// index. Callers must re-supply the deletion bitslice on reload.
///
/// Test data is chosen so that every value retains at least one live
/// point after deletions — otherwise `ImmutableMapIndex::open_mmap` hits a
/// pre-existing debug-only assertion when a value's slice becomes empty.
#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_map_index_reload(#[case] index_type: IndexType) {
    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    let data: Vec<Vec<IntPayloadType>> = vec![
        vec![1, 2], // id 0
        vec![1],    // id 1
        vec![2],    // id 2
        vec![1, 3], // id 3
        vec![2, 3], // id 4
        vec![3],    // id 5
    ];

    {
        save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());
        let mut index = load_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type);
        index.remove_point(1).unwrap();
        index.remove_point(2).unwrap();
        index.remove_point(5).unwrap();
        index.flusher()().unwrap();
        assert_eq!(index.get_indexed_points(), 3);
        drop(index);
    }

    let deleted = deleted_with(&[1, 2, 5]);
    let new_index = match index_type {
        IndexType::MutableGridstore => {
            MapIndex::<IntPayloadType>::new_gridstore(temp_dir.path().to_path_buf(), true)
                .unwrap()
                .unwrap()
        }
        IndexType::Mmap => MapIndex::<IntPayloadType>::new_mmap(temp_dir.path(), true, &deleted)
            .unwrap()
            .unwrap(),
        IndexType::RamMmap => {
            MapIndex::<IntPayloadType>::new_mmap(temp_dir.path(), false, &deleted)
                .unwrap()
                .unwrap()
        }
    };

    assert_eq!(new_index.get_indexed_points(), 3);

    let hw_counter = HardwareCounterCell::new();
    for id in [1u32, 2, 5] {
        assert_eq!(
            new_index.values_count(id),
            0,
            "deleted point {id} should have no values after reload",
        );
    }
    for id in [0u32, 3, 4] {
        assert!(
            new_index.values_count(id) > 0,
            "live point {id} should have values after reload",
        );
    }

    let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&1, &hw_counter).collect();
    hits.sort();
    assert_eq!(hits, vec![0, 3]);

    let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&2, &hw_counter).collect();
    hits.sort();
    assert_eq!(hits, vec![0, 4]);

    let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&3, &hw_counter).collect();
    hits.sort();
    assert_eq!(hits, vec![3, 4]);
}

/// Regression test: when reloading an mmap map index with a `deleted_points`
/// bitslice shorter than `point_to_values.len()`, missing entries must
/// default to live, not deleted. Empty-payload bits from the on-disk
/// `deleted.bin` and any deletions encoded inside the short bitslice must
/// still be honored.
#[rstest]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_map_index_reload_short_deleted_bitslice(#[case] index_type: IndexType) {
    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();

    let data: Vec<Vec<IntPayloadType>> = vec![
        vec![1],    // id 0
        vec![1, 2], // id 1
        vec![],     // id 2 — empty payload
        vec![2, 3], // id 3
        vec![3],    // id 4
    ];

    save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());

    let mut short_deleted = BitVec::repeat(false, 2);
    short_deleted.set(1, true);

    let new_index = match index_type {
        IndexType::Mmap => {
            MapIndex::<IntPayloadType>::new_mmap(temp_dir.path(), true, &short_deleted)
                .unwrap()
                .unwrap()
        }
        IndexType::RamMmap => {
            MapIndex::<IntPayloadType>::new_mmap(temp_dir.path(), false, &short_deleted)
                .unwrap()
                .unwrap()
        }
        IndexType::MutableGridstore => unreachable!(),
    };

    let hw_counter = HardwareCounterCell::new();

    assert!(new_index.values_count(0) > 0, "id 0 should be live");
    assert_eq!(new_index.values_count(1), 0, "id 1 deleted via bitslice");
    assert_eq!(
        new_index.values_count(2),
        0,
        "id 2 deleted via build-time empty"
    );
    assert!(
        new_index.values_count(3) > 0,
        "id 3 should be live (beyond bitslice)"
    );
    assert!(
        new_index.values_count(4) > 0,
        "id 4 should be live (beyond bitslice)"
    );

    let mut hits: Vec<PointOffsetType> = new_index.get_iterator(&2, &hw_counter).collect();
    hits.sort();
    assert_eq!(hits, vec![3]);
}
