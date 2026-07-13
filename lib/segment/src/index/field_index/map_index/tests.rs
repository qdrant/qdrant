use std::borrow::Borrow;
use std::collections::{BTreeMap, HashSet};
use std::hint::black_box;
use std::path::Path;

use blobstore::Blob;
use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use ecow::EcoString;
use rstest::rstest;
use serde_json::Value;
use tempfile::Builder;

use super::MapIndex;
use super::key::MapIndexKey;
use super::read_ops::MapIndexRead;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadFieldIndex, PayloadFieldIndexRead,
    ValueIndexer,
};
use crate::types::{FieldCondition, IntPayloadType, Memory, PayloadKeyType, UuidIntType};

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
            let mut builder = MapIndex::<N>::builder_mutable(path.to_path_buf(), true);
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
            let mut builder = MapIndex::<N>::builder_immutable(path, false, &empty_deleted(), true);
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
        IndexType::MutableGridstore => MapIndex::<N>::new_mutable(path.to_path_buf(), true, true)
            .unwrap()
            .unwrap(),
        IndexType::Mmap => MapIndex::<N>::new_immutable(path, Memory::Cold, &empty_deleted())
            .unwrap()
            .unwrap(),
        IndexType::RamMmap => MapIndex::<N>::new_immutable(path, Memory::Pinned, &empty_deleted())
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
        MapIndex::<UuidIntType>::builder_immutable(temp_dir.path(), false, &empty_deleted(), false);

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

#[rstest]
#[case(false)]
#[case(true)]
fn test_index_non_ascending_insertion(#[case] on_disk: bool) {
    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    let mut builder = MapIndex::<IntPayloadType>::builder_immutable(
        temp_dir.path(),
        on_disk,
        &empty_deleted(),
        false,
    );
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
    for (idx, values) in data.into_iter().enumerate().rev() {
        // values themselves don't promise any particular order
        // so we only compare the set of values.
        let values = HashSet::from_iter(values);
        let res: HashSet<_> = index
            .get_values(idx as u32, &hw_counter)
            .unwrap()
            .map(|i| *i as i32)
            .collect();
        assert_eq!(res, values);
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
            MapIndex::<IntPayloadType>::new_mutable(temp_dir.path().to_path_buf(), true, false)
                .unwrap()
                .unwrap()
        }
        IndexType::Mmap => {
            MapIndex::<IntPayloadType>::new_immutable(temp_dir.path(), Memory::Cold, &deleted)
                .unwrap()
                .unwrap()
        }
        IndexType::RamMmap => {
            MapIndex::<IntPayloadType>::new_immutable(temp_dir.path(), Memory::Pinned, &deleted)
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
            MapIndex::<IntPayloadType>::new_immutable(temp_dir.path(), Memory::Cold, &short_deleted)
                .unwrap()
                .unwrap()
        }
        IndexType::RamMmap => MapIndex::<IntPayloadType>::new_immutable(
            temp_dir.path(),
            Memory::Pinned,
            &short_deleted,
        )
        .unwrap()
        .unwrap(),
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

/// Run `for_values_map` for the given keys and collect `value -> sorted ids`.
/// Batched reads (the on-disk variant) may invoke the callback out of order, so
/// we sort each posting and key by a `BTreeMap`.
fn collect_for_values_map(
    index: &MapIndex<IntPayloadType>,
    keys: &[IntPayloadType],
) -> BTreeMap<IntPayloadType, Vec<PointOffsetType>> {
    let hw_counter = HardwareCounterCell::new();
    let mut out = BTreeMap::new();
    MapIndexRead::for_values_map(index, keys.iter(), &hw_counter, |key, ids| {
        let mut ids: Vec<PointOffsetType> = ids.collect();
        ids.sort_unstable();
        out.insert(*key, ids);
        Ok(())
    })
    .unwrap();
    out
}

/// The on-disk variant overrides `for_values_map` with a batched read; assert it
/// agrees with the in-memory variants (which use the default per-key impl) and
/// with the directly-computed expectation.
#[test]
fn test_for_values_map_congruence() {
    // Points 0..5; value 3 spans three points, value 6 only one.
    #[rustfmt::skip]
    let data = vec![
        vec![1, 2, 3],
        vec![2, 3, 4],
        vec![3],
        vec![6],
        vec![1, 5],
    ];
    // Present values (multi- and single-point) plus an absent one (99), which
    // must still be reported with an empty posting.
    let query = [1, 2, 3, 4, 5, 99];

    let expected: BTreeMap<IntPayloadType, Vec<PointOffsetType>> = query
        .iter()
        .map(|&q| {
            let ids = data
                .iter()
                .enumerate()
                .filter(|(_, vals)| vals.contains(&q))
                .map(|(i, _)| i as PointOffsetType)
                .collect();
            (q, ids)
        })
        .collect();

    for index_type in [
        IndexType::MutableGridstore,
        IndexType::Mmap,
        IndexType::RamMmap,
    ] {
        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());
        let index = load_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type);

        let result = collect_for_values_map(&index, &query);
        assert_eq!(result, expected, "mismatch for {index_type:?}");
    }
}

/// The batched on-disk `for_values_map` must drop deleted points from postings,
/// mirroring `get_iterator`.
#[test]
fn test_for_values_map_on_disk_deleted() {
    let data = vec![vec![1, 2, 3], vec![2, 3, 4], vec![3], vec![1, 5]];

    let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
    save_map_index::<IntPayloadType>(&data, temp_dir.path(), IndexType::Mmap, |v| (*v).into());

    // Load the on-disk variant with points 0 and 2 marked deleted.
    let deleted = deleted_with(&[0, 2]);
    let index = MapIndex::<IntPayloadType>::new_immutable(temp_dir.path(), Memory::Cold, &deleted)
        .unwrap()
        .unwrap();
    assert!(matches!(index, MapIndex::OnDisk(_)));

    let result = collect_for_values_map(&index, &[1, 2, 3, 4, 5]);

    // Postings with deleted points {0, 2} removed.
    let expected: BTreeMap<IntPayloadType, Vec<PointOffsetType>> = [
        (1, vec![3]),
        (2, vec![1]),
        (3, vec![1]),
        (4, vec![1]),
        (5, vec![3]),
    ]
    .into_iter()
    .collect();
    assert_eq!(result, expected);
}

// --- Prefix matching ---

/// Ground truth for a prefix match: points with at least one value starting
/// with the prefix.
fn naive_prefix_points(data: &[Vec<EcoString>], prefix: &str) -> Vec<PointOffsetType> {
    data.iter()
        .enumerate()
        .filter(|(_, values)| values.iter().any(|value| value.starts_with(prefix)))
        .map(|(idx, _)| idx as PointOffsetType)
        .collect()
}

fn prefix_test_data() -> Vec<Vec<EcoString>> {
    [
        vec!["https://qdrant.tech", "https://qdrant.tech/docs"],
        vec!["https://qdrant.tech"],
        vec!["https://example.com"],
        vec!["http://example.com", "tag"],
        vec!["tags"],
        vec!["αβγ", "αβδ"],
        vec!["tag"],
    ]
    .into_iter()
    .map(|values| values.into_iter().map(EcoString::from).collect())
    .collect()
}

const PREFIX_PROBES: &[&str] = &[
    "",
    "h",
    "http",
    "http://",
    "https://",
    "https://qdrant.",
    "https://qdrant.tech",
    "https://qdrant.tech/docs/deep",
    "tag",
    "tags",
    "tagz",
    "α",
    "αβ",
    "αβγ",
    "nonexistent",
];

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_str_prefix_match(#[case] index_type: IndexType) {
    use common::condition_checker::ConditionChecker as _;
    use common::counter::hardware_accumulator::HwMeasurementAcc;

    use crate::json_path::JsonPath;
    use crate::types::Match;

    let temp_dir = Builder::new().prefix("prefix_index_dir").tempdir().unwrap();
    let data = prefix_test_data();
    let hw_counter = HardwareCounterCell::new();

    save_map_index::<str>(&data, temp_dir.path(), index_type, |v| v.to_string().into());
    let index: MapIndex<str> = load_map_index(&data, temp_dir.path(), index_type);

    for prefix in PREFIX_PROBES {
        let expected = naive_prefix_points(&data, prefix);
        let condition = FieldCondition::new_match(JsonPath::new("test"), Match::new_prefix(prefix));

        // Filter iterator parity with the naive scan.
        let mut result: Vec<PointOffsetType> = index
            .filter(&condition, &hw_counter)
            .unwrap()
            .unwrap_or_else(|| panic!("prefix {prefix:?} must be served by the index"))
            .collect();
        result.sort_unstable();
        assert_eq!(result, expected, "prefix {prefix:?}");

        // Cardinality bounds contain the true count (no deletions here, so
        // even on-disk counts are accurate).
        let estimation = index
            .estimate_cardinality(&condition, &hw_counter)
            .unwrap()
            .unwrap_or_else(|| panic!("prefix {prefix:?} must be estimated by the index"));
        assert!(
            estimation.min <= expected.len() && expected.len() <= estimation.max,
            "prefix {prefix:?}: {} not in [{}, {}]",
            expected.len(),
            estimation.min,
            estimation.max,
        );

        // Condition checker parity (forward index path).
        let checker = index
            .condition_checker(&condition, HwMeasurementAcc::new())
            .unwrap()
            .unwrap();
        for idx in 0..data.len() as PointOffsetType {
            assert_eq!(
                checker.check(idx).unwrap(),
                expected.contains(&idx),
                "prefix {prefix:?}, point {idx}",
            );
        }
    }
}

/// An index built *without* the prefix option must decline prefix
/// filtering/estimation (fallback path) while still serving the per-point
/// condition checker through the forward index.
#[test]
fn test_str_prefix_match_disabled() {
    use common::condition_checker::ConditionChecker as _;
    use common::counter::hardware_accumulator::HwMeasurementAcc;

    use crate::json_path::JsonPath;
    use crate::types::Match;

    let temp_dir = Builder::new().prefix("prefix_index_dir").tempdir().unwrap();
    let data = prefix_test_data();
    let hw_counter = HardwareCounterCell::new();

    let mut builder = MapIndex::<str>::builder_immutable(
        temp_dir.path(),
        false,
        &empty_deleted(),
        false, // no prefix index
    );
    builder.init().unwrap();
    for (idx, values) in data.iter().enumerate() {
        let values: Vec<Value> = values.iter().map(|v| v.to_string().into()).collect();
        let values: Vec<_> = values.iter().collect();
        builder
            .add_point(idx as PointOffsetType, &values, &hw_counter)
            .unwrap();
    }
    let index = builder.finalize().unwrap();

    let condition = FieldCondition::new_match(JsonPath::new("test"), Match::new_prefix("https://"));
    assert!(index.filter(&condition, &hw_counter).unwrap().is_none());
    assert!(
        index
            .estimate_cardinality(&condition, &hw_counter)
            .unwrap()
            .is_none()
    );

    let checker = index
        .condition_checker(&condition, HwMeasurementAcc::new())
        .unwrap()
        .unwrap();
    let expected = naive_prefix_points(&data, "https://");
    for idx in 0..data.len() as PointOffsetType {
        assert_eq!(checker.check(idx).unwrap(), expected.contains(&idx));
    }
}

#[rstest]
#[case(IndexType::MutableGridstore)]
#[case(IndexType::Mmap)]
#[case(IndexType::RamMmap)]
fn test_str_prefix_match_after_deletion(#[case] index_type: IndexType) {
    use crate::json_path::JsonPath;
    use crate::types::Match;

    let temp_dir = Builder::new().prefix("prefix_index_dir").tempdir().unwrap();
    let mut data = prefix_test_data();
    let hw_counter = HardwareCounterCell::new();

    save_map_index::<str>(&data, temp_dir.path(), index_type, |v| v.to_string().into());
    let mut index: MapIndex<str> = load_map_index(&data, temp_dir.path(), index_type);

    // Delete two points, one of which held the only "http://" value.
    for deleted in [1, 3] {
        index.remove_point(deleted).unwrap();
        data[deleted as usize].clear();
    }

    for prefix in PREFIX_PROBES {
        let expected = naive_prefix_points(&data, prefix);
        let condition = FieldCondition::new_match(JsonPath::new("test"), Match::new_prefix(prefix));
        let mut result: Vec<PointOffsetType> = index
            .filter(&condition, &hw_counter)
            .unwrap()
            .unwrap_or_else(|| panic!("prefix {prefix:?} must be served by the index"))
            .collect();
        result.sort_unstable();
        assert_eq!(result, expected, "prefix {prefix:?}");
    }
}

/// Prefix payload blocks follow the geo index principle (`large_hashes`):
/// only the *smallest* heavy subsets produce blocks, so prefix blocks are
/// disjoint from each other and from the exact-value blocks.
#[test]
fn test_str_prefix_payload_blocks() {
    use crate::json_path::JsonPath;
    use crate::types::Match;

    let temp_dir = Builder::new().prefix("prefix_index_dir").tempdir().unwrap();
    // 4 points per qdrant url (8 under "https://qdrant.tech" total, sharing
    // "https://" with 4 more), 4 under "tag*".
    let data: Vec<Vec<EcoString>> = [
        ["https://qdrant.tech/docs"; 4].as_slice(),
        ["https://qdrant.tech/blog"; 4].as_slice(),
        ["https://example.com"; 4].as_slice(),
        ["tag"; 2].as_slice(),
        ["tags"; 2].as_slice(),
    ]
    .into_iter()
    .flatten()
    .map(|value| vec![EcoString::from(*value)])
    .collect();

    save_map_index::<str>(&data, temp_dir.path(), IndexType::Mmap, |v| {
        v.to_string().into()
    });
    let index: MapIndex<str> = load_map_index(&data, temp_dir.path(), IndexType::Mmap);

    let mut prefix_blocks = Vec::new();
    let mut value_blocks = Vec::new();
    index
        .for_each_payload_block(3, JsonPath::new("test"), &mut |block| {
            match block.condition.r#match.as_ref().unwrap() {
                Match::Prefix(prefix) => {
                    prefix_blocks.push((prefix.prefix.clone(), block.cardinality));
                }
                Match::Value(_)
                | Match::Text(_)
                | Match::TextAny(_)
                | Match::Phrase(_)
                | Match::Any(_)
                | Match::Except(_) => value_blocks.push(block.cardinality),
            }
            Ok(())
        })
        .unwrap();

    // Heavy branching nodes are "https://" (12), "https://qdrant.tech/" (8)
    // and "tag" (4), but the first two contain heavy exact values (each url
    // has 4 > 3 postings and gets its own exact-value block), so they are
    // suppressed — only the smallest heavy subset produces a block. "tag"
    // covers only light values (2 postings each) and is emitted.
    assert_eq!(prefix_blocks, vec![("tag".to_string(), 4)]);
    // Exact-value blocks are unaffected: the three urls with 4 postings each.
    assert_eq!(value_blocks, vec![4, 4, 4]);
}

/// `prefix_index.bin` must be tracked by `files()` / `immutable_files()`
/// (snapshot manifest) exactly when the index is built with the prefix
/// option.
#[rstest]
#[case(false)]
#[case(true)]
fn test_prefix_index_file_tracking(#[case] with_prefix: bool) {
    use std::ffi::OsStr;

    use super::prefix_index::PREFIX_INDEX_PATH;

    let temp_dir = Builder::new().prefix("prefix_index_dir").tempdir().unwrap();
    let data = prefix_test_data();
    let hw_counter = HardwareCounterCell::new();

    let mut builder =
        MapIndex::<str>::builder_immutable(temp_dir.path(), false, &empty_deleted(), with_prefix);
    builder.init().unwrap();
    for (idx, values) in data.iter().enumerate() {
        let values: Vec<Value> = values.iter().map(|v| v.to_string().into()).collect();
        let values: Vec<_> = values.iter().collect();
        builder
            .add_point(idx as PointOffsetType, &values, &hw_counter)
            .unwrap();
    }
    let index = builder.finalize().unwrap();

    let tracks_file = |files: &[std::path::PathBuf]| {
        files
            .iter()
            .any(|file| file.file_name() == Some(OsStr::new(PREFIX_INDEX_PATH)))
    };

    assert_eq!(tracks_file(&index.files()), with_prefix);
    assert_eq!(tracks_file(&index.immutable_files()), with_prefix);
    // The file must actually exist on disk to be snapshottable.
    assert_eq!(
        temp_dir.path().join(PREFIX_INDEX_PATH).exists(),
        with_prefix
    );
}
