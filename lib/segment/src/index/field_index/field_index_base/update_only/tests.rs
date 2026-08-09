//! Every test here writes through the update-only writer and reads back through
//! the ordinary appendable index, opened on the directory the writer produced.

use std::path::{Path, PathBuf};
use std::str::FromStr as _;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use serde_json::{Value, json};
use tempfile::TempDir;

use super::UpdateOnlyFieldIndex;
use crate::index::field_index::bool_index::{BoolIndexRead as _, MutableBoolIndex};
use crate::index::field_index::geo_index::mutable_geo_index::MutableGeoIndex;
use crate::index::field_index::geo_index::read_ops::GeoIndexRead as _;
use crate::index::field_index::map_index::mutable_map_index::MutableMapIndex;
use crate::index::field_index::map_index::read_ops::MapIndexRead as _;
use crate::index::field_index::null_index::{MutableNullIndex, NullIndexRead as _};
use crate::index::field_index::numeric_index::mutable_numeric_index::MutableNumericIndex;
use crate::index::payload_config::{
    FullPayloadIndexType, IndexMutability, PayloadIndexType, StorageType,
};
use crate::json_path::JsonPath;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, PayloadFieldSchema, PayloadSchemaType,
    UuidIntType,
};

type Writer = UpdateOnlyFieldIndex<MmapFile>;

fn field() -> JsonPath {
    JsonPath::new("f")
}

fn index_type(index_type: PayloadIndexType) -> FullPayloadIndexType {
    FullPayloadIndexType {
        index_type,
        mutability: IndexMutability::Mutable,
        storage_type: StorageType::Gridstore,
    }
}

/// Write `points` through the update-only writer and return the directory the
/// index landed in, ready to be opened by the appendable index.
fn write(
    dir: &Path,
    kind: PayloadIndexType,
    schema: &PayloadFieldSchema,
    points: &[(PointOffsetType, Value)],
) -> PathBuf {
    let hw_counter = HardwareCounterCell::new();
    let storage = kind.storage_dir(dir, &field());
    let index_type = index_type(kind);

    let mut writer = Writer::open(MmapFs, dir, &field(), schema, &index_type).unwrap();
    for (slot, value) in points {
        writer.add_point(*slot, &[value], &hw_counter).unwrap();
    }
    writer.flush(&hw_counter).unwrap();

    storage
}

fn schema(schema_type: PayloadSchemaType) -> PayloadFieldSchema {
    PayloadFieldSchema::FieldType(schema_type)
}

#[test]
fn int_index_round_trip() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let storage = write(
        dir.path(),
        PayloadIndexType::IntIndex,
        &schema(PayloadSchemaType::Integer),
        // A scalar, an array contributing both its elements, and a value of the wrong type
        &[
            (0, json!(7)),
            (1, json!([1, 2])),
            (3, json!("not a number")),
        ],
    );

    let index = MutableNumericIndex::<IntPayloadType>::open_gridstore(storage, false)
        .unwrap()
        .unwrap();
    let index = index.into_in_memory_index();

    assert_eq!(index.point_to_values[0], vec![7]);
    assert_eq!(index.point_to_values[1], vec![1, 2]);
    assert_eq!(index.points_count, 2, "slot 3 stored nothing");
}

#[test]
fn float_index_round_trip() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let storage = write(
        dir.path(),
        PayloadIndexType::FloatIndex,
        &schema(PayloadSchemaType::Float),
        &[(0, json!(1.5)), (2, json!([2.5, 3.5]))],
    );

    let index = MutableNumericIndex::<FloatPayloadType>::open_gridstore(storage, false)
        .unwrap()
        .unwrap()
        .into_in_memory_index();

    assert_eq!(index.point_to_values[0], vec![1.5]);
    assert_eq!(index.point_to_values[2], vec![2.5, 3.5]);
    assert!(index.point_to_values[1].is_empty(), "slot 1 was skipped");
}

/// The datetime index reads RFC 3339 strings and stores timestamps, so this
/// also covers the payload-type-to-stored-type encoding.
#[test]
fn datetime_index_round_trip() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let storage = write(
        dir.path(),
        PayloadIndexType::DatetimeIndex,
        &schema(PayloadSchemaType::Datetime),
        &[(0, json!("2026-08-09T12:00:00Z"))],
    );

    let index = MutableNumericIndex::<IntPayloadType>::open_gridstore(storage, false)
        .unwrap()
        .unwrap()
        .into_in_memory_index();

    // Taken from the payload type's own encoding: the index stores microseconds
    let expected = DateTimePayloadType::from_str("2026-08-09T12:00:00Z")
        .unwrap()
        .timestamp();
    assert_eq!(index.point_to_values[0], vec![expected]);
}

#[test]
fn keyword_index_round_trip() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let storage = write(
        dir.path(),
        PayloadIndexType::KeywordIndex,
        &schema(PayloadSchemaType::Keyword),
        &[(0, json!("alpha")), (1, json!(["beta", "gamma"]))],
    );

    let index = MutableMapIndex::<str>::open_gridstore(storage, false, false)
        .unwrap()
        .unwrap();

    let hw_counter = HardwareCounterCell::new();
    let values = |slot| {
        index
            .get_values(slot, &hw_counter)
            .map(|values| values.map(String::from).collect::<Vec<_>>())
    };
    assert_eq!(values(0), Some(vec!["alpha".to_string()]));
    assert_eq!(
        values(1),
        Some(vec!["beta".to_string(), "gamma".to_string()]),
    );
}

#[test]
fn uuid_map_index_round_trip() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let uuid = uuid::Uuid::from_u128(42);
    let storage = write(
        dir.path(),
        PayloadIndexType::UuidMapIndex,
        &schema(PayloadSchemaType::Uuid),
        &[(0, json!(uuid.to_string()))],
    );

    let index = MutableMapIndex::<UuidIntType>::open_gridstore(storage, false, false)
        .unwrap()
        .unwrap();

    let hw_counter = HardwareCounterCell::new();
    assert_eq!(
        index
            .get_values(0, &hw_counter)
            .map(|values| values.map(|value| *value).collect::<Vec<_>>()),
        Some(vec![uuid.as_u128()]),
    );
}

#[test]
fn geo_index_round_trip() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let storage = write(
        dir.path(),
        PayloadIndexType::GeoIndex,
        &schema(PayloadSchemaType::Geo),
        &[(0, json!({"lon": 13.4, "lat": 52.5}))],
    );

    let index = MutableGeoIndex::open(storage, false).unwrap().unwrap();

    let stored = index.get_values(0).unwrap().collect::<Vec<_>>();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].lon, 13.4);
    assert_eq!(stored[0].lat, 52.5);
}

/// Slots must be written in increasing order, nothing may go back and rewrite a
/// slot that was already indexed.
#[test]
fn rewriting_a_slot_is_rejected() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let hw_counter = HardwareCounterCell::new();
    let schema = schema(PayloadSchemaType::Integer);

    let mut writer = Writer::open(
        MmapFs,
        dir.path(),
        &field(),
        &schema,
        &index_type(PayloadIndexType::IntIndex),
    )
    .unwrap();

    writer.add_point(1, &[&json!(1)], &hw_counter).unwrap();
    assert!(writer.add_point(0, &[&json!(0)], &hw_counter).is_err());
}

/// The boolean index is mask-backed: its writer rewrites both masks whole, and
/// leaves behind the same two files the mutable index writes.
#[test]
fn bool_index_round_trip() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let storage = write(
        dir.path(),
        PayloadIndexType::BoolIndex,
        &schema(PayloadSchemaType::Bool),
        // True, both at once, false — and a value the index cannot read
        &[
            (0, json!(true)),
            (1, json!([true, false])),
            (2, json!(false)),
            (3, json!("not a bool")),
        ],
    );

    let index = MutableBoolIndex::open(&storage, false).unwrap().unwrap();

    assert_eq!(index.get_point_values(0).unwrap(), vec![true]);
    assert_eq!(index.get_point_values(1).unwrap(), vec![true, false]);
    assert_eq!(index.get_point_values(2).unwrap(), vec![false]);
    assert!(index.values_is_empty(3).unwrap());
    assert_eq!(index.indexed_count().unwrap(), 3);
}

/// The null index records two bits for every point of the batch, including the
/// points whose field holds nothing.
#[test]
fn null_index_round_trip() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let storage = write(
        dir.path(),
        PayloadIndexType::NullIndex,
        &schema(PayloadSchemaType::Keyword),
        &[
            (0, json!("a value")),
            (1, json!(null)),
            // An array holding a null holds both a value and a null
            (2, json!(["a value", null])),
            (3, json!([])),
        ],
    );

    let index = MutableNullIndex::open(&storage, 4, false).unwrap().unwrap();

    assert!(!index.values_is_empty(0).unwrap());
    assert!(!index.values_is_null(0).unwrap());

    assert!(index.values_is_empty(1).unwrap());
    assert!(index.values_is_null(1).unwrap());

    assert!(!index.values_is_empty(2).unwrap());
    assert!(index.values_is_null(2).unwrap());

    // An empty array is neither, and a point past the batch was never seen
    assert!(index.values_is_empty(3).unwrap());
    assert!(!index.values_is_null(3).unwrap());
    assert!(index.values_is_empty(9).unwrap());
}

/// A second writer picks the mask up where the first left it, rather than
/// starting from an empty one.
#[test]
fn bitmask_writers_resume() {
    let dir = TempDir::with_prefix("update_only_index").unwrap();
    let schema = schema(PayloadSchemaType::Bool);

    let storage = write(
        dir.path(),
        PayloadIndexType::BoolIndex,
        &schema,
        &[(0, json!(true))],
    );
    write(
        dir.path(),
        PayloadIndexType::BoolIndex,
        &schema,
        &[(1, json!(false))],
    );

    let index = MutableBoolIndex::open(&storage, false).unwrap().unwrap();
    assert_eq!(index.get_point_values(0).unwrap(), vec![true]);
    assert_eq!(index.get_point_values(1).unwrap(), vec![false]);
}
