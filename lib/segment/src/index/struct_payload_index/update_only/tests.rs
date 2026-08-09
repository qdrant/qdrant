//! Writes a batch through the whole fan-out, then reads each field index back
//! through the ordinary appendable index that owns it — the same contract the
//! per-index writers are tested against, one level up.

use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::{MmapFile, MmapFs};
use tempfile::TempDir;

use super::UpdateOnlyStructPayloadIndex;
use crate::index::field_index::map_index::mutable_map_index::MutableMapIndex;
use crate::index::field_index::map_index::read_ops::MapIndexRead as _;
use crate::index::field_index::null_index::NullIndexRead as _;
use crate::index::field_index::null_index::mutable_null_index::MutableNullIndex;
use crate::index::payload_config::{
    FullPayloadIndexType, IndexMutability, PayloadConfig, PayloadFieldSchemaWithIndexType,
    PayloadIndexType, StorageType,
};
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::segment_constructor::get_payload_index_path;
use crate::types::{Payload, PayloadFieldSchema, PayloadSchemaType};

type Index = UpdateOnlyStructPayloadIndex<MmapFile>;

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

/// Declare `f` as a keyword index, plus the null index that complements every
/// indexed field — the shape [`StructPayloadIndex::build_field_indexes`] leaves
/// behind.
fn write_config(segment_path: &Path, types: Vec<FullPayloadIndexType>) {
    let path = get_payload_index_path(segment_path);
    fs_err::create_dir_all(&path).unwrap();

    let mut config = PayloadConfig::default();
    config.indices.insert(
        field(),
        PayloadFieldSchemaWithIndexType::new(
            PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
            types,
        ),
    );
    config.save(&PayloadConfig::get_config_path(&path)).unwrap();
}

/// A batch reaches every index of the field: the one that stores values, and
/// the null index that records which points have any.
#[test]
fn batch_reaches_every_index_of_a_field() {
    let dir = TempDir::with_prefix("update_only_struct_index").unwrap();
    let hw_counter = HardwareCounterCell::new();
    write_config(
        dir.path(),
        vec![
            index_type(PayloadIndexType::KeywordIndex),
            index_type(PayloadIndexType::NullIndex),
        ],
    );

    let payloads: Vec<Payload> = vec![
        payload_json! { "f": "alpha" },
        // No value under `f` at all — the keyword index stores nothing for it,
        // the null index still has to record that.
        payload_json! { "other": 1 },
        payload_json! { "f": null },
    ];

    let mut index = Index::open(MmapFs, dir.path()).unwrap();
    index
        .append_many(
            payloads.iter().enumerate().map(|(i, p)| (i as u32, p)),
            &hw_counter,
        )
        .unwrap();
    drop(index);

    let path = get_payload_index_path(dir.path());

    let keyword = MutableMapIndex::<str>::open_gridstore(
        PayloadIndexType::KeywordIndex.storage_dir(&path, &field()),
        false,
        false,
    )
    .unwrap()
    .unwrap();
    let values = |slot| {
        keyword
            .get_values(slot, &hw_counter)
            .map(|values| values.map(String::from).collect::<Vec<_>>())
    };
    assert_eq!(values(0), Some(vec!["alpha".to_string()]));
    assert_eq!(values(1), None);
    assert_eq!(values(2), None);

    let null = MutableNullIndex::open(
        &PayloadIndexType::NullIndex.storage_dir(&path, &field()),
        payloads.len(),
        false,
    )
    .unwrap()
    .unwrap();
    assert!(!null.values_is_empty(0).unwrap());
    assert!(null.values_is_empty(1).unwrap());
    assert!(null.values_is_null(2).unwrap());
}

/// A second batch resumes where the first left off, for both the append-backed
/// index and the mask-backed one.
#[test]
fn batches_resume() {
    let dir = TempDir::with_prefix("update_only_struct_index").unwrap();
    let hw_counter = HardwareCounterCell::new();
    write_config(
        dir.path(),
        vec![
            index_type(PayloadIndexType::KeywordIndex),
            index_type(PayloadIndexType::NullIndex),
        ],
    );

    for (slot, value) in [(0, "alpha"), (1, "beta")] {
        let payload = payload_json! { "f": value };
        let mut index = Index::open(MmapFs, dir.path()).unwrap();
        index.append_many([(slot, &payload)], &hw_counter).unwrap();
    }

    let path = get_payload_index_path(dir.path());
    let keyword = MutableMapIndex::<str>::open_gridstore(
        PayloadIndexType::KeywordIndex.storage_dir(&path, &field()),
        false,
        false,
    )
    .unwrap()
    .unwrap();

    for (slot, expected) in [(0, "alpha"), (1, "beta")] {
        assert_eq!(
            keyword
                .get_values(slot, &hw_counter)
                .map(|values| values.map(String::from).collect::<Vec<_>>()),
            Some(vec![expected.to_string()]),
        );
    }

    let null = MutableNullIndex::open(
        &PayloadIndexType::NullIndex.storage_dir(&path, &field()),
        2,
        false,
    )
    .unwrap()
    .unwrap();
    assert!(!null.values_is_empty(0).unwrap(), "first batch survived");
    assert!(!null.values_is_empty(1).unwrap());
}

/// A segment that indexes nothing opens with nothing, and a batch through it is
/// a no-op rather than an error.
#[test]
fn segment_without_indexes_opens_empty() {
    let dir = TempDir::with_prefix("update_only_struct_index").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let payload = payload_json! { "f": "alpha" };
    let mut index = Index::open(MmapFs, dir.path()).unwrap();
    index.append_many([(0, &payload)], &hw_counter).unwrap();
}

/// A config that does not say which indexes a field has is refused: this writer
/// builds no indexes, so it cannot derive them the way the writable index does,
/// and carrying on would leave whatever is on disk to rot.
#[test]
fn undeclared_index_types_are_refused() {
    let dir = TempDir::with_prefix("update_only_struct_index").unwrap();
    write_config(dir.path(), vec![]);

    let Err(err) = Index::open(MmapFs, dir.path()) else {
        panic!("a field with no declared index types must be refused");
    };
    assert!(
        format!("{err}").contains("does not record which indexes"),
        "{err}"
    );
}
