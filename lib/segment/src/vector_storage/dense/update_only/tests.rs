//! Writes through the update-only storage, then reads back through the
//! ordinary appendable storage opened on the same directory.

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::universal_io::MmapFs;
use tempfile::TempDir;

use super::UpdateOnlyDenseVectorStorage;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::Distance;
use crate::vector_storage::dense::appendable_dense_vector_storage::open_appendable_memmap_vector_storage_impl;
use crate::vector_storage::update_only::VectorToStore;
use crate::vector_storage::{DenseVectorStorageRead, VectorStorageRead};

type Writer = UpdateOnlyDenseVectorStorage<VectorElementType>;

const DIM: usize = 4;

fn read_back(path: &std::path::Path) -> impl DenseVectorStorageRead<VectorElementType> + use<> {
    open_appendable_memmap_vector_storage_impl::<VectorElementType>(
        path,
        DIM,
        Distance::Dot,
        AdviceSetting::Global,
        false,
    )
    .unwrap()
}

/// A batch of decoded vectors lands where the appendable storage reads it.
#[test]
fn decoded_vectors_round_trip() {
    let dir = TempDir::with_prefix("update_only_dense").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let vectors = [
        vec![1.0, 2.0, 3.0, 4.0],
        vec![5.0, 6.0, 7.0, 8.0],
        vec![9.0, 10.0, 11.0, 12.0],
    ];

    let mut writer = Writer::open(&MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(
            &MmapFs,
            0,
            vectors
                .iter()
                .map(|v| VectorToStore::Decoded(VectorRef::from(v.as_slice()))),
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let storage = read_back(dir.path());
    assert_eq!(storage.total_vector_count(), vectors.len());
    for (slot, expected) in vectors.iter().enumerate() {
        assert_eq!(
            storage.get_dense::<Random>(slot as u32).as_ref(),
            expected.as_slice(),
        );
        assert!(!storage.is_deleted_vector(slot as u32));
    }
}

/// A point with no vector under this name still takes its slot, and reads back
/// as deleted rather than as a zero vector.
#[test]
fn missing_vectors_take_their_slot_and_are_flagged() {
    let dir = TempDir::with_prefix("update_only_dense").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let present = vec![1.0, 2.0, 3.0, 4.0];
    let mut writer = Writer::open(&MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(
            &MmapFs,
            0,
            [
                VectorToStore::Decoded(VectorRef::from(present.as_slice())),
                VectorToStore::Missing,
                VectorToStore::Decoded(VectorRef::from(present.as_slice())),
            ],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let storage = read_back(dir.path());
    assert_eq!(
        storage.total_vector_count(),
        3,
        "the missing vector still occupies slot 1",
    );
    assert!(!storage.is_deleted_vector(0));
    assert!(storage.is_deleted_vector(1));
    assert!(!storage.is_deleted_vector(2));
}

/// Storage-native bytes are appended verbatim, so a vector carried over from
/// another slot needs no decode round-trip.
#[test]
fn raw_bytes_round_trip() {
    let dir = TempDir::with_prefix("update_only_dense").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let vector: Vec<VectorElementType> = vec![1.5, 2.5, 3.5, 4.5];
    let bytes = bytemuck::cast_slice(&vector).to_vec();

    let mut writer = Writer::open(&MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(&MmapFs, 0, [VectorToStore::Raw(&bytes)], &hw_counter)
        .unwrap();
    drop(writer);

    let storage = read_back(dir.path());
    assert_eq!(storage.get_dense::<Random>(0).as_ref(), vector.as_slice());
}

/// A second writer resumes where the first left off.
#[test]
fn batches_resume() {
    let dir = TempDir::with_prefix("update_only_dense").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let first = vec![1.0, 1.0, 1.0, 1.0];
    let second = vec![2.0, 2.0, 2.0, 2.0];

    let mut writer = Writer::open(&MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(
            &MmapFs,
            0,
            [VectorToStore::Decoded(VectorRef::from(first.as_slice()))],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let mut writer = Writer::open(&MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(
            &MmapFs,
            1,
            [VectorToStore::Decoded(VectorRef::from(second.as_slice()))],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let storage = read_back(dir.path());
    assert_eq!(storage.total_vector_count(), 2);
    assert_eq!(storage.get_dense::<Random>(0).as_ref(), first.as_slice());
    assert_eq!(storage.get_dense::<Random>(1).as_ref(), second.as_slice());
}

/// A malformed raw blob is reported as such, so a bad blob that reached the WAL
/// is skipped on replay instead of crash-looping recovery.
#[test]
fn malformed_raw_bytes_are_rejected() {
    let dir = TempDir::with_prefix("update_only_dense").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut writer = Writer::open(&MmapFs, dir.path(), DIM).unwrap();
    let err = writer
        .append_many(&MmapFs, 0, [VectorToStore::Raw(&[1, 2, 3])], &hw_counter)
        .unwrap_err();
    assert!(
        format!("{err}").contains("Malformed dense vector blob"),
        "{err}"
    );
}
