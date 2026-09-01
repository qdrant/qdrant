//! Writes through the update-only storage, then reads back through the
//! ordinary mmap sparse storage opened on the same directory.

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::universal_io::{MmapFile, MmapFs};
use sparse::common::sparse_vector::SparseVector;
use tempfile::TempDir;

use super::UpdateOnlySparseVectorStorage;
use crate::data_types::vectors::VectorRef;
use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::vector_storage::update_only::VectorToStore;
use crate::vector_storage::{SparseVectorStorageRead, VectorStorageRead};

type Writer = UpdateOnlySparseVectorStorage<MmapFile>;

fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).unwrap()
}

/// Sparse vectors land where the mmap storage reads them, and a point without
/// one is flagged rather than stored empty.
#[test]
fn sparse_vectors_round_trip() {
    let dir = TempDir::with_prefix("update_only_sparse").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let first = sparse(&[1, 5], &[1.0, 2.0]);
    let second = sparse(&[3], &[3.0]);

    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    writer
        .append_many(
            &MmapFs,
            0,
            [
                VectorToStore::Decoded(VectorRef::from(&first)),
                VectorToStore::Missing,
                VectorToStore::Decoded(VectorRef::from(&second)),
            ],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
    assert_eq!(storage.get_sparse::<Random>(0).unwrap(), first);
    assert_eq!(storage.get_sparse::<Random>(2).unwrap(), second);
    assert!(storage.is_deleted_vector(1));
    assert!(!storage.is_deleted_vector(0));
    assert!(!storage.is_deleted_vector(2));
}

/// A second writer appends above what the first left, rather than starting over.
#[test]
fn batches_resume() {
    let dir = TempDir::with_prefix("update_only_sparse").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let first = sparse(&[1], &[1.0]);
    let second = sparse(&[2], &[2.0]);

    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    writer
        .append_many(
            &MmapFs,
            0,
            [VectorToStore::Decoded(VectorRef::from(&first))],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let mut writer = Writer::open(MmapFs, dir.path()).unwrap();
    writer
        .append_many(
            &MmapFs,
            1,
            [VectorToStore::Decoded(VectorRef::from(&second))],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
    assert_eq!(storage.get_sparse::<Random>(0).unwrap(), first);
    assert_eq!(storage.get_sparse::<Random>(1).unwrap(), second);
}
