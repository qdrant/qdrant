//! Writes through the update-only storage, then reads back through the
//! ordinary appendable multi-vector storage opened on the same directory.

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::universal_io::{MmapFile, MmapFs};
use tempfile::TempDir;

use super::UpdateOnlyMultiDenseVectorStorage;
use crate::data_types::vectors::{
    MultiDenseVectorInternal, TypedMultiDenseVectorRef, VectorElementType, VectorRef,
};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::open_appendable_memmap_multi_vector_storage;
use crate::vector_storage::update_only::VectorToStore;
use crate::vector_storage::{MultiVectorStorageRead, VectorStorageRead};

type Writer = UpdateOnlyMultiDenseVectorStorage<VectorElementType, MmapFile>;

const DIM: usize = 2;

fn multi(rows: &[[VectorElementType; DIM]]) -> MultiDenseVectorInternal {
    MultiDenseVectorInternal::new(rows.iter().flatten().copied().collect(), DIM)
}

/// Reopen through the writable storage the reader also uses, so the assertion
/// runs against the real decode path rather than the raw files.
fn read_back(path: &std::path::Path) -> crate::vector_storage::VectorStorageEnum {
    open_appendable_memmap_multi_vector_storage(
        VectorStorageDatatype::Float32,
        path,
        DIM,
        Distance::Dot,
        MultiVectorConfig::default(),
        AdviceSetting::Global,
        false,
    )
    .unwrap()
}

fn stored(storage: &crate::vector_storage::VectorStorageEnum, slot: u32) -> Vec<Vec<f32>> {
    let crate::vector_storage::VectorStorageEnum::MultiDenseAppendableMemmap(storage) = storage
    else {
        panic!("expected an appendable multi-dense storage");
    };
    let multi = storage.get_multi::<Random>(slot);
    let multi = TypedMultiDenseVectorRef::from(multi.as_vec_ref());
    multi
        .flattened_vectors
        .chunks_exact(multi.dim)
        .map(<[f32]>::to_vec)
        .collect()
}

/// Each point owns a run of rows, and the runs land back to back.
#[test]
fn multi_vectors_round_trip() {
    let dir = TempDir::with_prefix("update_only_multi").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let first = multi(&[[1.0, 2.0], [3.0, 4.0]]);
    let second = multi(&[[5.0, 6.0]]);

    let mut writer = Writer::open(MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(
            0,
            [
                VectorToStore::Decoded(VectorRef::from(&first)),
                VectorToStore::Decoded(VectorRef::from(&second)),
            ],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let storage = read_back(dir.path());
    assert_eq!(stored(&storage, 0), vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    assert_eq!(stored(&storage, 1), vec![vec![5.0, 6.0]]);
}

/// A point with no multi-vector here owns no rows, and is flagged deleted; the
/// point after it still reads back correctly.
#[test]
fn missing_multi_vectors_own_no_rows() {
    let dir = TempDir::with_prefix("update_only_multi").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let present = multi(&[[1.0, 2.0]]);
    let mut writer = Writer::open(MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(
            0,
            [
                VectorToStore::Missing,
                VectorToStore::Decoded(VectorRef::from(&present)),
            ],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let storage = read_back(dir.path());
    let crate::vector_storage::VectorStorageEnum::MultiDenseAppendableMemmap(inner) = &storage
    else {
        panic!("expected an appendable multi-dense storage");
    };
    assert!(inner.is_deleted_vector(0));
    assert!(!inner.is_deleted_vector(1));
    assert_eq!(stored(&storage, 1), vec![vec![1.0, 2.0]]);
}

/// A second writer resumes at the row the first one left off at, rather than
/// overwriting its rows.
#[test]
fn batches_resume_at_the_row_space_end() {
    let dir = TempDir::with_prefix("update_only_multi").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let first = multi(&[[1.0, 1.0], [2.0, 2.0]]);
    let second = multi(&[[3.0, 3.0]]);

    let mut writer = Writer::open(MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(
            0,
            [VectorToStore::Decoded(VectorRef::from(&first))],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let mut writer = Writer::open(MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(
            1,
            [VectorToStore::Decoded(VectorRef::from(&second))],
            &hw_counter,
        )
        .unwrap();
    drop(writer);

    let storage = read_back(dir.path());
    assert_eq!(stored(&storage, 0), vec![vec![1.0, 1.0], vec![2.0, 2.0]]);
    assert_eq!(
        stored(&storage, 1),
        vec![vec![3.0, 3.0]],
        "the second batch must not have reused the first batch's rows",
    );
}

/// Storage-native bytes are the flattened inner vectors; a blob that is not a
/// whole number of them is reported as malformed.
#[test]
fn raw_multi_bytes_round_trip() {
    let dir = TempDir::with_prefix("update_only_multi").unwrap();
    let hw_counter = HardwareCounterCell::new();

    let flattened: Vec<VectorElementType> = vec![1.0, 2.0, 3.0, 4.0];
    let bytes = bytemuck::cast_slice(&flattened).to_vec();

    let mut writer = Writer::open(MmapFs, dir.path(), DIM).unwrap();
    writer
        .append_many(0, [VectorToStore::Raw(&bytes)], &hw_counter)
        .unwrap();

    let err = writer
        .append_many(1, [VectorToStore::Raw(&bytes[..5])], &hw_counter)
        .unwrap_err();
    assert!(
        format!("{err}").contains("Malformed multi vector blob"),
        "{err}"
    );
    drop(writer);

    let storage = read_back(dir.path());
    assert_eq!(stored(&storage, 0), vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
}
