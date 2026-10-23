//! Writes through the update-only storage, then reads back through the ordinary
//! TurboQuant multi-vector storages opened on the same directory.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, Populate};
use quantization::turboquant::TQBits;
use tempfile::TempDir;

use super::UpdateOnlyMultiTurboVectorStorage;
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::turbo::multi_turbo::appendable_mmap_multi_turbo_vector_storage::{
    AppendableMmapMultiTurboVectorStorage, open_appendable_turbo_multi_vector_storage,
};
use crate::vector_storage::turbo::multi_turbo::read_only::ReadOnlyChunkedMultiTurboVectorStorage;
use crate::vector_storage::update_only::VectorToStore;
use crate::vector_storage::{
    MultiTQVectorStorage, MultiTQVectorStorageRead, TurboMultiScoring, VectorStorageRead,
};

const DIM: usize = 8;
const COUNT: PointOffsetType = 3;

fn read_back(path: &std::path::Path) -> AppendableMmapMultiTurboVectorStorage {
    open_appendable_turbo_multi_vector_storage(
        path,
        DIM,
        Distance::Dot,
        TQBits::Bits4,
        MultiVectorConfig::default(),
        false,
    )
    .unwrap()
}

/// Every slot reads as deleted and without records, one at a time and batched.
fn check_reads<S: TurboMultiScoring>(storage: &S) {
    let mut batched = vec![None; COUNT as usize];
    storage
        .for_each_record_range::<Random, _>((0..COUNT).map(|key| (key, key)), |_, key, records| {
            batched[key as usize] = Some(records.len())
        })
        .unwrap();
    assert_eq!(batched, vec![Some(0); COUNT as usize]);

    for key in 0..COUNT {
        assert!(storage.is_deleted_vector(key), "slot {key}");
        assert!(storage.get_multi_tq::<Random>(key).is_empty(), "slot {key}");
    }
}

/// No point has the vector, so no records and no chunk were written. The
/// storage must still read, and copy with `update_from` as the optimizer does:
/// a missing vector gets one zero record and stays deleted.
#[test]
fn storage_without_vectors_reads_and_copies() {
    let _scope = ambient::test_guard();
    let dir = TempDir::with_prefix("update_only_multi_turbo").unwrap();

    let mut writer = UpdateOnlyMultiTurboVectorStorage::open(
        &MmapFs,
        dir.path(),
        DIM,
        Distance::Dot,
        TQBits::Bits4,
    )
    .unwrap();
    writer
        .append_many(&MmapFs, 0, (0..COUNT).map(|_| VectorToStore::Missing))
        .unwrap();

    let storage = read_back(dir.path());
    check_reads(&storage);
    check_reads(
        &ReadOnlyChunkedMultiTurboVectorStorage::<MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            TQBits::Bits4,
            MultiVectorConfig::default(),
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap(),
    );

    let copy_dir = TempDir::with_prefix("multi_turbo_copy").unwrap();
    let mut copy = read_back(copy_dir.path());
    let zero_record = vec![0; copy.quantized_vector_size()];
    let mut records = (0..COUNT).map(|key| {
        (
            storage.get_multi_tq::<Random>(key),
            storage.is_deleted_vector(key),
        )
    });
    let stopped = AtomicBool::new(false);
    assert_eq!(copy.update_from(&mut records, &stopped).unwrap(), 0..COUNT);
    for key in 0..COUNT {
        assert!(copy.is_deleted_vector(key), "slot {key}");
        assert_eq!(
            copy.get_multi_tq::<Random>(key).as_ref(),
            zero_record,
            "slot {key}"
        );
    }
}
