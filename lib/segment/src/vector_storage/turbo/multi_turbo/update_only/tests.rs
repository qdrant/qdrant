//! Writes through the update-only storage, then reads back through the ordinary
//! appendable TurboQuant multi-vector storage opened on the same directory.

use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::MmapFs;
use quantization::turboquant::TQBits;
use tempfile::TempDir;

use super::UpdateOnlyMultiTurboVectorStorage;
use crate::data_types::vectors::{MultiDenseVectorInternal, VectorRef};
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::turbo::multi_turbo::appendable_mmap_multi_turbo_vector_storage::{
    AppendableMmapMultiTurboVectorStorage, open_appendable_turbo_multi_vector_storage,
};
use crate::vector_storage::update_only::VectorToStore;
use crate::vector_storage::{
    MultiTQVectorStorage, MultiTQVectorStorageRead, VectorStorage, VectorStorageRead,
};

type Writer = UpdateOnlyMultiTurboVectorStorage;

const DIM: usize = 8;

fn multi(rows: usize) -> MultiDenseVectorInternal {
    MultiDenseVectorInternal::new((0..rows * DIM).map(|i| i as f32 + 0.5).collect(), DIM)
}

fn read_back(path: &Path) -> AppendableMmapMultiTurboVectorStorage {
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

/// A missing multi-vector is stored as the writable side stores it: one
/// placeholder record, flagged deleted.
#[test]
fn missing_vectors_match_the_writable_side() {
    let present = multi(2);
    let _scope = ambient::test_guard();

    // Written by the update-only writer.
    let ours = TempDir::with_prefix("update_only_multi_turbo").unwrap();
    let mut writer = Writer::open(&MmapFs, ours.path(), DIM, Distance::Dot, TQBits::Bits4).unwrap();
    writer
        .append_many(
            &MmapFs,
            0,
            [
                VectorToStore::Missing,
                VectorToStore::Decoded(VectorRef::from(&present)),
                VectorToStore::Missing,
            ],
        )
        .unwrap();
    drop(writer);

    // Written by the storage itself, as `insert_deleted_default` does.
    let theirs = TempDir::with_prefix("multi_turbo_reference").unwrap();
    let mut reference = read_back(theirs.path());
    let placeholder = MultiDenseVectorInternal::placeholder(DIM);
    for (key, vector) in [(0, &placeholder), (1, &present), (2, &placeholder)] {
        reference
            .insert_vector(key, VectorRef::from(vector))
            .unwrap();
    }
    reference.delete_vector(0).unwrap();
    reference.delete_vector(2).unwrap();

    let ours = read_back(ours.path());
    for key in 0..3 {
        assert_eq!(
            ours.get_multi_tq::<Random>(key).as_ref(),
            reference.get_multi_tq::<Random>(key).as_ref(),
            "slot {key}",
        );
        assert_eq!(
            ours.is_deleted_vector(key),
            reference.is_deleted_vector(key)
        );
    }
}

/// The optimizer can copy what this writer stores, missing vectors included,
/// even when a batch has no vector at all.
#[test]
fn missing_vectors_survive_update_from() {
    let present = multi(3);
    let _scope = ambient::test_guard();

    let source_dir = TempDir::with_prefix("update_only_multi_turbo").unwrap();
    let mut writer = Writer::open(
        &MmapFs,
        source_dir.path(),
        DIM,
        Distance::Dot,
        TQBits::Bits4,
    )
    .unwrap();
    writer
        .append_many(&MmapFs, 0, [VectorToStore::Missing, VectorToStore::Missing])
        .unwrap();
    writer
        .append_many(
            &MmapFs,
            2,
            [
                VectorToStore::Missing,
                VectorToStore::Decoded(VectorRef::from(&present)),
                VectorToStore::Missing,
            ],
        )
        .unwrap();
    drop(writer);

    let source = read_back(source_dir.path());
    let copy_dir = TempDir::with_prefix("multi_turbo_copy").unwrap();
    let mut copy = read_back(copy_dir.path());
    let stopped = AtomicBool::new(false);
    let mut records = (0..5).map(|key| {
        (
            source.get_multi_tq::<Random>(key),
            source.is_deleted_vector(key),
        )
    });
    assert_eq!(copy.update_from(&mut records, &stopped).unwrap(), 0..5);

    for key in 0..5 as PointOffsetType {
        assert_eq!(copy.is_deleted_vector(key), key != 3, "slot {key}");
        assert_eq!(
            copy.get_multi_tq::<Random>(key).as_ref(),
            source.get_multi_tq::<Random>(key).as_ref(),
            "slot {key}",
        );
    }
    assert_eq!(copy.deleted_vector_count(), 4);
}
