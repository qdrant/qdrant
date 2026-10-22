//! Writes through the update-only storage, then reads back through the ordinary
//! TurboQuant multi-vector storages opened on the same directory.

use std::borrow::Cow;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, Populate};
use quantization::turboquant::TQBits;
use tempfile::TempDir;

use super::UpdateOnlyMultiTurboVectorStorage;
use crate::data_types::vectors::{MultiDenseVectorInternal, VectorRef};
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::turbo::multi_turbo::appendable_mmap_multi_turbo_vector_storage::{
    AppendableMmapMultiTurboVectorStorage, open_appendable_turbo_multi_vector_storage,
};
use crate::vector_storage::turbo::multi_turbo::read_only::ReadOnlyChunkedMultiTurboVectorStorage;
use crate::vector_storage::update_only::VectorToStore;
use crate::vector_storage::{
    MultiTQVectorStorage, MultiTQVectorStorageRead, TurboMultiScoring, VectorStorageRead,
};

type Writer = UpdateOnlyMultiTurboVectorStorage;

const DIM: usize = 8;

fn multi(rows: usize) -> MultiDenseVectorInternal {
    MultiDenseVectorInternal::new((0..rows * DIM).map(|i| i as f32 + 0.5).collect(), DIM)
}

/// Write `batches` of vectors, `None` for a missing one, one `append_many`
/// per batch.
fn write(path: &Path, batches: &[&[Option<&MultiDenseVectorInternal>]]) {
    let mut writer = Writer::open(&MmapFs, path, DIM, Distance::Dot, TQBits::Bits4).unwrap();
    let mut start = 0;
    for batch in batches {
        let vectors = batch.iter().map(|vector| match vector {
            Some(vector) => VectorToStore::Decoded(VectorRef::from(*vector)),
            None => VectorToStore::Missing,
        });
        writer.append_many(&MmapFs, start, vectors).unwrap();
        start += batch.len() as PointOffsetType;
    }
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

fn read_only(path: &Path) -> ReadOnlyChunkedMultiTurboVectorStorage<MmapFile> {
    ReadOnlyChunkedMultiTurboVectorStorage::open(
        &MmapFs,
        path,
        DIM,
        Distance::Dot,
        TQBits::Bits4,
        MultiVectorConfig::default(),
        AdviceSetting::Global,
        Populate::No,
    )
    .unwrap()
}

/// A missing vector reads as deleted and without records, one at a time and
/// batched; the others read as written. `present` holds the inner vector
/// count of each point, `None` for a missing vector.
fn check_reads<S: TurboMultiScoring>(storage: &S, present: &[Option<usize>]) {
    let record_size = storage.quantized_vector_size();
    let mut batched = vec![None; present.len()];
    storage
        .for_each_record_range::<Random, _>(
            (0..present.len() as PointOffsetType).map(|key| (key, key)),
            |_, key, records| batched[key as usize] = Some(records.len()),
        )
        .unwrap();

    for (key, rows) in present.iter().enumerate() {
        let records = rows.map_or(0, |rows| rows * record_size);
        assert_eq!(batched[key], Some(records), "slot {key}");
        let key = key as PointOffsetType;
        assert_eq!(storage.is_deleted_vector(key), rows.is_none(), "slot {key}");
        assert_eq!(
            storage.get_multi_tq::<Random>(key).len(),
            records,
            "slot {key}"
        );
    }
}

/// Copy `source` with `update_from`, as the optimizer does: a missing vector
/// gets one placeholder record and stays deleted.
fn check_copy(source: &AppendableMmapMultiTurboVectorStorage, present: &[Option<usize>]) {
    let dir = TempDir::with_prefix("multi_turbo_copy").unwrap();
    let mut copy = read_back(dir.path());
    let record_size = copy.quantized_vector_size();
    let stopped = AtomicBool::new(false);
    let count = present.len() as PointOffsetType;

    let mut records = (0..count).map(|key| {
        (
            source.get_multi_tq::<Random>(key),
            source.is_deleted_vector(key),
        )
    });
    assert_eq!(copy.update_from(&mut records, &stopped).unwrap(), 0..count);

    for (key, rows) in present.iter().enumerate() {
        let key = key as PointOffsetType;
        let expected = match rows {
            Some(_) => source.get_multi_tq::<Random>(key),
            None => Cow::Owned(vec![0; record_size]),
        };
        assert_eq!(copy.is_deleted_vector(key), rows.is_none(), "slot {key}");
        assert_eq!(
            copy.get_multi_tq::<Random>(key).as_ref(),
            expected.as_ref(),
            "slot {key}",
        );
    }
}

/// Missing vectors between and around present ones, over two batches.
#[test]
fn missing_vectors_read_and_copy() {
    let (two, three) = (multi(2), multi(3));
    let _scope = ambient::test_guard();

    let dir = TempDir::with_prefix("update_only_multi_turbo").unwrap();
    write(
        dir.path(),
        &[&[None, Some(&two)], &[None, None, Some(&three), None]],
    );
    let present = [None, Some(2), None, None, Some(3), None];

    let storage = read_back(dir.path());
    check_reads(&storage, &present);
    check_copy(&storage, &present);
    check_reads(&read_only(dir.path()), &present);
}

/// No point has the vector, so no records were written at all.
#[test]
fn storage_without_vectors_reads_and_copies() {
    let _scope = ambient::test_guard();

    let dir = TempDir::with_prefix("update_only_multi_turbo").unwrap();
    write(dir.path(), &[&[None, None], &[None]]);
    let present = [None; 3];

    let storage = read_back(dir.path());
    check_reads(&storage, &present);
    check_copy(&storage, &present);
    check_reads(&read_only(dir.path()), &present);
}
