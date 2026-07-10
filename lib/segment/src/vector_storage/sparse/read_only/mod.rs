use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::vector_storage::sparse::stored_sparse_vectors::StoredSparseVector;

mod lifecycle;
mod live_reload;
mod read_ops;

#[derive(Debug)]
pub struct ReadOnlySparseVectorStorage<S: UniversalRead> {
    storage: GridstoreReader<StoredSparseVector, S>,
    /// Flags marking deleted vectors.
    deleted: InMemoryBitvecFlags,
    next_point_offset: usize,
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{CachedFs, CachedReadFs, MmapFile, MmapFs};
    use sparse::common::sparse_vector::SparseVector;
    use tempfile::Builder;

    use super::*;
    use crate::common::live_reload::LiveReload;
    use crate::data_types::named_vectors::CowVector;
    use crate::data_types::vectors::VectorRef;
    use crate::vector_storage::sparse::SPARSE_VECTOR_DISTANCE;
    use crate::vector_storage::sparse::mmap_sparse_vector_storage::{
        DELETED_DIRNAME, MmapSparseVectorStorage, STORAGE_DIRNAME,
    };
    use crate::vector_storage::{VectorStorage, VectorStorageRead};

    /// Write sparse vectors (deleting some) through the writable mmap storage,
    /// then reopen the same directory read-only and assert it mirrors the state,
    /// including per-point sparse contents and the reconstructed point count.
    #[test]
    fn read_only_sparse_round_trip() {
        const POINT_COUNT: PointOffsetType = 500;

        let dir = Builder::new().prefix("ro_sparse").tempdir().unwrap();
        let hw = HardwareCounterCell::disposable();

        let sparse_vectors: Vec<SparseVector> = (0..POINT_COUNT)
            .map(|id| {
                let value = id as f32;
                SparseVector {
                    indices: vec![1, 5, 9],
                    values: vec![value + 0.1, value + 0.2, value + 0.3],
                }
            })
            .collect();

        let mut deleted_ids = Vec::new();
        {
            let mut storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
            for (id, vector) in sparse_vectors.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            for id in (0..POINT_COUNT).step_by(7) {
                storage.delete_vector(id).unwrap();
                deleted_ids.push(id);
            }
            storage.flusher()().unwrap();
        }

        let storage = ReadOnlySparseVectorStorage::<MmapFile>::open(&MmapFs, dir.path()).unwrap();

        assert_eq!(storage.total_vector_count(), POINT_COUNT as usize);
        assert_eq!(storage.distance(), SPARSE_VECTOR_DISTANCE);
        assert_eq!(storage.deleted_vector_count(), deleted_ids.len());

        for id in 0..POINT_COUNT {
            let deleted = deleted_ids.contains(&id);
            assert_eq!(storage.is_deleted_vector(id), deleted);

            // Deleting a sparse vector reclaims its Gridstore entry, so only
            // live points still carry their contents.
            if deleted {
                continue;
            }

            match storage.get_vector::<Random>(id) {
                CowVector::Sparse(got) => {
                    assert_eq!(got.indices, sparse_vectors[id as usize].indices);
                    assert_eq!(got.values, sparse_vectors[id as usize].values);
                }
                CowVector::Dense(_) | CowVector::MultiDense(_) => {
                    panic!("expected sparse vector for point {id}")
                }
            }
        }
    }

    /// `preopen` must schedule exactly the files `open` goes on to consume.
    ///
    /// Merely opening after a `preopen` proves nothing: `CachedFs` falls back
    /// to a plain inner open for any path that was never scheduled, so a
    /// scheduled-vs-opened path mismatch would still yield a correct storage.
    /// To make the prefetch pool the *only* possible source, the storage
    /// directories are removed between `preopen` and `open`: the already-open
    /// handles parked in the pool stay readable, while any fallback open hits
    /// `NotFound`.
    #[test]
    fn preopen_then_open_through_cached_fs() {
        const POINT_COUNT: PointOffsetType = 500;

        let dir = Builder::new()
            .prefix("ro_sparse_preopen")
            .tempdir()
            .unwrap();
        let hw = HardwareCounterCell::disposable();

        let sparse_vectors: Vec<SparseVector> = (0..POINT_COUNT)
            .map(|id| {
                let value = id as f32;
                SparseVector {
                    indices: vec![1, 5, 9],
                    values: vec![value + 0.1, value + 0.2, value + 0.3],
                }
            })
            .collect();

        {
            let mut storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
            for (id, vector) in sparse_vectors.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            storage.flusher()().unwrap();
        }

        // Same order as the segment open path: snapshot, then preopen, then open.
        let mut cached_fs = CachedFs::new(MmapFs, dir.path()).unwrap();
        cached_fs.cache_file_info().unwrap();
        ReadOnlySparseVectorStorage::<MmapFile>::preopen(&cached_fs, dir.path()).unwrap();

        // Everything `open` reads must now come from the prefetch pool.
        for dir_name in [STORAGE_DIRNAME, DELETED_DIRNAME] {
            fs_err::remove_dir_all(dir.path().join(dir_name)).unwrap();
        }

        let storage =
            ReadOnlySparseVectorStorage::<MmapFile>::open(&cached_fs, dir.path()).unwrap();

        assert_eq!(storage.total_vector_count(), POINT_COUNT as usize);
        match storage.get_vector::<Random>(7) {
            CowVector::Sparse(got) => {
                assert_eq!(got.indices, sparse_vectors[7].indices);
                assert_eq!(got.values, sparse_vectors[7].values);
            }
            CowVector::Dense(_) | CowVector::MultiDense(_) => {
                panic!("expected sparse vector for point 7")
            }
        }
    }

    /// After `live_reload`, the read-only view reflects appends and deletions.
    #[test]
    fn live_reload_picks_up_appends_and_deletions() {
        let dir = Builder::new().prefix("ro_sparse_reload").tempdir().unwrap();
        let hw = HardwareCounterCell::disposable();

        fn make(id: usize) -> SparseVector {
            SparseVector {
                indices: vec![1, 5, 9],
                values: vec![id as f32 + 0.1, id as f32 + 0.2, id as f32 + 0.3],
            }
        }
        let first: Vec<SparseVector> = (0..150).map(make).collect();
        let second: Vec<SparseVector> = (150..250).map(make).collect();

        let mut writer = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
        for (id, vector) in first.iter().enumerate() {
            writer
                .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                .unwrap();
        }
        writer.flusher()().unwrap();

        let mut reader =
            ReadOnlySparseVectorStorage::<MmapFile>::open(&MmapFs, dir.path()).unwrap();
        assert_eq!(reader.total_vector_count(), first.len());

        for (offset, vector) in second.iter().enumerate() {
            writer
                .insert_vector(
                    (first.len() + offset) as PointOffsetType,
                    VectorRef::from(vector),
                    &hw,
                )
                .unwrap();
        }
        let deleted_ids: Vec<PointOffsetType> = vec![2, 80, 149];
        for &id in &deleted_ids {
            writer.delete_vector(id).unwrap();
        }
        writer.flusher()().unwrap();

        let new_ids: Vec<PointOffsetType> = (first.len()..first.len() + second.len())
            .map(|offset| offset as PointOffsetType)
            .collect();
        reader
            .live_reload(
                &MmapFs,
                &SortedSlice::new(&deleted_ids).unwrap(),
                &SortedSlice::new(&new_ids).unwrap(),
                &hw,
            )
            .unwrap();

        assert_eq!(reader.total_vector_count(), first.len() + second.len());
        assert_eq!(reader.deleted_vector_count(), deleted_ids.len());

        // The appended vector is visible; deleted ones are flagged.
        match reader.get_vector::<Random>(first.len() as PointOffsetType) {
            CowVector::Sparse(got) => assert_eq!(got.values, second[0].values),
            CowVector::Dense(_) | CowVector::MultiDense(_) => panic!("expected sparse vector"),
        }
        for &id in &deleted_ids {
            assert!(reader.is_deleted_vector(id));
        }
        assert!(!reader.is_deleted_vector(0));
    }
}
