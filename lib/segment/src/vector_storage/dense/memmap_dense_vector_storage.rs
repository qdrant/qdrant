use std::borrow::Cow;
use std::io::{self, BufWriter, Write};
use std::mem::MaybeUninit;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use fs_err as fs;
use fs_err::{File, OpenOptions};
use memory::fadvise::clear_disk_cache;
use memory::mmap_ops;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::common::get_async_scorer;
use crate::vector_storage::dense::mmap_dense_vectors::MmapDenseVectors;
use crate::vector_storage::{AccessPattern, DenseVectorStorage, VectorStorage, VectorStorageEnum};

const VECTORS_PATH: &str = "matrix.dat";
const DELETED_PATH: &str = "deleted.dat";

/// Stores all dense vectors in mem-mapped file
///
/// It is not possible to insert new vectors into mem-mapped storage,
/// but possible to mark some vectors as removed
///
/// Mem-mapped storage can only be constructed from another storage
#[derive(Debug)]
pub struct MemmapDenseVectorStorage<T: PrimitiveVectorElement> {
    vectors_path: PathBuf,
    deleted_path: PathBuf,
    mmap_store: Option<MmapDenseVectors<T>>,
    distance: Distance,
}

impl<T: PrimitiveVectorElement> MemmapDenseVectorStorage<T> {
    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        if let Some(mmap_store) = &self.mmap_store {
            mmap_store.populate()?;
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        clear_disk_cache(&self.vectors_path)?;
        clear_disk_cache(&self.deleted_path)?;
        Ok(())
    }
}

pub fn open_memmap_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_memmap_vector_storage_with_async_io_impl::<VectorElementType>(
        path,
        dim,
        distance,
        get_async_scorer(),
    )?;
    Ok(VectorStorageEnum::DenseMemmap(storage))
}

pub fn open_memmap_vector_storage_byte(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_memmap_vector_storage_with_async_io_impl(path, dim, distance, get_async_scorer())?;
    Ok(VectorStorageEnum::DenseMemmapByte(storage))
}

pub fn open_memmap_vector_storage_half(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_memmap_vector_storage_with_async_io_impl(path, dim, distance, get_async_scorer())?;
    Ok(VectorStorageEnum::DenseMemmapHalf(storage))
}

pub fn open_memmap_vector_storage_with_async_io(
    path: &Path,
    dim: usize,
    distance: Distance,
    with_async_io: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_memmap_vector_storage_with_async_io_impl::<VectorElementType>(
        path,
        dim,
        distance,
        with_async_io,
    )?;
    Ok(VectorStorageEnum::DenseMemmap(storage))
}

fn open_memmap_vector_storage_with_async_io_impl<T: PrimitiveVectorElement>(
    path: &Path,
    dim: usize,
    distance: Distance,
    with_async_io: bool,
) -> OperationResult<Box<MemmapDenseVectorStorage<T>>> {
    fs::create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_PATH);
    let deleted_path = path.join(DELETED_PATH);
    let mmap_store = MmapDenseVectors::open(&vectors_path, &deleted_path, dim, with_async_io)?;

    Ok(Box::new(MemmapDenseVectorStorage {
        vectors_path,
        deleted_path,
        mmap_store: Some(mmap_store),
        distance,
    }))
}

impl<T: PrimitiveVectorElement> MemmapDenseVectorStorage<T> {
    pub fn get_mmap_vectors(&self) -> &MmapDenseVectors<T> {
        self.mmap_store.as_ref().unwrap()
    }

    pub fn has_async_reader(&self) -> bool {
        self.mmap_store
            .as_ref()
            .map(|x| x.has_async_reader())
            .unwrap_or(false)
    }
}

impl<T: PrimitiveVectorElement> DenseVectorStorage<T> for MemmapDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.mmap_store.as_ref().unwrap().dim
    }

    fn get_dense<P: AccessPattern>(&self, key: PointOffsetType) -> &[T] {
        self.mmap_store
            .as_ref()
            .unwrap()
            .get_vector_opt::<P>(key)
            .unwrap_or_else(|| panic!("vector not found: {key}"))
    }

    fn get_dense_batch<'a>(
        &'a self,
        keys: &[PointOffsetType],
        vectors: &'a mut [MaybeUninit<&'a [T]>],
    ) -> &'a [&'a [T]] {
        let mmap_store = self.mmap_store.as_ref().unwrap();
        mmap_store.get_vectors(keys, vectors)
    }
}

impl<T: PrimitiveVectorElement> VectorStorage for MemmapDenseVectorStorage<T> {
    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        T::datatype()
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.mmap_store.as_ref().unwrap().num_vectors
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.mmap_store
            .as_ref()
            .unwrap()
            .get_vector_opt::<P>(key)
            .map(|vector| T::slice_to_float_cow(vector.into()).into())
            .expect("Vector not found")
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.mmap_store
            .as_ref()
            .unwrap()
            .get_vector_opt::<P>(key)
            .map(|vector| T::slice_to_float_cow(vector.into()).into())
    }

    fn insert_vector(
        &mut self,
        _key: PointOffsetType,
        _vector: VectorRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        panic!("Can't directly update vector in mmap storage")
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let dim = self.vector_dim();
        let start_index = self.mmap_store.as_ref().unwrap().num_vectors as PointOffsetType;
        let mut end_index = start_index;

        let with_async_io = self
            .mmap_store
            .take()
            .map(|x| x.has_async_reader())
            .unwrap_or(get_async_scorer());

        // Extend vectors file, write other vectors into it
        let mut vectors_file = BufWriter::new(open_append(&self.vectors_path)?);
        let mut deleted_ids = vec![];
        for (offset, (other_vector, other_deleted)) in other_vectors.enumerate() {
            check_process_stopped(stopped)?;
            let vector = T::slice_from_float_cow(Cow::try_from(other_vector)?);
            let raw_bites = mmap_ops::transmute_to_u8_slice(vector.as_ref());
            vectors_file.write_all(raw_bites)?;
            end_index += 1;

            // Remember deleted IDs so we can propagate deletions later
            if other_deleted {
                deleted_ids.push(start_index as PointOffsetType + offset as PointOffsetType);
            }
        }

        // Explicitly fsync file contents to ensure durability
        vectors_file.flush()?;
        vectors_file
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?
            .sync_data()?;

        // Load store with updated files
        self.mmap_store.replace(MmapDenseVectors::open(
            &self.vectors_path,
            &self.deleted_path,
            dim,
            with_async_io,
        )?);

        // Flush deleted flags into store
        // We must do that in the updated store, and cannot do it in the previous loop. That is
        // because the file backing delete storage must be resized, and for that we'd need to know
        // the exact number of vectors beforehand. When opening the store it is done automatically.
        let store = self.mmap_store.as_mut().unwrap();
        for id in deleted_ids {
            check_process_stopped(stopped)?;
            store.delete(id);
        }
        store.flusher()()?;

        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        match &self.mmap_store {
            Some(mmap_store) => {
                let mmap_flusher = mmap_store.flusher();
                Box::new(move || mmap_flusher().map_err(OperationError::from))
            }
            None => Box::new(|| Ok(())),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.vectors_path.clone(), self.deleted_path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        // Vector storage is initialized by `SegmentBuilder` during segment construction
        // and can't be changed after
        vec![self.vectors_path.clone()]
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        Ok(self.mmap_store.as_mut().unwrap().delete(key))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.mmap_store.as_ref().unwrap().is_deleted_vector(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.mmap_store.as_ref().unwrap().deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.mmap_store.as_ref().unwrap().deleted_vector_bitslice()
    }
}

/// Open a file shortly for appending
fn open_append<P: AsRef<Path>>(path: P) -> io::Result<File> {
    let path = path.as_ref().to_path_buf();
    OpenOptions::new().append(true).open(path)
}

#[cfg(test)]
mod tests {
    use std::mem::transmute;
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use common::counter::hardware_counter::HardwareCounterCell;
    use memory::mmap_ops::transmute_to_u8_slice;
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::{DenseVector, QueryVector};
    use crate::fixtures::payload_context_fixture::FixtureIdTracker;
    use crate::id_tracker::id_tracker_base::IdTracker;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::types::{PointIdType, QuantizationConfig, ScalarQuantizationConfig};
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::quantized::quantized_vectors::{
        QuantizedVectors, QuantizedVectorsStorageType,
    };
    use crate::vector_storage::{DEFAULT_STOPPED, Random, new_raw_scorer};

    #[test]
    fn test_basic_persistence() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let points = [
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let mut storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let mut borrowed_id_tracker = id_tracker.borrow_mut();

        // Assert this storage lists both the vector and deleted file
        let files = storage.files();
        for file_name in [VECTORS_PATH, DELETED_PATH] {
            files
                .iter()
                .find(|p| p.file_name().unwrap() == file_name)
                .expect("storage is missing required file");
        }

        let hw_counter = HardwareCounterCell::new();

        {
            let mut storage2 = new_volatile_dense_vector_storage(4, Distance::Dot);
            {
                storage2
                    .insert_vector(0, points[0].as_slice().into(), &hw_counter)
                    .unwrap();
                storage2
                    .insert_vector(1, points[1].as_slice().into(), &hw_counter)
                    .unwrap();
                storage2
                    .insert_vector(2, points[2].as_slice().into(), &hw_counter)
                    .unwrap();
            }
            let mut iter = (0..3).map(|i| {
                let i = i as PointOffsetType;
                let vector = storage2.get_vector::<Random>(i);
                let deleted = storage2.is_deleted_vector(i);
                (vector, deleted)
            });
            storage.update_from(&mut iter, &Default::default()).unwrap();
        }

        assert_eq!(storage.total_vector_count(), 3);

        let vector = storage.get_vector::<Random>(1).to_owned();
        let vector: DenseVector = vector.try_into().unwrap();

        assert_eq!(points[1], vector);

        borrowed_id_tracker.drop(PointIdType::NumId(2)).unwrap();

        {
            let mut storage2 = new_volatile_dense_vector_storage(4, Distance::Dot);
            {
                storage2
                    .insert_vector(3, points[3].as_slice().into(), &hw_counter)
                    .unwrap();
                storage2
                    .insert_vector(4, points[4].as_slice().into(), &hw_counter)
                    .unwrap();
            }
            let mut iter = (0..2).map(|i| {
                let i = i as PointOffsetType;
                let vector = storage2.get_vector::<Random>(i);
                let deleted = storage2.is_deleted_vector(i);
                (vector, deleted)
            });
            storage.update_from(&mut iter, &Default::default()).unwrap();
        }

        assert_eq!(storage.total_vector_count(), 5);

        let stored_ids: Vec<PointOffsetType> = borrowed_id_tracker.iter_internal().collect();

        assert_eq!(stored_ids, [0, 1, 3, 4]);

        let scorer = FilteredScorer::new_for_test(
            points[2].as_slice().into(),
            &storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        );
        let res = scorer.peek_top_all(2, &DEFAULT_STOPPED).unwrap();

        assert_eq!(res.len(), 2);

        assert_ne!(res[0].idx, 2);

        let res = scorer
            .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 2, &DEFAULT_STOPPED)
            .unwrap();

        assert_eq!(res.len(), 2);
        assert_ne!(res[0].idx, 2);
    }

    #[test]
    fn test_delete_points() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let points = [
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let delete_mask = [false, false, true, true, false];
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let mut storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();

        let hw_counter = HardwareCounterCell::new();

        {
            let mut storage2 = new_volatile_dense_vector_storage(4, Distance::Dot);
            {
                points.iter().enumerate().for_each(|(i, vec)| {
                    storage2
                        .insert_vector(i as PointOffsetType, vec.as_slice().into(), &hw_counter)
                        .unwrap();
                });
            }
            let mut iter = (0..points.len()).map(|i| {
                let i = i as PointOffsetType;
                let vector = storage2.get_vector::<Random>(i);
                let deleted = storage2.is_deleted_vector(i);
                (vector, deleted)
            });
            storage.update_from(&mut iter, &Default::default()).unwrap();
        }

        assert_eq!(storage.total_vector_count(), 5);
        assert_eq!(storage.deleted_vector_count(), 0);

        // Delete select number of points
        delete_mask
            .into_iter()
            .enumerate()
            .filter(|(_, d)| *d)
            .for_each(|(i, _)| {
                storage.delete_vector(i as PointOffsetType).unwrap();
            });
        assert_eq!(
            storage.deleted_vector_count(),
            2,
            "2 vectors must be deleted"
        );

        let vector = vec![0.0, 1.0, 1.1, 1.0];
        let query = vector.as_slice().into();
        let scorer = FilteredScorer::new_for_test(
            query,
            &storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        );

        let closest = scorer
            .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5, &DEFAULT_STOPPED)
            .unwrap();
        assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
        assert_eq!(closest[0].idx, 0);
        assert_eq!(closest[1].idx, 1);
        assert_eq!(closest[2].idx, 4);
        drop(scorer);

        // Delete 1, redelete 2
        storage.delete_vector(1 as PointOffsetType).unwrap();
        storage.delete_vector(2 as PointOffsetType).unwrap();
        assert_eq!(
            storage.deleted_vector_count(),
            3,
            "3 vectors must be deleted"
        );

        let vector = vec![1.0, 0.0, 0.0, 0.0];
        let query = vector.as_slice().into();

        let scorer = FilteredScorer::new_for_test(
            query,
            &storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        );
        let closest = scorer
            .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5, &DEFAULT_STOPPED)
            .unwrap();
        assert_eq!(closest.len(), 2, "must have 2 vectors, 3 are deleted");
        assert_eq!(closest[0].idx, 4);
        assert_eq!(closest[1].idx, 0);
        drop(scorer);

        // Delete all
        storage.delete_vector(0 as PointOffsetType).unwrap();
        storage.delete_vector(4 as PointOffsetType).unwrap();
        assert_eq!(
            storage.deleted_vector_count(),
            5,
            "all vectors must be deleted"
        );

        let vector = vec![1.0, 0.0, 0.0, 0.0];
        let query = vector.as_slice().into();
        let scorer = FilteredScorer::new_for_test(
            query,
            &storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        );
        let closest = scorer.peek_top_all(5, &DEFAULT_STOPPED).unwrap();
        assert!(closest.is_empty(), "must have no results, all deleted");
    }

    /// Test that deleted points are properly transferred when updating from other storage.
    #[test]
    fn test_update_from_delete_points() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let points = [
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let delete_mask = [false, false, true, true, false];
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let mut storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();

        let hw_counter = HardwareCounterCell::new();

        {
            let mut storage2 = new_volatile_dense_vector_storage(4, Distance::Dot);
            {
                points.iter().enumerate().for_each(|(i, vec)| {
                    storage2
                        .insert_vector(i as PointOffsetType, vec.as_slice().into(), &hw_counter)
                        .unwrap();
                    if delete_mask[i] {
                        storage2.delete_vector(i as PointOffsetType).unwrap();
                    }
                });
            }
            let mut iter = (0..points.len()).map(|i| {
                let i = i as PointOffsetType;
                let vector = storage2.get_vector::<Random>(i);
                let deleted = storage2.is_deleted_vector(i);
                (vector, deleted)
            });
            storage.update_from(&mut iter, &Default::default()).unwrap();
        }

        assert_eq!(
            storage.deleted_vector_count(),
            2,
            "2 vectors must be deleted from other storage"
        );

        let vector = vec![0.0, 1.0, 1.1, 1.0];
        let query = vector.as_slice().into();
        let scorer = FilteredScorer::new_for_test(
            query,
            &storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        );
        let closest = scorer
            .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5, &DEFAULT_STOPPED)
            .unwrap();

        drop(scorer);

        assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
        assert_eq!(closest[0].idx, 0);
        assert_eq!(closest[1].idx, 1);
        assert_eq!(closest[2].idx, 4);

        // Delete all
        storage.delete_vector(0 as PointOffsetType).unwrap();
        storage.delete_vector(1 as PointOffsetType).unwrap();
        storage.delete_vector(4 as PointOffsetType).unwrap();
        assert_eq!(
            storage.deleted_vector_count(),
            5,
            "all vectors must be deleted"
        );
    }

    #[test]
    fn test_mmap_raw_scorer() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let points = [
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let mut storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();

        let hw_counter = HardwareCounterCell::new();

        {
            let mut storage2 = new_volatile_dense_vector_storage(4, Distance::Dot);
            {
                for (i, vec) in points.iter().enumerate() {
                    storage2
                        .insert_vector(i as PointOffsetType, vec.as_slice().into(), &hw_counter)
                        .unwrap();
                }
            }
            let mut iter = (0..points.len()).map(|i| {
                let i = i as PointOffsetType;
                let vector = storage2.get_vector::<Random>(i);
                let deleted = storage2.is_deleted_vector(i);
                (vector, deleted)
            });
            storage.update_from(&mut iter, &Default::default()).unwrap();
        }

        let vector = vec![-1.0, -1.0, -1.0, -1.0];
        let query = vector.as_slice().into();

        let mut scorer = FilteredScorer::new_for_test(
            query,
            &storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        );

        let mut query_points: Vec<PointOffsetType> = vec![0, 2, 4];
        let res = scorer
            .score_points(&mut query_points, 0)
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].idx, 0);
        assert_eq!(res[1].idx, 2);
        assert_eq!(res[2].idx, 4);

        assert_eq!(res[2].score, -1.0);
    }

    #[test]
    fn test_casts() {
        let data: DenseVector = vec![0.42, 0.069, 333.1, 100500.];

        let raw_data = transmute_to_u8_slice(&data);

        eprintln!("raw_data.len() = {:#?}", raw_data.len());

        let arr: &[VectorElementType] = unsafe { transmute(raw_data) };

        let slice = &arr[0..data.len()];

        eprintln!("slice.len() = {:#?}", slice.len());

        for (idx, element) in slice.iter().enumerate() {
            println!("slice[{idx}]  = {element:?}");
        }
    }

    #[test]
    fn test_mmap_quantization() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let points = [
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let mut storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();

        let hw_counter = HardwareCounterCell::new();

        {
            let mut storage2 = new_volatile_dense_vector_storage(4, Distance::Dot);
            {
                for (i, vec) in points.iter().enumerate() {
                    storage2
                        .insert_vector(i as PointOffsetType, vec.as_slice().into(), &hw_counter)
                        .unwrap();
                }
            }
            let mut iter = (0..points.len()).map(|i| {
                let i = i as PointOffsetType;
                let vector = storage2.get_vector::<Random>(i);
                let deleted = storage2.is_deleted_vector(i);
                (vector, deleted)
            });
            storage.update_from(&mut iter, &Default::default()).unwrap();
        }

        let config: QuantizationConfig = ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into();

        let stopped = Arc::new(AtomicBool::new(false));
        let hardware_counter = HardwareCounterCell::new();
        let quantized_vectors = QuantizedVectors::create(
            &storage,
            &config,
            QuantizedVectorsStorageType::Immutable,
            dir.path(),
            1,
            &stopped,
        )
        .unwrap();

        let query: QueryVector = [0.5, 0.5, 0.5, 0.5].into();

        let scorer_quant = quantized_vectors
            .raw_scorer(query.clone(), hardware_counter)
            .unwrap();

        let scorer_orig =
            new_raw_scorer(query.clone(), &storage, HardwareCounterCell::new()).unwrap();

        for i in 0..5 {
            let quant = scorer_quant.score_point(i);
            let orig = scorer_orig.score_point(i);
            assert!((orig - quant).abs() < 0.15);

            let quant = scorer_quant.score_internal(0, i);
            let orig = scorer_orig.score_internal(0, i);
            assert!((orig - quant).abs() < 0.15);
        }

        let files = storage.files();
        let quantization_files = quantized_vectors.files();

        // test save-load
        let quantized_vectors = QuantizedVectors::load(&config, &storage, dir.path(), &stopped)
            .unwrap()
            .unwrap();
        assert_eq!(files, storage.files());
        assert_eq!(quantization_files, quantized_vectors.files());
        let hardware_counter = HardwareCounterCell::new();
        let scorer_quant = quantized_vectors
            .raw_scorer(query.clone(), hardware_counter)
            .unwrap();
        let scorer_orig = new_raw_scorer(query, &storage, HardwareCounterCell::new()).unwrap();

        for i in 0..5 {
            let quant = scorer_quant.score_point(i);
            let orig = scorer_orig.score_point(i);
            assert!((orig - quant).abs() < 0.15);

            let quant = scorer_quant.score_internal(0, i);
            let orig = scorer_orig.score_internal(0, i);
            assert!((orig - quant).abs() < 0.15);
        }
    }
}
