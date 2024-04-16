use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
use memory::mmap_ops;

use crate::common::operation_error::{check_process_stopped, OperationResult};
use crate::common::Flusher;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::Distance;
use crate::vector_storage::common::get_async_scorer;
use crate::vector_storage::dense::mmap_dense_vectors::MmapDenseVectors;
use crate::vector_storage::{DenseVectorStorage, VectorStorage, VectorStorageEnum};

const VECTORS_PATH: &str = "matrix.dat";
const DELETED_PATH: &str = "deleted.dat";

/// Stores all dense vectors in mem-mapped file
///
/// It is not possible to insert new vectors into mem-mapped storage,
/// but possible to mark some vectors as removed
///
/// Mem-mapped storage can only be constructed from another storage
pub struct MemmapDenseVectorStorage<T: PrimitiveVectorElement> {
    vectors_path: PathBuf,
    deleted_path: PathBuf,
    mmap_store: Option<MmapDenseVectors<T>>,
    distance: Distance,
}

pub fn open_memmap_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    open_memmap_vector_storage_with_async_io(path, dim, distance, get_async_scorer())
}

pub fn open_memmap_vector_storage_with_async_io(
    path: &Path,
    dim: usize,
    distance: Distance,
    with_async_io: bool,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_PATH);
    let deleted_path = path.join(DELETED_PATH);
    let mmap_store = MmapDenseVectors::open(&vectors_path, &deleted_path, dim, with_async_io)?;

    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::DenseMemmap(Box::new(MemmapDenseVectorStorage {
            vectors_path,
            deleted_path,
            mmap_store: Some(mmap_store),
            distance,
        })),
    )))
}

impl<T: PrimitiveVectorElement> MemmapDenseVectorStorage<T> {
    pub fn prefault_mmap_pages(&self) -> Option<mmap_ops::PrefaultMmapPages> {
        Some(
            self.mmap_store
                .as_ref()?
                .prefault_mmap_pages(&self.vectors_path),
        )
    }

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

impl DenseVectorStorage<VectorElementType> for MemmapDenseVectorStorage<VectorElementType> {
    fn get_dense(&self, key: PointOffsetType) -> &[VectorElementType] {
        self.mmap_store.as_ref().unwrap().get_vector(key)
    }
}

impl<T: PrimitiveVectorElement> VectorStorage for MemmapDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.mmap_store.as_ref().unwrap().dim
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.mmap_store.as_ref().unwrap().num_vectors
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        CowVector::from(T::slice_to_float_cow(
            self.mmap_store.as_ref().unwrap().get_vector(key),
        ))
    }

    fn insert_vector(&mut self, _key: PointOffsetType, _vector: VectorRef) -> OperationResult<()> {
        panic!("Can't directly update vector in mmap storage")
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut impl Iterator<Item = PointOffsetType>,
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
        let mut vectors_file = open_append(&self.vectors_path)?;
        let mut deleted_ids = vec![];
        for id in other_ids {
            check_process_stopped(stopped)?;
            let other_vector = other.get_vector(id);
            let vector = T::from_vector_ref(other_vector.as_vec_ref())?;
            let raw_bites = mmap_ops::transmute_to_u8_slice(vector.as_ref());
            vectors_file.write_all(raw_bites)?;
            end_index += 1;

            // Remember deleted IDs so we can propagate deletions later
            if other.is_deleted_vector(id) {
                deleted_ids.push((start_index + id) as PointOffsetType);
            }
        }
        vectors_file.flush()?;
        drop(vectors_file);

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

        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        match &self.mmap_store {
            Some(mmap_store) => mmap_store.flusher(),
            None => Box::new(|| Ok(())),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.vectors_path.clone(), self.deleted_path.clone()]
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
    OpenOptions::new()
        .read(false)
        .write(false)
        .append(true)
        .create(false)
        .open(path)
}

#[cfg(test)]
mod tests {
    use std::mem::transmute;

    use common::types::ScoredPointOffset;
    use memory::mmap_ops::transmute_to_u8_slice;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::data_types::vectors::{DenseVector, QueryVector};
    use crate::fixtures::payload_context_fixture::FixtureIdTracker;
    use crate::id_tracker::IdTracker;
    use crate::types::{PointIdType, QuantizationConfig, ScalarQuantizationConfig};
    use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;
    use crate::vector_storage::new_raw_scorer;
    use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

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
        let storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let mut borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        // Assert this storage lists both the vector and deleted file
        let files = borrowed_storage.files();
        for file_name in [VECTORS_PATH, DELETED_PATH] {
            files
                .iter()
                .find(|p| p.file_name().unwrap() == file_name)
                .expect("storage is missing required file");
        }

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                4,
                Distance::Dot,
                &AtomicBool::new(false),
            )
            .unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                borrowed_storage2
                    .insert_vector(0, points[0].as_slice().into())
                    .unwrap();
                borrowed_storage2
                    .insert_vector(1, points[1].as_slice().into())
                    .unwrap();
                borrowed_storage2
                    .insert_vector(2, points[2].as_slice().into())
                    .unwrap();
            }
            borrowed_storage
                .update_from(&storage2.borrow(), &mut Box::new(0..3), &Default::default())
                .unwrap();
        }

        assert_eq!(borrowed_storage.total_vector_count(), 3);

        let vector = borrowed_storage.get_vector(1).to_owned();
        let vector: DenseVector = vector.try_into().unwrap();

        assert_eq!(points[1], vector);

        borrowed_id_tracker.drop(PointIdType::NumId(2)).unwrap();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                4,
                Distance::Dot,
                &AtomicBool::new(false),
            )
            .unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                borrowed_storage2
                    .insert_vector(3, points[3].as_slice().into())
                    .unwrap();
                borrowed_storage2
                    .insert_vector(4, points[4].as_slice().into())
                    .unwrap();
            }
            borrowed_storage
                .update_from(&storage2.borrow(), &mut Box::new(0..2), &Default::default())
                .unwrap();
        }

        assert_eq!(borrowed_storage.total_vector_count(), 5);

        let stored_ids: Vec<PointOffsetType> = borrowed_id_tracker.iter_ids().collect();

        assert_eq!(stored_ids, [0, 1, 3, 4]);

        let raw_scorer = new_raw_scorer(
            points[2].as_slice().into(),
            &borrowed_storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        )
        .unwrap();
        let res = raw_scorer.peek_top_all(2);

        assert_eq!(res.len(), 2);

        assert_ne!(res[0].idx, 2);

        let res = raw_scorer.peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 2);

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
        let storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                4,
                Distance::Dot,
                &AtomicBool::new(false),
            )
            .unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                points.iter().enumerate().for_each(|(i, vec)| {
                    borrowed_storage2
                        .insert_vector(i as PointOffsetType, vec.as_slice().into())
                        .unwrap();
                });
            }
            borrowed_storage
                .update_from(
                    &storage2.borrow(),
                    &mut Box::new(0..points.len() as u32),
                    &Default::default(),
                )
                .unwrap();
        }

        assert_eq!(borrowed_storage.total_vector_count(), 5);
        assert_eq!(borrowed_storage.deleted_vector_count(), 0);

        // Delete select number of points
        delete_mask
            .into_iter()
            .enumerate()
            .filter(|(_, d)| *d)
            .for_each(|(i, _)| {
                borrowed_storage
                    .delete_vector(i as PointOffsetType)
                    .unwrap();
            });
        assert_eq!(
            borrowed_storage.deleted_vector_count(),
            2,
            "2 vectors must be deleted"
        );

        let vector = vec![0.0, 1.0, 1.1, 1.0];
        let query = vector.as_slice().into();
        let closest = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        )
        .unwrap()
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
        assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
        assert_eq!(closest[0].idx, 0);
        assert_eq!(closest[1].idx, 1);
        assert_eq!(closest[2].idx, 4);

        // Delete 1, redelete 2
        borrowed_storage
            .delete_vector(1 as PointOffsetType)
            .unwrap();
        borrowed_storage
            .delete_vector(2 as PointOffsetType)
            .unwrap();
        assert_eq!(
            borrowed_storage.deleted_vector_count(),
            3,
            "3 vectors must be deleted"
        );

        let vector = vec![1.0, 0.0, 0.0, 0.0];
        let query = vector.as_slice().into();

        let closest = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        )
        .unwrap()
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
        assert_eq!(closest.len(), 2, "must have 2 vectors, 3 are deleted");
        assert_eq!(closest[0].idx, 4);
        assert_eq!(closest[1].idx, 0);

        // Delete all
        borrowed_storage
            .delete_vector(0 as PointOffsetType)
            .unwrap();
        borrowed_storage
            .delete_vector(4 as PointOffsetType)
            .unwrap();
        assert_eq!(
            borrowed_storage.deleted_vector_count(),
            5,
            "all vectors must be deleted"
        );

        let vector = vec![1.0, 0.0, 0.0, 0.0];
        let query = vector.as_slice().into();
        let closest = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        )
        .unwrap()
        .peek_top_all(5);
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
        let storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                4,
                Distance::Dot,
                &AtomicBool::new(false),
            )
            .unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                points.iter().enumerate().for_each(|(i, vec)| {
                    borrowed_storage2
                        .insert_vector(i as PointOffsetType, vec.as_slice().into())
                        .unwrap();
                    if delete_mask[i] {
                        borrowed_storage2
                            .delete_vector(i as PointOffsetType)
                            .unwrap();
                    }
                });
            }
            borrowed_storage
                .update_from(
                    &storage2.borrow(),
                    &mut Box::new(0..points.len() as u32),
                    &Default::default(),
                )
                .unwrap();
        }

        assert_eq!(
            borrowed_storage.deleted_vector_count(),
            2,
            "2 vectors must be deleted from other storage"
        );

        let vector = vec![0.0, 1.0, 1.1, 1.0];
        let query = vector.as_slice().into();
        let closest = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        )
        .unwrap()
        .peek_top_iter(&mut [0, 1, 2, 3, 4].iter().cloned(), 5);
        assert_eq!(closest.len(), 3, "must have 3 vectors, 2 are deleted");
        assert_eq!(closest[0].idx, 0);
        assert_eq!(closest[1].idx, 1);
        assert_eq!(closest[2].idx, 4);

        // Delete all
        borrowed_storage
            .delete_vector(0 as PointOffsetType)
            .unwrap();
        borrowed_storage
            .delete_vector(1 as PointOffsetType)
            .unwrap();
        borrowed_storage
            .delete_vector(4 as PointOffsetType)
            .unwrap();
        assert_eq!(
            borrowed_storage.deleted_vector_count(),
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
        let storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                4,
                Distance::Dot,
                &AtomicBool::new(false),
            )
            .unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                for (i, vec) in points.iter().enumerate() {
                    borrowed_storage2
                        .insert_vector(i as PointOffsetType, vec.as_slice().into())
                        .unwrap();
                }
            }
            borrowed_storage
                .update_from(
                    &storage2.borrow(),
                    &mut Box::new(0..points.len() as PointOffsetType),
                    &Default::default(),
                )
                .unwrap();
        }

        let vector = vec![-1.0, -1.0, -1.0, -1.0];
        let query = vector.as_slice().into();
        let query_points: Vec<PointOffsetType> = vec![0, 2, 4];

        let scorer = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        )
        .unwrap();

        let mut res = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
        let res_count = scorer.score_points(&query_points, &mut res);
        res.resize(res_count, ScoredPointOffset { idx: 0, score: 0. });

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
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let storage = open_memmap_vector_storage(dir.path(), 4, Distance::Dot).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                4,
                Distance::Dot,
                &AtomicBool::new(false),
            )
            .unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                for (i, vec) in points.iter().enumerate() {
                    borrowed_storage2
                        .insert_vector(i as PointOffsetType, vec.as_slice().into())
                        .unwrap();
                }
            }
            borrowed_storage
                .update_from(
                    &storage2.borrow(),
                    &mut Box::new(0..points.len() as PointOffsetType),
                    &Default::default(),
                )
                .unwrap();
        }

        let config: QuantizationConfig = ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into();

        let stopped = Arc::new(AtomicBool::new(false));
        let quantized_vectors =
            QuantizedVectors::create(&borrowed_storage, &config, dir.path(), 1, &stopped).unwrap();

        let query: QueryVector = [0.5, 0.5, 0.5, 0.5].into();

        let scorer_quant = quantized_vectors
            .raw_scorer(
                query.clone(),
                borrowed_id_tracker.deleted_point_bitslice(),
                borrowed_storage.deleted_vector_bitslice(),
                &stopped,
            )
            .unwrap();
        let scorer_orig = new_raw_scorer(
            query.clone(),
            &borrowed_storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        )
        .unwrap();
        for i in 0..5 {
            let quant = scorer_quant.score_point(i);
            let orig = scorer_orig.score_point(i);
            assert!((orig - quant).abs() < 0.15);

            let quant = scorer_quant.score_internal(0, i);
            let orig = scorer_orig.score_internal(0, i);
            assert!((orig - quant).abs() < 0.15);
        }
        let files = borrowed_storage.files();
        let quantization_files = quantized_vectors.files();

        // test save-load
        let quantized_vectors = QuantizedVectors::load(&borrowed_storage, dir.path()).unwrap();
        assert_eq!(files, borrowed_storage.files());
        assert_eq!(quantization_files, quantized_vectors.files());

        let scorer_quant = quantized_vectors
            .raw_scorer(
                query.clone(),
                borrowed_id_tracker.deleted_point_bitslice(),
                borrowed_storage.deleted_vector_bitslice(),
                &stopped,
            )
            .unwrap();
        let scorer_orig = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_point_bitslice(),
        )
        .unwrap();

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
