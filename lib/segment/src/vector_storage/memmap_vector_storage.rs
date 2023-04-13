use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Write};
use std::mem::size_of;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use super::quantized::quantized_vectors_base::QuantizedVectorsStorage;
use super::VectorStorageEnum;
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{check_process_stopped, OperationResult};
use crate::types::{Distance, PointOffsetType, QuantizationConfig};
use crate::vector_storage::mmap_vectors::MmapVectors;
use crate::vector_storage::quantized::quantized_vectors_base::QuantizedVectors;
use crate::vector_storage::VectorStorage;

fn vf_to_u8<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * size_of::<T>()) }
}

/// Stores all vectors in mem-mapped file
///
/// It is not possible to insert new vectors into mem-mapped storage,
/// but possible to mark some vectors as removed
///
/// Mem-mapped storage can only be constructed from another storage
pub struct MemmapVectorStorage {
    vectors_path: PathBuf,
    deleted_path: PathBuf,
    mmap_store: Option<MmapVectors>,
    distance: Distance,
}

pub fn open_memmap_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    create_dir_all(path)?;

    let vectors_path = path.join("matrix.dat");
    let deleted_path = path.join("deleted.dat");
    let mmap_store = MmapVectors::open(&vectors_path, &deleted_path, dim)?;

    Ok(Arc::new(AtomicRefCell::new(VectorStorageEnum::Memmap(
        Box::new(MemmapVectorStorage {
            vectors_path,
            deleted_path,
            mmap_store: Some(mmap_store),
            distance,
        }),
    ))))
}

impl VectorStorage for MemmapVectorStorage {
    fn vector_dim(&self) -> usize {
        self.mmap_store.as_ref().unwrap().dim
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn total_vector_count(&self) -> usize {
        self.mmap_store.as_ref().unwrap().num_vectors
    }

    fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType] {
        self.mmap_store.as_ref().unwrap().get_vector(key)
    }

    fn insert_vector(
        &mut self,
        _key: PointOffsetType,
        _vector: &[VectorElementType],
    ) -> OperationResult<()> {
        panic!("Can't directly update vector in mmap storage")
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let dim = self.vector_dim();

        let start_index = self.mmap_store.as_ref().unwrap().num_vectors as PointOffsetType;
        let mut end_index = start_index;

        self.mmap_store.take();

        // Extend vectors file, write other vectors to it
        let mut vectors_file = open_append(&self.vectors_path)?;
        for id in other_ids {
            check_process_stopped(stopped)?;
            let vector = other.get_vector(id);
            let raw_bites = vf_to_u8(vector);
            vectors_file.write_all(raw_bites)?;
            end_index += 1;
        }
        vectors_file.flush()?;
        drop(vectors_file);

        // Extend deleted file, append same number of deleted flags to it
        let mut deleted_file = open_append(&self.deleted_path)?;
        deleted_file.write_all(&vec![0; (end_index - start_index) as usize])?;
        deleted_file.flush()?;
        drop(deleted_file);

        self.mmap_store.replace(MmapVectors::open(
            &self.vectors_path,
            &self.deleted_path,
            dim,
        )?);

        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    fn quantize(
        &mut self,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()> {
        let mmap_store = self.mmap_store.as_mut().unwrap();
        mmap_store.quantize(self.distance, data_path, quantization_config)
    }

    fn load_quantization(&mut self, data_path: &Path) -> OperationResult<()> {
        let mmap_store = self.mmap_store.as_mut().unwrap();
        mmap_store.load_quantization(data_path, self.distance)
    }

    fn quantized_storage(&self) -> Option<&QuantizedVectorsStorage> {
        let mmap_store = self.mmap_store.as_ref().unwrap();
        mmap_store.quantized_vectors.as_ref()
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![self.vectors_path.clone()];
        if let Some(Some(quantized_vectors)) =
            &self.mmap_store.as_ref().map(|x| &x.quantized_vectors)
        {
            files.extend(quantized_vectors.files())
        }
        files
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        self.mmap_store.as_mut().unwrap().delete(key)
    }

    fn is_deleted(&self, key: PointOffsetType) -> bool {
        self.mmap_store.as_ref().unwrap().is_deleted(key)
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

    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::payload_context_fixture::FixtureIdTracker;
    use crate::id_tracker::IdTracker;
    use crate::types::{PointIdType, ScalarQuantizationConfig};
    use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;
    use crate::vector_storage::{new_raw_scorer, ScoredPointOffset};

    #[test]
    fn test_basic_persistence() {
        let dist = Distance::Dot;
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let points = vec![
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let storage = open_memmap_vector_storage(dir.path(), 4, dist).unwrap();
        let mut borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, dist).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                borrowed_storage2.insert_vector(0, &points[0]).unwrap();
                borrowed_storage2.insert_vector(1, &points[1]).unwrap();
                borrowed_storage2.insert_vector(2, &points[2]).unwrap();
            }
            borrowed_storage
                .update_from(&storage2.borrow(), &mut Box::new(0..3), &Default::default())
                .unwrap();
        }

        assert_eq!(borrowed_storage.total_vector_count(), 3);

        let vector = borrowed_storage.get_vector(1).to_vec();

        assert_eq!(points[1], vector);

        borrowed_id_tracker.drop(PointIdType::NumId(2)).unwrap();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, dist).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                borrowed_storage2.insert_vector(3, &points[3]).unwrap();
                borrowed_storage2.insert_vector(4, &points[4]).unwrap();
            }
            borrowed_storage
                .update_from(&storage2.borrow(), &mut Box::new(0..2), &Default::default())
                .unwrap();
        }

        assert_eq!(borrowed_storage.total_vector_count(), 5);

        let stored_ids: Vec<PointOffsetType> = borrowed_id_tracker.iter_ids().collect();

        assert_eq!(stored_ids, [0, 1, 3, 4]);

        let raw_scorer = new_raw_scorer(
            points[2].clone(),
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        );
        let res = raw_scorer.peek_top_all(2);

        assert_eq!(res.len(), 2);

        assert_ne!(res[0].idx, 2);

        let res = raw_scorer.peek_top_iter(&mut vec![0, 1, 2, 3, 4].iter().cloned(), 2);

        assert_eq!(res.len(), 2);
        assert_ne!(res[0].idx, 2);
    }

    #[test]
    fn test_mmap_raw_scorer() {
        let dist = Distance::Dot;
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let points = vec![
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let storage = open_memmap_vector_storage(dir.path(), 4, dist).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, dist).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                for (i, vec) in points.iter().enumerate() {
                    borrowed_storage2
                        .insert_vector(i as PointOffsetType, vec)
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

        let query = vec![-1.0, -1.0, -1.0, -1.0];
        let query_points: Vec<PointOffsetType> = vec![0, 2, 4];

        let scorer = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        );

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
        let data: Vec<VectorElementType> = vec![0.42, 0.069, 333.1, 100500.];

        let raw_data = vf_to_u8(&data);

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
        let dist = Distance::Dot;
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let points = vec![
            vec![1.0, 0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0, 0.0],
            vec![1.0, 1.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0, 1.0],
            vec![1.0, 0.0, 0.0, 0.0],
        ];
        let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(points.len())));
        let storage = open_memmap_vector_storage(dir.path(), 4, dist).unwrap();
        let borrowed_id_tracker = id_tracker.borrow_mut();
        let mut borrowed_storage = storage.borrow_mut();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, dist).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                for (i, vec) in points.iter().enumerate() {
                    borrowed_storage2
                        .insert_vector(i as PointOffsetType, vec)
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

        borrowed_storage.quantize(dir.path(), &config).unwrap();

        let query = vec![0.5, 0.5, 0.5, 0.5];

        {
            let scorer_quant = borrowed_storage
                .quantized_storage()
                .unwrap()
                .raw_scorer(&query, borrowed_id_tracker.deleted_bitvec());
            let scorer_orig = new_raw_scorer(
                query.clone(),
                &borrowed_storage,
                borrowed_id_tracker.deleted_bitvec(),
            );
            for i in 0..5 {
                let quant = scorer_quant.score_point(i);
                let orig = scorer_orig.score_point(i);
                assert!((orig - quant).abs() < 0.15);

                let quant = scorer_quant.score_internal(0, i);
                let orig = scorer_orig.score_internal(0, i);
                assert!((orig - quant).abs() < 0.15);
            }
        }

        // test save-load
        borrowed_storage.load_quantization(dir.path()).unwrap();

        let scorer_quant = borrowed_storage
            .quantized_storage()
            .unwrap()
            .raw_scorer(&query, borrowed_id_tracker.deleted_bitvec());
        let scorer_orig = new_raw_scorer(
            query,
            &borrowed_storage,
            borrowed_id_tracker.deleted_bitvec(),
        );

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
