use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use super::VectorStorageEnum;
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{check_process_stopped, OperationResult};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::spaces::tools::peek_top_largest_iterable;
use crate::types::{Distance, PointOffsetType, QuantizationConfig, ScoreType};
use crate::vector_storage::mmap_vectors::MmapVectors;
use crate::vector_storage::quantized::quantized_vectors_base::QuantizedVectors;
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorage};

fn vf_to_u8<T>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * size_of::<T>()) }
}

/// An scored iterator over search result.
/// Keeps iteration context, which allows to use this iterator in external functions safely
pub struct MemmapRawScorer<'a, TMetric: Metric> {
    query: Vec<VectorElementType>,
    metric: std::marker::PhantomData<TMetric>,
    mmap_store: &'a MmapVectors,
}

impl<TMetric> RawScorer for MemmapRawScorer<'_, TMetric>
where
    TMetric: Metric,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        let mut size: usize = 0;
        // Use `read_deleted_map` instead of `deleted` to prevent multiple locks
        let deleted_map = self.mmap_store.read_deleted_map();
        for point in points {
            if MmapVectors::check_deleted(&deleted_map, *point).unwrap_or(true) {
                continue;
            }
            let other_vector = self.mmap_store.raw_vector(*point).unwrap();
            scores[size] = ScoredPointOffset {
                idx: *point,
                score: TMetric::similarity(&self.query, other_vector),
            };

            size += 1;
            if size == scores.len() {
                return size;
            }
        }
        size
    }

    fn check_point(&self, point: PointOffsetType) -> bool {
        (point < self.mmap_store.num_vectors as PointOffsetType)
            && !self.mmap_store.deleted(point).unwrap_or(true)
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        let other_vector = self.mmap_store.raw_vector(point).unwrap();
        TMetric::similarity(&self.query, other_vector)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let vector_a = self.mmap_store.raw_vector(point_a).unwrap();
        let vector_b = self.mmap_store.raw_vector(point_b).unwrap();
        TMetric::similarity(vector_a, vector_b)
    }

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let scores = points
            .filter(|point| !self.mmap_store.deleted(*point).unwrap_or(true))
            .map(|point| {
                let other_vector = self.mmap_store.raw_vector(point).unwrap();
                ScoredPointOffset {
                    idx: point,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = self.mmap_store.iter_ids().map(|point| {
            let other_vector = self.mmap_store.raw_vector(point).unwrap();
            ScoredPointOffset {
                idx: point,
                score: TMetric::similarity(&self.query, other_vector),
            }
        });

        peek_top_largest_iterable(scores, top)
    }
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

    fn vector_count(&self) -> usize {
        self.mmap_store
            .as_ref()
            .map(|store| store.num_vectors - store.deleted_count)
            .unwrap()
    }

    fn deleted_count(&self) -> usize {
        self.mmap_store.as_ref().unwrap().deleted_count
    }

    fn total_vector_count(&self) -> usize {
        self.mmap_store.as_ref().unwrap().num_vectors
    }

    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        self.mmap_store.as_ref().and_then(|x| x.get_vector(key))
    }

    fn put_vector(&mut self, _vector: Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        panic!("Can't put vector in mmap storage")
    }

    fn insert_vector(
        &mut self,
        _key: PointOffsetType,
        _vector: Vec<VectorElementType>,
    ) -> OperationResult<()> {
        panic!("Can't directly update vector in mmap storage")
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let dim = self.vector_dim();

        let start_index = self.mmap_store.as_ref().unwrap().num_vectors as PointOffsetType;
        let mut end_index = start_index;

        self.mmap_store = None;

        {
            let mut file = OpenOptions::new()
                .read(false)
                .write(false)
                .append(true)
                .create(false)
                .open(&self.vectors_path)?;

            for id in other.iter_ids() {
                check_process_stopped(stopped)?;
                let vector = &other.get_vector(id).unwrap();
                let raw_bites = vf_to_u8(vector);
                file.write_all(raw_bites)?;
                end_index += 1;
            }

            file.flush()?;
        }
        {
            let mut file = OpenOptions::new()
                .read(false)
                .write(false)
                .append(true)
                .create(false)
                .open(&self.deleted_path)?;

            let flags: Vec<u8> = vec![0; (end_index - start_index) as usize];
            let flag_bytes = vf_to_u8(&flags);
            file.write_all(flag_bytes)?;
            file.flush()?;
        }

        self.mmap_store = Some(MmapVectors::open(
            &self.vectors_path,
            &self.deleted_path,
            dim,
        )?);

        Ok(start_index..end_index)
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        self.mmap_store.as_mut().unwrap().delete(key)
    }

    fn is_deleted(&self, key: PointOffsetType) -> bool {
        self.mmap_store
            .as_ref()
            .unwrap()
            .deleted(key)
            .unwrap_or(false)
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.mmap_store.as_ref().unwrap().iter_ids()
    }

    fn flusher(&self) -> Flusher {
        match &self.mmap_store {
            None => Box::new(|| Ok(())),
            Some(x) => x.flusher(),
        }
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
        mmap_store.load_quantization(data_path)
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![self.vectors_path.clone(), self.deleted_path.clone()];
        if let Some(Some(quantized_vectors)) =
            &self.mmap_store.as_ref().map(|x| &x.quantized_vectors)
        {
            files.extend(quantized_vectors.files())
        }
        files
    }

    fn raw_scorer(&self, vector: Vec<VectorElementType>) -> Box<dyn RawScorer + '_> {
        match self.distance {
            Distance::Cosine => Box::new(MemmapRawScorer::<CosineMetric> {
                query: CosineMetric::preprocess(&vector).unwrap_or(vector),
                metric: PhantomData,
                mmap_store: self.mmap_store.as_ref().unwrap(),
            }),
            Distance::Euclid => Box::new(MemmapRawScorer::<EuclidMetric> {
                query: EuclidMetric::preprocess(&vector).unwrap_or(vector),
                metric: PhantomData,
                mmap_store: self.mmap_store.as_ref().unwrap(),
            }),
            Distance::Dot => Box::new(MemmapRawScorer::<DotProductMetric> {
                query: DotProductMetric::preprocess(&vector).unwrap_or(vector),
                metric: PhantomData,
                mmap_store: self.mmap_store.as_ref().unwrap(),
            }),
        }
    }

    fn quantized_raw_scorer(
        &self,
        vector: &[VectorElementType],
    ) -> Option<Box<dyn RawScorer + '_>> {
        let mmap_store = self.mmap_store.as_ref().unwrap();
        if let Some(quantized_data) = &mmap_store.quantized_vectors {
            if let Some(deleted_ram) = &mmap_store.deleted_flags {
                let vector: Vec<_> = vector.to_vec();
                let preprocessed_vector =
                    self.distance.preprocess_vector(&vector).unwrap_or(vector);
                Some(quantized_data.raw_scorer(&preprocessed_vector, deleted_ram))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::transmute;

    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::types::ScalarQuantizationConfig;
    use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;

    #[test]
    fn test_basic_persistence() {
        let dist = Distance::Dot;
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let storage = open_memmap_vector_storage(dir.path(), 4, dist).unwrap();
        let mut borrowed_storage = storage.borrow_mut();

        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, dist).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                borrowed_storage2.put_vector(vec1).unwrap();
                borrowed_storage2.put_vector(vec2.clone()).unwrap();
                borrowed_storage2.put_vector(vec3.clone()).unwrap();
            }
            borrowed_storage
                .update_from(&storage2.borrow(), &Default::default())
                .unwrap();
        }

        assert_eq!(borrowed_storage.vector_count(), 3);

        let vector = borrowed_storage.get_vector(1).unwrap();

        assert_eq!(vec2, vector);

        assert_eq!(borrowed_storage.deleted_count(), 0);

        borrowed_storage.delete(2).unwrap();

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, dist).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                borrowed_storage2.put_vector(vec4).unwrap();
                borrowed_storage2.put_vector(vec5).unwrap();
            }
            borrowed_storage
                .update_from(&storage2.borrow(), &Default::default())
                .unwrap();
        }

        assert_eq!(borrowed_storage.vector_count(), 4);

        let stored_ids: Vec<PointOffsetType> = borrowed_storage.iter_ids().collect();

        assert_eq!(stored_ids, [0, 1, 3, 4]);

        let raw_scorer = borrowed_storage.raw_scorer(vec3);
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
        let storage = open_memmap_vector_storage(dir.path(), 4, dist).unwrap();
        let mut borrowed_storage = storage.borrow_mut();

        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, dist).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                borrowed_storage2.put_vector(vec1).unwrap();
                borrowed_storage2.put_vector(vec2).unwrap();
                borrowed_storage2.put_vector(vec3).unwrap();
                borrowed_storage2.put_vector(vec4).unwrap();
                borrowed_storage2.put_vector(vec5).unwrap();
            }
            borrowed_storage
                .update_from(&storage2.borrow(), &Default::default())
                .unwrap();
        }

        let query = vec![-1.0, -1.0, -1.0, -1.0];
        let query_points: Vec<PointOffsetType> = vec![0, 2, 4];

        let scorer = borrowed_storage.raw_scorer(query);

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
        let storage = open_memmap_vector_storage(dir.path(), 4, dist).unwrap();
        let mut borrowed_storage = storage.borrow_mut();

        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];

        {
            let dir2 = Builder::new().prefix("db_dir").tempdir().unwrap();
            let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
            let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, 4, dist).unwrap();
            {
                let mut borrowed_storage2 = storage2.borrow_mut();
                borrowed_storage2.put_vector(vec1).unwrap();
                borrowed_storage2.put_vector(vec2).unwrap();
                borrowed_storage2.put_vector(vec3).unwrap();
                borrowed_storage2.put_vector(vec4).unwrap();
                borrowed_storage2.put_vector(vec5).unwrap();
            }
            borrowed_storage
                .update_from(&storage2.borrow(), &Default::default())
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
            let scorer_quant = borrowed_storage.quantized_raw_scorer(&query).unwrap();
            let scorer_orig = borrowed_storage.raw_scorer(query.clone());
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

        let scorer_quant = borrowed_storage.quantized_raw_scorer(&query).unwrap();
        let scorer_orig = borrowed_storage.raw_scorer(query);

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
