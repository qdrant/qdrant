use std::marker::PhantomData;
use std::ops::Range;

use log::debug;
use rocksdb::{IteratorMode, DB};
use serde::{Deserialize, Serialize};

use crate::common::rocksdb_operations::{db_write_options, DB_VECTOR_CF};
use crate::entry::entry_point::OperationResult;
use crate::spaces::tools::peek_top_largest_scores_iterable;
use crate::types::{Distance, PointOffsetType, ScoreType, VectorElementType};
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorageSS};

use super::chunked_vectors::ChunkedVectors;
use super::vector_storage_base::VectorStorage;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use atomic_refcell::AtomicRefCell;
use bitvec::prelude::BitVec;
use std::mem::size_of;
use std::sync::Arc;

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleVectorStorage<TMetric: Metric> {
    dim: usize,
    metric: PhantomData<TMetric>,
    vectors: ChunkedVectors,
    deleted: BitVec,
    deleted_count: usize,
    store: Arc<AtomicRefCell<DB>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct StoredRecord {
    pub deleted: bool,
    pub vector: Vec<VectorElementType>,
}

pub struct SimpleRawScorer<'a, TMetric: Metric> {
    pub query: Vec<VectorElementType>,
    pub vectors: &'a ChunkedVectors,
    pub deleted: &'a BitVec,
    pub metric: PhantomData<TMetric>,
}

impl<TMetric> RawScorer for SimpleRawScorer<'_, TMetric>
where
    TMetric: Metric,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if self.deleted[point_id as usize] {
                continue;
            }
            let other_vector = self.vectors.get(point_id);
            scores[size] = ScoredPointOffset {
                idx: point_id,
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
        (point as usize) < self.vectors.len() && !self.deleted[point as usize]
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        let other_vector = self.vectors.get(point);
        TMetric::similarity(&self.query, other_vector)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let vector_a = self.vectors.get(point_a);
        let vector_b = self.vectors.get(point_b);
        TMetric::similarity(vector_a, vector_b)
    }
}

pub fn open_simple_vector_storage(
    store: Arc<AtomicRefCell<DB>>,
    dim: usize,
    distance: Distance,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageSS>>> {
    let mut vectors = ChunkedVectors::new(dim);
    let mut deleted = BitVec::new();
    let mut deleted_count = 0;

    {
        let store_ref = store.borrow();
        let cf_handle = store_ref.cf_handle(DB_VECTOR_CF).unwrap();
        for (key, val) in store_ref.iterator_cf(cf_handle, IteratorMode::Start) {
            let point_id: PointOffsetType = bincode::deserialize(&key).unwrap();
            let stored_record: StoredRecord = bincode::deserialize(&val).unwrap();
            if stored_record.deleted {
                deleted_count += 1;
            }

            while deleted.len() <= (point_id as usize) {
                deleted.push(false);
            }

            deleted.set(point_id as usize, stored_record.deleted);
            vectors.insert(point_id, &stored_record.vector);
        }
    }

    debug!("Segment vectors: {}", vectors.len());
    debug!(
        "Estimated segment size {} MB",
        vectors.len() * dim * size_of::<VectorElementType>() / 1024 / 1024
    );

    match distance {
        Distance::Cosine => Ok(Arc::new(AtomicRefCell::new(SimpleVectorStorage::<
            CosineMetric,
        > {
            dim,
            metric: PhantomData,
            vectors,
            deleted,
            deleted_count,
            store,
        }))),
        Distance::Euclid => Ok(Arc::new(AtomicRefCell::new(SimpleVectorStorage::<
            EuclidMetric,
        > {
            dim,
            metric: PhantomData,
            vectors,
            deleted,
            deleted_count,
            store,
        }))),
        Distance::Dot => Ok(Arc::new(AtomicRefCell::new(SimpleVectorStorage::<
            DotProductMetric,
        > {
            dim,
            metric: PhantomData,
            vectors,
            deleted,
            deleted_count,
            store,
        }))),
    }
}

impl<TMetric> SimpleVectorStorage<TMetric>
where
    TMetric: Metric,
{
    fn update_stored(&self, point_id: PointOffsetType) -> OperationResult<()> {
        let v = self.vectors.get(point_id);

        let record = StoredRecord {
            deleted: self.deleted[point_id as usize],
            vector: v.to_vec(), // ToDo: try to reduce number of vector copies
        };

        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(DB_VECTOR_CF).unwrap();
        store_ref.put_cf_opt(
            cf_handle,
            bincode::serialize(&point_id).unwrap(),
            bincode::serialize(&record).unwrap(),
            &db_write_options(),
        )?;

        Ok(())
    }
}

impl<TMetric> VectorStorage for SimpleVectorStorage<TMetric>
where
    TMetric: Metric,
{
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn vector_count(&self) -> usize {
        self.vectors.len() - self.deleted_count
    }

    fn deleted_count(&self) -> usize {
        self.deleted_count
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        if self.deleted.get(key as usize).map(|x| *x).unwrap_or(true) {
            return None;
        }
        Some(self.vectors.get(key).to_vec())
    }

    fn put_vector(&mut self, vector: Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        assert_eq!(self.dim, vector.len());
        let new_id = self.vectors.push(&vector);
        self.deleted.push(false);
        self.update_stored(new_id)?;
        Ok(new_id)
    }

    fn update_vector(
        &mut self,
        key: PointOffsetType,
        vector: Vec<VectorElementType>,
    ) -> OperationResult<PointOffsetType> {
        self.vectors.insert(key, &vector);
        if self.deleted.len() <= (key as usize) {
            self.deleted.resize(key as usize + 1, false);
        }
        self.deleted.set(key as usize, false);
        self.update_stored(key)?;
        Ok(key)
    }

    fn update_from(&mut self, other: &VectorStorageSS) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for point_id in other.iter_ids() {
            let other_vector = other.get_vector(point_id).unwrap();
            // Do not perform preprocessing - vectors should be already processed
            self.deleted.push(false);
            let new_id = self.vectors.push(&other_vector);
            self.update_stored(new_id)?;
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        if (key as usize) >= self.deleted.len() {
            return Ok(());
        }
        if !self.deleted[key as usize] {
            self.deleted_count += 1;
        }
        self.deleted.set(key as usize, true);
        self.update_stored(key)?;
        Ok(())
    }

    fn is_deleted(&self, key: PointOffsetType) -> bool {
        self.deleted[key as usize]
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let iter = (0..self.vectors.len() as PointOffsetType)
            .filter(move |id| !self.deleted[*id as usize]);
        Box::new(iter)
    }

    fn flush(&self) -> OperationResult<()> {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(DB_VECTOR_CF).unwrap();
        Ok(store_ref.flush_cf(cf_handle)?)
    }

    fn raw_scorer(&self, vector: Vec<VectorElementType>) -> Box<dyn RawScorer + '_> {
        Box::new(SimpleRawScorer::<TMetric> {
            query: TMetric::preprocess(&vector).unwrap_or(vector),
            vectors: &self.vectors,
            deleted: &self.deleted,
            metric: PhantomData,
        })
    }

    fn raw_scorer_internal(&self, point_id: PointOffsetType) -> Box<dyn RawScorer + '_> {
        Box::new(SimpleRawScorer::<TMetric> {
            query: self.vectors.get(point_id).to_vec(),
            vectors: &self.vectors,
            deleted: &self.deleted,
            metric: PhantomData,
        })
    }

    fn score_points(
        &self,
        vector: &[VectorElementType],
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let preprocessed_vector = TMetric::preprocess(vector).unwrap_or_else(|| vector.to_owned());
        let scores = points
            .filter(|point_id| !self.deleted[*point_id as usize])
            .map(|point_id| {
                let other_vector = self.vectors.get(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&preprocessed_vector, other_vector),
                }
            });
        peek_top_largest_scores_iterable(scores, top)
    }

    fn score_all(&self, vector: &[VectorElementType], top: usize) -> Vec<ScoredPointOffset> {
        let preprocessed_vector = TMetric::preprocess(vector).unwrap_or_else(|| vector.to_owned());

        let scores = (0..self.vectors.len())
            .filter(|point_id| !self.deleted[*point_id])
            .map(|point_id| {
                let point_id = point_id as PointOffsetType;
                let other_vector = &self.vectors.get(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&preprocessed_vector, other_vector),
                }
            });
        peek_top_largest_scores_iterable(scores, top)
    }

    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let vector = self.get_vector(point).unwrap();
        self.score_points(&vector, points, top)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::common::rocksdb_operations::open_db;

    #[test]
    fn test_score_points() {
        let dir = TempDir::new("storage_dir").unwrap();
        let db = open_db(dir.path()).unwrap();
        let distance = Distance::Dot;
        let dim = 4;
        let storage = open_simple_vector_storage(db, dim, distance).unwrap();
        let mut borrowed_storage = storage.borrow_mut();

        let vec0 = vec![1.0, 0.0, 1.0, 1.0];
        let vec1 = vec![1.0, 0.0, 1.0, 0.0];
        let vec2 = vec![1.0, 1.0, 1.0, 1.0];
        let vec3 = vec![1.0, 1.0, 0.0, 1.0];
        let vec4 = vec![1.0, 0.0, 0.0, 0.0];

        let _id1 = borrowed_storage.put_vector(vec0).unwrap();
        let id2 = borrowed_storage.put_vector(vec1).unwrap();
        let _id3 = borrowed_storage.put_vector(vec2).unwrap();
        let _id4 = borrowed_storage.put_vector(vec3).unwrap();
        let id5 = borrowed_storage.put_vector(vec4).unwrap();

        assert_eq!(id2, 1);
        assert_eq!(id5, 4);

        let query = vec![0.0, 1.0, 1.1, 1.0];

        let closest =
            borrowed_storage.score_points(&query, &mut [0, 1, 2, 3, 4].iter().cloned(), 2);

        let top_idx = match closest.get(0) {
            Some(scored_point) => {
                assert_eq!(scored_point.idx, 2);
                scored_point.idx
            }
            None => {
                panic!("No close vector found!")
            }
        };

        borrowed_storage.delete(top_idx).unwrap();

        let closest =
            borrowed_storage.score_points(&query, &mut [0, 1, 2, 3, 4].iter().cloned(), 2);

        let raw_scorer = borrowed_storage.raw_scorer(query);
        let query_points = vec![0, 1, 2, 3, 4];

        let mut raw_res1 = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
        let raw_res1_count = raw_scorer.score_points(&query_points, &mut raw_res1);
        raw_res1.resize(raw_res1_count, ScoredPointOffset { idx: 0, score: 0. });

        let mut raw_res2 = vec![ScoredPointOffset { idx: 0, score: 0. }; query_points.len()];
        let raw_res2_count = raw_scorer.score_points(&query_points, &mut raw_res2);
        raw_res2.resize(raw_res2_count, ScoredPointOffset { idx: 0, score: 0. });

        assert_eq!(raw_res1, raw_res2);

        let _top_idx = match closest.get(0) {
            Some(scored_point) => {
                assert_ne!(scored_point.idx, 2);
                assert_eq!(&raw_res1[scored_point.idx as usize], scored_point);
            }
            None => {
                panic!("No close vector found!")
            }
        };

        let all_ids1: Vec<_> = borrowed_storage.iter_ids().collect();
        let all_ids2: Vec<_> = borrowed_storage.iter_ids().collect();

        assert_eq!(all_ids1, all_ids2);

        assert!(!all_ids1.contains(&top_idx))
    }
}
