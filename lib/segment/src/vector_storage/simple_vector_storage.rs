use std::ops::Range;
use std::path::Path;

use log::debug;
use rocksdb::{IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};

use crate::entry::entry_point::OperationResult;
use crate::spaces::tools::peek_top_scores_iterable;
use crate::types::{Distance, PointOffsetType, ScoreType, VectorElementType};
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorageSS};

use super::vector_storage_base::VectorStorage;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use atomic_refcell::AtomicRefCell;
use bit_vec::BitVec;
use std::mem::size_of;
use std::sync::Arc;

/// Since sled is used for reading only during the initialization, large read cache is not required
const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleVectorStorage<TMetric: Metric> {
    dim: usize,
    metric: TMetric,
    vectors: Vec<Vec<VectorElementType>>,
    deleted: BitVec,
    deleted_count: usize,
    store: DB,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct StoredRecord {
    pub deleted: bool,
    pub vector: Vec<VectorElementType>,
}

pub struct SimpleRawScorer<'a, TMetric: Metric> {
    pub query: Vec<VectorElementType>,
    pub metric: &'a TMetric,
    pub vectors: &'a Vec<Vec<VectorElementType>>,
    pub deleted: &'a BitVec,
}

impl<TMetric> RawScorer for SimpleRawScorer<'_, TMetric>
where
    TMetric: Metric,
{
    fn score_points<'a>(
        &'a self,
        points: &'a mut dyn Iterator<Item = PointOffsetType>,
    ) -> Box<dyn Iterator<Item = ScoredPointOffset> + 'a> {
        let res_iter = points
            .filter(move |point| !self.deleted[*point as usize])
            .map(move |point| {
                let other_vector = self.vectors.get(point as usize).unwrap();
                ScoredPointOffset {
                    idx: point,
                    score: self.metric.similarity(&self.query, other_vector),
                }
            });
        Box::new(res_iter)
    }

    fn check_point(&self, point: PointOffsetType) -> bool {
        (point < self.vectors.len() as PointOffsetType) && !self.deleted[point as usize]
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        let other_vector = &self.vectors[point as usize];
        self.metric.similarity(&self.query, other_vector)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let vector_a = &self.vectors[point_a as usize];
        let vector_b = &self.vectors[point_b as usize];
        self.metric.similarity(vector_a, vector_b)
    }
}

pub fn open_simple_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageSS>>> {
    let mut vectors: Vec<Vec<VectorElementType>> = vec![];
    let mut deleted = BitVec::new();
    let mut deleted_count = 0;

    let mut options: Options = Options::default();
    options.set_write_buffer_size(DB_CACHE_SIZE);
    options.create_if_missing(true);

    let store = DB::open(&options, path)?;

    for (key, val) in store.iterator(IteratorMode::Start) {
        let point_id: PointOffsetType = bincode::deserialize(&key).unwrap();
        let stored_record: StoredRecord = bincode::deserialize(&val).unwrap();
        if stored_record.deleted {
            deleted_count += 1;
        }

        if vectors.len() <= (point_id as usize) {
            vectors.resize((point_id + 1) as usize, vec![0 as f32; dim]);
        }
        while deleted.len() <= (point_id as usize) {
            deleted.push(false);
        }

        deleted.set(point_id as usize, stored_record.deleted);
        vectors[point_id as usize] = stored_record.vector;
    }

    debug!("Segment vectors: {}", vectors.len());
    debug!(
        "Estimated segment size {} MB",
        vectors.len() * dim * size_of::<VectorElementType>() / 1024 / 1024
    );

    match distance {
        Distance::Cosine => Ok(Arc::new(AtomicRefCell::new(SimpleVectorStorage {
            dim,
            metric: CosineMetric {},
            vectors,
            deleted,
            deleted_count,
            store,
        }))),
        Distance::Euclid => Ok(Arc::new(AtomicRefCell::new(SimpleVectorStorage {
            dim,
            metric: EuclidMetric {},
            vectors,
            deleted,
            deleted_count,
            store,
        }))),
        Distance::Dot => Ok(Arc::new(AtomicRefCell::new(SimpleVectorStorage {
            dim,
            metric: DotProductMetric {},
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
        let v = self.vectors.get(point_id as usize).unwrap();

        let record = StoredRecord {
            deleted: self.deleted[point_id as usize],
            vector: v.to_vec(), // ToDo: try to reduce number of vector copies
        };
        self.store.put(
            bincode::serialize(&point_id).unwrap(),
            bincode::serialize(&record).unwrap(),
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
        if self.deleted.get(key as usize).unwrap_or(true) {
            return None;
        }
        let vec = self.vectors.get(key as usize)?.clone();
        Some(vec.to_vec())
    }

    fn put_vector(&mut self, vector: Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        assert_eq!(self.dim, vector.len());
        self.vectors.push(vector);
        self.deleted.push(false);
        let new_id = (self.vectors.len() - 1) as PointOffsetType;
        self.update_stored(new_id)?;
        Ok(new_id)
    }

    fn update_vector(
        &mut self,
        key: PointOffsetType,
        vector: Vec<VectorElementType>,
    ) -> OperationResult<PointOffsetType> {
        self.vectors[key as usize] = vector;
        self.update_stored(key)?;
        Ok(key)
    }

    fn update_from(&mut self, other: &VectorStorageSS) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for id in other.iter_ids() {
            let other_vector = other.get_vector(id).unwrap();
            // Do not perform preprocessing - vectors should be already processed
            self.deleted.push(false);
            self.vectors.push(other_vector);
            let new_id = (self.vectors.len() - 1) as PointOffsetType;
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
        Ok(self.store.flush()?)
    }

    fn raw_scorer(&self, vector: Vec<VectorElementType>) -> Box<dyn RawScorer + '_> {
        Box::new(SimpleRawScorer {
            query: self.metric.preprocess(&vector).unwrap_or(vector),
            metric: &self.metric,
            vectors: &self.vectors,
            deleted: &self.deleted,
        })
    }

    fn raw_scorer_internal(&self, point_id: PointOffsetType) -> Box<dyn RawScorer + '_> {
        Box::new(SimpleRawScorer {
            query: self.vectors[point_id as usize].clone(),
            metric: &self.metric,
            vectors: &self.vectors,
            deleted: &self.deleted,
        })
    }

    fn score_points(
        &self,
        vector: &[VectorElementType],
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let preprocessed_vector = self
            .metric
            .preprocess(vector)
            .unwrap_or_else(|| vector.to_owned());
        let scores = points
            .filter(|point| !self.deleted[*point as usize])
            .map(|point| {
                let other_vector = self.vectors.get(point as usize).unwrap();
                ScoredPointOffset {
                    idx: point,
                    score: self.metric.similarity(&preprocessed_vector, other_vector),
                }
            });
        peek_top_scores_iterable(scores, top)
    }

    fn score_all(&self, vector: &[VectorElementType], top: usize) -> Vec<ScoredPointOffset> {
        let preprocessed_vector = self
            .metric
            .preprocess(vector)
            .unwrap_or_else(|| vector.to_owned());
        let scores = self
            .vectors
            .iter()
            .enumerate()
            .filter(|(point, _)| !self.deleted[*point])
            .map(|(point, other_vector)| ScoredPointOffset {
                idx: point as PointOffsetType,
                score: self.metric.similarity(&preprocessed_vector, other_vector),
            });
        peek_top_scores_iterable(scores, top)
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
    use itertools::Itertools;

    #[test]
    fn test_score_points() {
        let dir = TempDir::new("storage_dir").unwrap();
        let distance = Distance::Dot;
        let dim = 4;
        let storage = open_simple_vector_storage(dir.path(), dim, distance).unwrap();
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
        let mut query_points1 = query_points.iter().cloned();
        let mut query_points2 = query_points.iter().cloned();

        let raw_res1 = raw_scorer.score_points(&mut query_points1).collect_vec();
        let raw_res2 = raw_scorer.score_points(&mut query_points2).collect_vec();

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
