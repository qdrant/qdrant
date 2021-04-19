use std::collections::HashSet;
use std::ops::Range;
use std::path::Path;

use log::debug;
use rocksdb::{DB, IteratorMode, Options};
use serde::{Deserialize, Serialize};

use crate::entry::entry_point::OperationResult;
use crate::spaces::tools::{mertic_object, peek_top_scores};
use crate::types::{Distance, PointOffsetType, VectorElementType};
use crate::vector_storage::vector_storage::{ScoredPointOffset, RawScorer};

use super::vector_storage::VectorStorage;
use std::mem::size_of;
use ndarray::{Array1, Array};
use crate::spaces::metric::Metric;

/// Since sled is used for reading only during the initialization, large read cache is not required
const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb

pub struct SimpleVectorStorage {
    dim: usize,
    vectors: Vec<Array1<VectorElementType>>,
    deleted: HashSet<PointOffsetType>,
    store: DB,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
struct StoredRecord {
    pub deleted: bool,
    pub vector: Vec<VectorElementType>,
}

struct SimpleRawScorer<'a> {
    query: Array1<VectorElementType>,
    metric: Box<dyn Metric>,
    vectors: &'a Vec<Array1<VectorElementType>>,
    deleted: &'a HashSet<PointOffsetType>,
}

impl RawScorer for SimpleRawScorer<'_> {
    fn score_points<'a>(&'a self, points: &'a mut dyn Iterator<Item=PointOffsetType>) -> Box<dyn Iterator<Item=ScoredPointOffset> + 'a> {
        let res_iter = points
            .filter(move |point| !self.deleted.contains(point))
            .map(move |point| {
                let other_vector = self.vectors.get(point as usize).unwrap();
                ScoredPointOffset {
                    idx: point,
                    score: self.metric.blas_similarity(&self.query, other_vector),
                }
            });
        Box::new(res_iter)
    }
}


impl SimpleVectorStorage {
    pub fn open(path: &Path, dim: usize) -> OperationResult<Self> {
        let mut vectors: Vec<Array1<VectorElementType>> = vec![];
        let mut deleted: HashSet<PointOffsetType> = HashSet::new();

        let mut options: Options = Options::default();
        options.set_write_buffer_size(DB_CACHE_SIZE);
        options.create_if_missing(true);

        let store = DB::open(&options, path)?;

        for (key, val) in store.iterator(IteratorMode::Start) {
            let point_id: PointOffsetType = bincode::deserialize(&key).unwrap();
            let stored_record: StoredRecord = bincode::deserialize(&val).unwrap();
            if stored_record.deleted {
                deleted.insert(point_id);
            }
            if vectors.len() <= (point_id as usize) {
                vectors.resize((point_id + 1) as usize, Array::zeros(dim))
            }
            vectors[point_id as usize].assign(&Array::from(stored_record.vector));
        }

        debug!("Segment vectors: {}", vectors.len());
        debug!("Estimated segment size {} MB", vectors.len() * dim * size_of::<VectorElementType>() / 1024 / 1024);


        return Ok(SimpleVectorStorage {
            dim,
            vectors,
            deleted,
            store,
        });
    }

    fn update_stored(&self, point_id: PointOffsetType) -> OperationResult<()> {
        let v = self.vectors.get(point_id as usize).unwrap();

        let record = StoredRecord {
            deleted: self.deleted.contains(&point_id),
            vector: v.to_vec(), // ToDo: try to reduce number of vector copies
        };
        self.store.put(
            bincode::serialize(&point_id).unwrap(),
            bincode::serialize(&record).unwrap(),
        )?;

        Ok(())
    }
}


impl VectorStorage for SimpleVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn vector_count(&self) -> usize {
        self.vectors.len() - self.deleted.len()
    }

    fn deleted_count(&self) -> usize {
        self.deleted.len()
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        if self.deleted.contains(&key) { return None; }
        let vec = self.vectors.get(key as usize)?.clone();
        return Some(vec.to_vec());
    }

    fn put_vector(&mut self, vector: &Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        assert_eq!(self.dim, vector.len());
        self.vectors.push(Array::from(vector.clone()));
        let new_id = (self.vectors.len() - 1) as PointOffsetType;
        self.update_stored(new_id)?;
        return Ok(new_id);
    }

    fn update_vector(&mut self, key: PointOffsetType, vector: &Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        self.vectors[key as usize].assign(&Array::from(vector.clone()));
        self.update_stored(key)?;
        return Ok(key);
    }

    fn update_from(&mut self, other: &dyn VectorStorage) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for id in other.iter_ids() {
            self.put_vector(&other.get_vector(id).unwrap())?;
        }
        let end_index = self.vectors.len() as PointOffsetType;
        return Ok(start_index..end_index);
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        self.deleted.insert(key);
        self.update_stored(key)?;
        Ok(())
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item=PointOffsetType> + '_> {
        let iter = (0..self.vectors.len() as PointOffsetType)
            .filter(move |id| !self.deleted.contains(id));
        return Box::new(iter);
    }

    fn flush(&self) -> OperationResult<()> {
        Ok(self.store.flush()?)
    }

    fn raw_scorer(&self, vector: &Vec<VectorElementType>, distance: &Distance) -> Box<dyn RawScorer + '_> {
        let metric = mertic_object(distance);
        let raw_scorer = SimpleRawScorer {
            query: Array::from(metric.preprocess(vector.clone())),
            metric,
            vectors: &self.vectors,
            deleted: &self.deleted,
        };

        Box::new(raw_scorer)
    }

    fn score_points(
        &self,
        vector: &Vec<VectorElementType>,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance,
    ) -> Vec<ScoredPointOffset> {
        let metric = mertic_object(distance);
        let preprocessed_vector = Array::from(metric.preprocess(vector.clone()));
        let scores: Vec<ScoredPointOffset> = points.iter()
            .cloned()
            .filter(|point| !self.deleted.contains(point))
            .map(|point| {
                let other_vector = self.vectors.get(point as usize).unwrap();
                ScoredPointOffset {
                    idx: point,
                    score: metric.blas_similarity(&preprocessed_vector, other_vector),
                }
            }).collect();
        return peek_top_scores(&scores, top, distance);
    }


    fn score_all(&self, vector: &Vec<VectorElementType>, top: usize, distance: &Distance) -> Vec<ScoredPointOffset> {
        let metric = mertic_object(distance);
        let preprocessed_vector = Array::from(metric.preprocess(vector.clone()));
        let scores: Vec<ScoredPointOffset> = self.vectors.iter()
            .enumerate()
            .filter(|(point, _)| !self.deleted.contains(&(*point as PointOffsetType)))
            .map(|(point, other_vector)| ScoredPointOffset {
                idx: point as PointOffsetType,
                score: metric.blas_similarity(&preprocessed_vector, other_vector),
            }).collect();
        return peek_top_scores(&scores, top, distance);
    }

    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance,
    ) -> Vec<ScoredPointOffset> {
        let vector = self.get_vector(point).unwrap();
        return self.score_points(&vector, points, top, distance);
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
        let mut storage = SimpleVectorStorage::open(dir.path(), dim).unwrap();
        let vec0 = vec![1.0, 0.0, 1.0, 1.0];
        let vec1 = vec![1.0, 0.0, 1.0, 0.0];
        let vec2 = vec![1.0, 1.0, 1.0, 1.0];
        let vec3 = vec![1.0, 1.0, 0.0, 1.0];
        let vec4 = vec![1.0, 0.0, 0.0, 0.0];

        let _id1 = storage.put_vector(&vec0).unwrap();
        let id2 = storage.put_vector(&vec1).unwrap();
        let _id3 = storage.put_vector(&vec2).unwrap();
        let _id4 = storage.put_vector(&vec3).unwrap();
        let id5 = storage.put_vector(&vec4).unwrap();

        assert_eq!(id2, 1);
        assert_eq!(id5, 4);

        let query = vec![0.0, 1.0, 1.1, 1.0];

        let closest = storage.score_points(
            &query,
            &[0, 1, 2, 3, 4],
            2,
            &distance,
        );

        let top_idx = match closest.get(0) {
            Some(scored_point) => {
                assert_eq!(scored_point.idx, 2);
                scored_point.idx
            }
            None => {
                assert!(false, "No close vector found!");
                0
            }
        };

        storage.delete(top_idx).unwrap();

        let closest = storage.score_points(
            &query,
            &[0, 1, 2, 3, 4],
            2,
            &distance,
        );

        let raw_scorer = storage.raw_scorer(&query, &distance);

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
            None => { assert!(false, "No close vector found!") }
        };

        let all_ids1: Vec<_> = storage.iter_ids().collect();
        let all_ids2: Vec<_> = storage.iter_ids().collect();

        assert_eq!(all_ids1, all_ids2);

        assert!(!all_ids1.contains(&top_idx))
    }
}