use super::vector_storage::{VectorStorage, VectorMatcher};
use crate::types::{PointOffsetType, VectorElementType, Distance};
use std::collections::{HashSet};
use crate::vector_storage::vector_storage::{ScoredPointOffset};

use crate::spaces::tools::{mertic_object, peek_top_scores};
use crate::entry::entry_point::OperationResult;
use std::ops::Range;
use std::path::Path;
use sled::Db;
use serde::{Deserialize, Serialize};

pub struct SimpleVectorStorage {
    dim: usize,
    vectors: Vec<Vec<VectorElementType>>,
    deleted: HashSet<PointOffsetType>,
    store: Db,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
struct StoredRecord {
    pub deleted: bool,
    pub vector: Vec<VectorElementType>,
}


impl SimpleVectorStorage {
    pub fn open(path: &Path, dim: usize) -> Self {
        let mut vectors: Vec<Vec<VectorElementType>> = vec![];
        let mut deleted: HashSet<PointOffsetType> = HashSet::new();

        let store = sled::open(path).unwrap();

        vectors.resize(store.len(), vec![]);

        for record in store.iter() {
            let (key, val) = record.unwrap();
            let point_id: PointOffsetType = bincode::deserialize(&key).unwrap();
            let stored_record: StoredRecord = bincode::deserialize(&val).unwrap();
            if stored_record.deleted {
                deleted.insert(point_id);
            }
            vectors.insert(point_id, stored_record.vector.clone());
        }

        return SimpleVectorStorage {
            dim,
            vectors,
            deleted,
            store,
        };
    }

    fn update_stored(&self, point_id: PointOffsetType) -> OperationResult<()> {
        let v = self.vectors.get(point_id).unwrap();

        let record = StoredRecord {
            deleted: self.deleted.contains(&point_id),
            vector: v.clone(), // ToDo: try to reduce number of vector copies
        };
        self.store.insert(
            bincode::serialize(&point_id).unwrap(),
            bincode::serialize(&record).unwrap()
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
        return self.deleted.len();
    }

    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        if self.deleted.contains(&key) { return None; }
        let vec = self.vectors.get(key)?.clone();
        return Some(vec);
    }

    fn put_vector(&mut self, vector: &Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        assert_eq!(self.dim, vector.len());
        self.vectors.push(vector.clone());
        self.update_stored(self.vectors.len() - 1)?;
        return Ok(self.vectors.len() - 1);
    }

    fn update_from(&mut self, other: &dyn VectorStorage) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len();
        for id in other.iter_ids() {
            self.put_vector(&other.get_vector(id).unwrap())?;
        }
        let end_index = self.vectors.len();
        return Ok(start_index..end_index);
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        self.deleted.insert(key);
        self.update_stored(key)?;
        Ok(())
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item=usize> + '_> {
        let iter = (0..self.vectors.len())
            .filter(move |id| !self.deleted.contains(id));
        return Box::new(iter);
    }

    fn flush(&self) -> OperationResult<usize> {
        Ok(self.store.flush()?)
    }
}

impl VectorMatcher for SimpleVectorStorage {
    fn score_points(
        &self,
        vector: &Vec<VectorElementType>,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance,
    ) -> Vec<ScoredPointOffset> {
        let metric = mertic_object(distance);

        let scores: Vec<ScoredPointOffset> = points.iter()
            .cloned()
            .filter(|point| !self.deleted.contains(point))
            .map(|point| {
                let other_vector = self.vectors.get(point).unwrap();
                ScoredPointOffset {
                    idx: point,
                    score: metric.similarity(vector, other_vector),
                }
            }).collect();
        return peek_top_scores(&scores, top, distance);
    }


    fn score_all(&self, vector: &Vec<VectorElementType>, top: usize, distance: &Distance) -> Vec<ScoredPointOffset> {
        let metric = mertic_object(distance);

        let scores: Vec<ScoredPointOffset> = self.vectors.iter()
            .enumerate()
            .filter(|(point, _)| !self.deleted.contains(point))
            .map(|(point, other_vector)| ScoredPointOffset {
                idx: point,
                score: metric.similarity(vector, other_vector),
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
    use super::*;
    use tempdir::TempDir;


    #[test]
    fn test_score_points() {

        let dir = TempDir::new("storage_dir").unwrap();
        let distance = Distance::Dot;
        let dim = 4;
        let mut storage = SimpleVectorStorage::open(dir.path(), dim);
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


        let _top_idx = match closest.get(0) {
            Some(scored_point) => {
                assert_ne!(scored_point.idx, 2);
            }
            None => { assert!(false, "No close vector found!") }
        };

        let all_ids1: Vec<_> = storage.iter_ids().collect();
        let all_ids2: Vec<_> = storage.iter_ids().collect();

        assert_eq!(all_ids1, all_ids2);

        assert!(!all_ids1.contains(&top_idx))
    }
}