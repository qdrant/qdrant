use super::vector_storage::{VectorStorage, VectorMatcher};
use crate::types::{PointOffsetType, VectorElementType, Distance};
use std::collections::{HashSet};
use crate::vector_storage::vector_storage::{ScoredPointOffset};

use crate::spaces::tools::{mertic_object, peek_top_scores};
use crate::entry::entry_point::OperationResult;

pub struct SimpleVectorStorage {
    dim: usize,
    vectors: Vec<Vec<VectorElementType>>,
    deleted: HashSet<PointOffsetType>,
}

impl SimpleVectorStorage {
    pub fn new(dim: usize) -> SimpleVectorStorage {
        return SimpleVectorStorage {
            dim,
            vectors: Vec::new(),
            deleted: Default::default(),
        };
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
        return Ok(self.vectors.len() - 1);
    }

    fn commit(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn delete(&mut self, key: usize) -> OperationResult<()> {
        self.deleted.insert(key);
        Ok(())
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item=usize> + '_> {
        let iter = (0..self.vectors.len())
            .filter(move |id| !self.deleted.contains(id));
        return Box::new(iter);
    }

    fn flush(&self) -> OperationResult<usize> {
        unimplemented!()
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


    #[test]
    fn test_score_points() {
        let distance = Distance::Dot;
        let dim = 4;
        let mut storage = SimpleVectorStorage::new(dim);
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