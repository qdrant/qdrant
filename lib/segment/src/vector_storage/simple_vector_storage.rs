use super::vector_storage::{VectorStorage, VectorMatcher};
use crate::spaces::metric::Metric;
use crate::types::{PointOffsetType, VectorElementType};
use std::collections::BinaryHeap;
use crate::vector_storage::vector_storage::{ScoredPoint, VectorCounter};

pub struct SimpleVectorStorage {
    dim: usize,
    vectors: Vec<Vec<VectorElementType>>,
    metric: Box<dyn Metric<VectorElementType>>,
}

impl SimpleVectorStorage {
    pub fn new(metric: Box<dyn Metric<VectorElementType>>, dim: usize) -> SimpleVectorStorage {
        return SimpleVectorStorage {
            dim,
            vectors: Vec::new(),
            metric,
        };
    }
}

fn peek_top_scores(scores: Vec<ScoredPoint>, top: usize) -> Vec<ScoredPoint> {
    if top == 0 {
        return scores;
    }
    let mut heap = BinaryHeap::from(scores);
    let mut res: Vec<ScoredPoint> = vec![];
    for _ in 0..top {
        match heap.pop() {
            Some(score_point) => res.push(score_point),
            None => break
        }
    }
    return res;
}

impl VectorStorage for SimpleVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        let vec = self.vectors.get(key)?.clone();
        return Some(vec);
    }
    fn put_vector(&mut self, vector: &Vec<VectorElementType>) -> PointOffsetType {
        assert_eq!(self.dim, vector.len());
        self.vectors.push(vector.clone());
        return self.vectors.len() - 1;
    }
}

impl VectorCounter for SimpleVectorStorage {
    fn vector_count(&self) -> PointOffsetType {
        return self.vectors.len();
    }
}

impl VectorMatcher for SimpleVectorStorage {
    fn score_points(
        &self,
        vector: &Vec<VectorElementType>,
        points: &[PointOffsetType],
        top: usize,
    ) -> Vec<ScoredPoint> {
        let scores: Vec<ScoredPoint> = points.iter().cloned()
            .map(|point| {
                let other_vector = self.vectors.get(point).unwrap();
                ScoredPoint {
                    idx: point,
                    score: self.metric.similarity(vector, other_vector),
                }
            }).collect();
        return peek_top_scores(scores, top);
    }


    fn score_all(&self, vector: &Vec<VectorElementType>, top: usize) -> Vec<ScoredPoint> {
        let scores: Vec<ScoredPoint> = self.vectors.iter()
            .enumerate().map(|(point, other_vector)| ScoredPoint {
            idx: point,
            score: self.metric.similarity(vector, other_vector),
        }).collect();
        return peek_top_scores(scores, top);
    }
    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &[PointOffsetType],
        top: usize,
    ) -> Vec<ScoredPoint> {
        let vector = self.get_vector(point).unwrap();
        return self.score_points(&vector, points, top);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::simple::DotProductMetric;

    #[test]
    fn test_score_points() {
        let metric = Box::new(DotProductMetric {});
        let dim = 4;
        let mut storage = SimpleVectorStorage::new(metric, dim);
        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];

        let _id1 = storage.put_vector(&vec1);
        let id2 = storage.put_vector(&vec2);
        let _id3 = storage.put_vector(&vec3);
        let _id4 = storage.put_vector(&vec4);
        let id5 = storage.put_vector(&vec5);

        assert_eq!(id2, 1);
        assert_eq!(id5, 4);

        let query = vec![0.0, 1.0, 1.1, 1.0];

        let closest = storage.score_points(&query, &[0, 1, 2, 3, 4], 2);

        println!("closest = {:#?}", closest);

        match closest.get(0) {
            Some(scored_point) => assert_eq!(scored_point.idx, 2),
            None => assert!(false, "No close vector found!")
        }
    }
}