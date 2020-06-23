use super::vector_storage::{VectorStorage, VectorMatcher};
use crate::spaces::metric::Metric;
use ordered_float::OrderedFloat;
use std::cmp::Reverse;
use crate::types::{PointOffsetType, ScoreType};


pub struct SimpleVectorStorage<El> {
    dim: usize,
    vectors: Vec<Vec<El>>,
    metric: Box<dyn Metric<El>>,
}

impl<El: Clone> SimpleVectorStorage<El> {
    fn new(metric: Box<dyn Metric<El>>, dim: usize) ->SimpleVectorStorage<El> {
        return SimpleVectorStorage {
            dim,
            vectors: Vec::new(),
            metric
        }
    }
}


impl<El: Clone> VectorStorage<El> for SimpleVectorStorage<El> {
    fn vector_count(&self) -> PointOffsetType {
        return self.vectors.len()
    }
    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<El>> {
        let vec = self.vectors.get(key)?.clone();
        return Some(vec)
    }
    fn put_vector(&mut self, vector: &Vec<El>) -> PointOffsetType {
        assert_eq!(self.dim, vector.len());
        self.vectors.push(vector.clone());
        return self.vectors.len() - 1
    }
}

impl<El> VectorMatcher<El> for SimpleVectorStorage<El> {
    fn score_points(
        &self,
        vector: &Vec<El>,
        points: &[PointOffsetType],
        top: usize
    ) -> Vec<(PointOffsetType, ScoreType)> {
        let mut scores: Vec<(PointOffsetType, ScoreType)> = points.iter().cloned()
            .map(|point| {
                let other_vector = self.vectors.get(point).unwrap();
                (point, self.metric.similarity(vector, other_vector))
            }).collect();

        scores.sort_unstable_by_key(|(_, score)| Reverse(OrderedFloat(*score)));
        return scores.into_iter().take(top).collect()
    }


    fn score_all(&self, vector: &Vec<El>, top: usize) -> Vec<(PointOffsetType, ScoreType)> {
        todo!()
    }
    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &[PointOffsetType],
        top: usize
    ) -> Vec<(PointOffsetType, ScoreType)> {
        todo!()
    }
    
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::simple::DotProductMetric;

    #[test]
    fn test_score_points() {
        let metric = Box::new(DotProductMetric{});
        let dim = 4;
        let mut storage = SimpleVectorStorage::new(metric, dim);
        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];

        let id1 = storage.put_vector(&vec1);
        let id2 = storage.put_vector(&vec2);
        let id3 = storage.put_vector(&vec3);
        let id4 = storage.put_vector(&vec4);
        let id5 = storage.put_vector(&vec5);

        assert_eq!(id2, 1);
        assert_eq!(id5, 4);

        let query = vec![0.0, 1.0, 1.1, 1.0];

        let closest = storage.score_points(&query, &[0,1,2,3,4], 2);

        println!("closest = {:#?}", closest);

        match closest.get(0) {
            Some(&(idx, score)) => assert_eq!(idx, 2),
            None => assert!(false, "No close vector found!")
        }

    }
}