use ndarray::{Array1, Array};
use crate::types::{VectorElementType, PointOffsetType, Distance};
use crate::spaces::metric::Metric;
use std::collections::HashSet;
use crate::spaces::tools::mertic_object;
use crate::vector_storage::simple_vector_storage::SimpleRawScorer;
use crate::vector_storage::vector_storage::RawScorer;

pub struct TestRawScorerProducer {
    vectors: Vec<Array1<VectorElementType>>,
    deleted: HashSet<PointOffsetType>,
    metric: Box<dyn Metric>
}


impl TestRawScorerProducer {
    pub fn new(vectors: Vec<Vec<VectorElementType>>) -> Self {
        TestRawScorerProducer {
            vectors: vectors.into_iter().map(|v| Array::from(v)).collect(),
            deleted: Default::default(),
            metric: mertic_object(&Distance::Dot)
        }
    }

    pub fn get_raw_scorer(&self, query: Vec<VectorElementType>) -> Box<dyn RawScorer + '_> {
        let metric = mertic_object(&Distance::Dot);
        Box::new(SimpleRawScorer {
            query: Array::from(metric.preprocess(query)),
            metric: &self.metric,
            vectors: &self.vectors,
            deleted: &self.deleted,
        })
    }
}