use crate::types::{Filter, PointOffsetType, ScoreType};
use crate::vector_storage::vector_storage::{VectorMatcher, ScoredPoint};
use crate::payload_storage::payload_storage::PayloadStorage;

/// Trait for vector searching
pub trait Index<El> {
    /// Return list of Ids with fitting
    fn search(&self, vector: &Vec<El>, filter: Option<&Filter>, top: usize) -> Vec<(PointOffsetType, ScoreType)>;
}

pub struct PlainIndex<'s, El> {
    vector_matcher: Box<&'s dyn VectorMatcher<El>>,
    payload_storage: Box<&'s dyn PayloadStorage>
}

impl<'s, El> PlainIndex<'s, El> {
    fn new(
        vector_matcher: &'s dyn VectorMatcher<El>,
        payload_storage: &'s dyn PayloadStorage,
    ) -> PlainIndex<'s, El>{
        return PlainIndex {
            vector_matcher: Box::new(vector_matcher),
            payload_storage: Box::new(payload_storage),
        }
    }
}


impl<'s, El> Index<El> for PlainIndex<'s, El> {
    fn search(&self, vector: &Vec<El>, filter: Option<&Filter>, top: usize) -> Vec<(PointOffsetType, ScoreType)> {
        match filter {
            Some(filter) => {
                let filtered_ids = self.payload_storage.query_points(filter);
                self.vector_matcher.score_points(vector, &filtered_ids, 0)
            },
            None => self.vector_matcher.score_all(vector, top)
        } .iter().map(ScoredPoint::to_tuple).collect()
    }
}