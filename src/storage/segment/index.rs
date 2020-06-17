
use crate::spaces::metric::Metric;
use crate::common::types::{Filter, PointIdType};


use super::payload_storage::PayloadStorage;
use super::vector_storage::VectorStorage;


/// Trait for vector searching
trait Index<El> {
    /// Return list of Ids with fitting
    fn search(&self, vector: &Vec<El>, filter: Option<&Filter>) -> Vec<(PointIdType, f64)>;
}

pub struct PlainIndex<'s, El> {
    vector_storage: Box<&'s dyn VectorStorage<El>>,
    payload_storage: Box<&'s dyn PayloadStorage>,
    metric: Box<&'s dyn Metric<El>>
}
