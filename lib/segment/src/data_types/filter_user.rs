use crate::data_types::vectors::QueryVector;
use crate::types::{SearchParams, VectorName};

/// One search of a batch that uses a filter, described for
/// [`share_filter`](crate::entry::entry_point::ReadSegmentEntry::share_filter):
/// the arguments of the [`search_batch`](crate::entry::entry_point::ReadSegmentEntry::search_batch)
/// call it will make.
#[derive(Debug, Clone, Copy)]
pub struct FilterUser<'a> {
    pub vector_name: &'a VectorName,
    pub query_vectors: &'a [QueryVector],
    pub top: usize,
    pub params: Option<&'a SearchParams>,
}
