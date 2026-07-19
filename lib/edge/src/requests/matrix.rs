use segment::types::{Filter, VectorNameBuf};

/// A single-shard distance-matrix request: sample `sample_size` random points that
/// carry the `using` vector, then find each sample's `limit_per_sample` nearest
/// neighbours restricted to the sampled set.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchMatrixRequest {
    /// How many random points to sample.
    pub sample_size: usize,
    /// How many nearest neighbours to return per sampled point.
    pub limit_per_sample: usize,
    /// Look only for points which satisfy these conditions.
    pub filter: Option<Filter>,
    /// Name of the vector the sampled points are compared by.
    pub using: VectorNameBuf,
}

impl SearchMatrixRequest {
    pub fn new(sample_size: usize, limit_per_sample: usize, using: VectorNameBuf) -> Self {
        Self {
            sample_size,
            limit_per_sample,
            filter: None,
            using,
        }
    }
}
