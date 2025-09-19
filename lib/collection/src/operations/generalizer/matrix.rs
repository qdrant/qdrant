use api::rest::SearchMatrixRequestInternal;

use crate::collection::distance_matrix::CollectionSearchMatrixRequest;
use crate::operations::generalizer::Generalizer;

impl Generalizer for SearchMatrixRequestInternal {
    fn remove_details(&self) -> Self {
        let SearchMatrixRequestInternal {
            filter,
            sample,
            limit,
            using,
        } = self;

        Self {
            filter: filter.clone(),
            sample: *sample,
            limit: *limit,
            using: using.clone(),
        }
    }
}

impl Generalizer for CollectionSearchMatrixRequest {
    fn remove_details(&self) -> Self {
        let CollectionSearchMatrixRequest {
            sample_size,
            limit_per_sample,
            filter,
            using,
        } = self;

        Self {
            sample_size: *sample_size,
            limit_per_sample: *limit_per_sample,
            filter: filter.clone(),
            using: using.clone(),
        }
    }
}
