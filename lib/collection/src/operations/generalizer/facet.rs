use api::rest::FacetRequestInternal;

use crate::operations::generalizer::Generalizer;

impl Generalizer for FacetRequestInternal {
    fn remove_details(&self) -> Self {
        let FacetRequestInternal {
            key,
            limit,
            filter,
            exact,
        } = self;

        Self {
            key: key.clone(),
            limit: *limit,
            filter: filter.clone(),
            exact: *exact,
        }
    }
}
