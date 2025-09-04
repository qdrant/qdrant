use crate::operations::generalizer::Generalizer;
use crate::operations::types::CountRequestInternal;

impl Generalizer for CountRequestInternal {
    fn remove_details(&self) -> Self {
        let CountRequestInternal { filter, exact } = self;
        Self {
            filter: filter.clone(),
            exact: *exact,
        }
    }
}
