use serde_json::Value;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::types::CountRequestInternal;

impl Generalizer for CountRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let CountRequestInternal { filter, exact } = self;

        let mut result = serde_json::json!({
            "exact": exact,
        });

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        result
    }
}