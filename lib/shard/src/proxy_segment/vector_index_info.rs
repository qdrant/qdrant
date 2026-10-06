use std::collections::HashMap;

use segment::entry::{VectorIndexInfo, VectorIndexInfoProvider};
use segment::types::VectorNameBuf;

use super::ProxySegment;

#[cfg(test)]
mod tests;

impl VectorIndexInfoProvider for ProxySegment {
    fn vector_index_info(&self) -> HashMap<VectorNameBuf, VectorIndexInfo> {
        let mut indexes = self.wrapped_segment.get_read().read().vector_index_info();
        indexes.retain(|name, _| {
            !self
                .pending_changes
                .vector_name_changes()
                .is_wrapped_data_stale(name)
        });
        indexes
    }
}
