use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent, MemoryReporter};
use crate::index::field_index::field_index_base::FieldIndex;

impl MemoryReporter for FieldIndex {
    fn memory_usage(&self) -> ComponentMemoryUsage {
        let files = self.files();

        if files.is_empty() {
            // In-memory only (e.g., NullIndex), no files
            return ComponentMemoryUsage::empty();
        }

        ComponentMemoryUsage::from_files(files, FileStorageIntent::Cached)
    }
}
