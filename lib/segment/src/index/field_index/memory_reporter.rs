use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent, MemoryReporter};
use crate::index::field_index::field_index_base::FieldIndex;

impl MemoryReporter for FieldIndex {
    fn memory_usage(&self) -> ComponentMemoryUsage {
        let files = self.files();
        let ram_bytes = self.ram_usage_bytes() as u64;

        // Payload indexes are either fully in RAM (Mutable/Immutable variants)
        // or on disk (Mmap variants). None of them are populated into page cache.
        // Files always represent on-disk persistence, RAM is reported separately.
        ComponentMemoryUsage::from_files_and_ram(files, FileStorageIntent::OnDisk, ram_bytes)
    }
}
