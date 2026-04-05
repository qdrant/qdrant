use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent, MemoryReporter};
use crate::payload_storage::payload_storage_base::PayloadStorage as _;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;

impl MemoryReporter for PayloadStorageEnum {
    fn memory_usage(&self) -> ComponentMemoryUsage {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                // Purely in-memory, no files. Approximate RAM from storage size.
                ComponentMemoryUsage::ram_only(s.get_storage_size_bytes().unwrap_or(0) as u64)
            }
            PayloadStorageEnum::MmapPayloadStorage(s) => {
                let intent = if s.is_on_disk() {
                    FileStorageIntent::OnDisk
                } else {
                    FileStorageIntent::Cached
                };

                ComponentMemoryUsage::from_files(s.files(), intent)
            }
        }
    }
}
