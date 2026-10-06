use common::counter::hardware_counter::HardwareCounterCell;

use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent, MemoryReporter};
use crate::payload_storage::payload_storage_base::{PayloadStorage as _, PayloadStorageRead as _};
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;

impl MemoryReporter for PayloadStorageEnum {
    #[cfg_attr(not(feature = "testing"), expect(unused_variables))]
    fn memory_usage(&self, hw_counter: &HardwareCounterCell) -> ComponentMemoryUsage {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemory(s) => {
                // Purely in-memory, no files. Approximate RAM from storage size.
                ComponentMemoryUsage::ram_only(
                    s.get_storage_size_bytes(hw_counter).unwrap_or(0) as u64
                )
            }
            PayloadStorageEnum::Mmap(s) => {
                let intent = if s.is_on_disk() {
                    FileStorageIntent::OnDisk
                } else {
                    FileStorageIntent::Cached
                };

                ComponentMemoryUsage::from_files_and_ram(
                    s.files(),
                    intent,
                    s.ram_usage_bytes() as u64,
                )
            }
            #[cfg(target_os = "linux")]
            PayloadStorageEnum::IoUring(s) => {
                debug_assert!(s.is_on_disk());
                ComponentMemoryUsage::from_files_and_ram(
                    s.files(),
                    FileStorageIntent::OnDisk,
                    s.ram_usage_bytes() as u64,
                )
            }
        }
    }
}
