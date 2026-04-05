use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent, MemoryReporter};
use crate::id_tracker::id_tracker_base::{IdTracker as _, IdTrackerEnum};

impl MemoryReporter for IdTrackerEnum {
    fn memory_usage(&self) -> ComponentMemoryUsage {
        match self {
            IdTrackerEnum::MutableIdTracker(tracker) => {
                // All mappings and versions live in RAM.
                // Files are append-only logs for persistence only.
                ComponentMemoryUsage::from_files_and_ram(
                    tracker.files(),
                    FileStorageIntent::OnDisk,
                    tracker.ram_usage_bytes() as u64,
                )
            }
            IdTrackerEnum::ImmutableIdTracker(tracker) => {
                // All mappings and versions are loaded into compressed
                // in-memory structures. Mmap files are for persistence.
                ComponentMemoryUsage::from_files_and_ram(
                    tracker.files(),
                    FileStorageIntent::OnDisk,
                    tracker.ram_usage_bytes() as u64,
                )
            }
            IdTrackerEnum::InMemoryIdTracker(tracker) => {
                // Purely in-memory, no files.
                ComponentMemoryUsage::ram_only(tracker.ram_usage_bytes() as u64)
            }
        }
    }
}
