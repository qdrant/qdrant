use std::path::PathBuf;

use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent, MemoryReporter};
use crate::vector_storage::vector_storage_base::{
    DenseVectorStorage as _, MultiVectorStorage as _, VectorStorage as _, VectorStorageEnum,
};

/// Build a `ComponentMemoryUsage` from files + `is_on_disk()`.
///
/// Uses `is_on_disk()` which each concrete type implements based on its
/// actual configuration (e.g., `ChunkedVectors` checks the populate flag).
fn from_files_with_on_disk(files: Vec<PathBuf>, is_on_disk: bool) -> ComponentMemoryUsage {
    let intent = if is_on_disk {
        // Files are mmap'd but not expected to be fully resident
        // (no populate, or explicitly on-disk).
        FileStorageIntent::OnDisk
    } else {
        // Files are mmap'd and populated into RAM on load,
        // expected to be fully resident.
        FileStorageIntent::Cached
    };
    ComponentMemoryUsage::from_files(files, intent)
}

impl MemoryReporter for VectorStorageEnum {
    fn memory_usage(&self) -> ComponentMemoryUsage {
        match self {
            // Volatile (in-memory) dense variants: report RAM size, no files
            VectorStorageEnum::DenseVolatile(v) => {
                ComponentMemoryUsage::ram_only(v.size_of_available_vectors_in_bytes() as u64)
            }
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => {
                ComponentMemoryUsage::ram_only(v.size_of_available_vectors_in_bytes() as u64)
            }
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => {
                ComponentMemoryUsage::ram_only(v.size_of_available_vectors_in_bytes() as u64)
            }

            // Mmap dense variants: intent depends on populate config
            VectorStorageEnum::DenseMemmap(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }
            VectorStorageEnum::DenseMemmapByte(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }
            VectorStorageEnum::DenseMemmapHalf(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }

            // io_uring dense variants: always on-disk, no mmap caching
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => {
                ComponentMemoryUsage::from_files(v.files(), FileStorageIntent::OnDisk)
            }
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => {
                ComponentMemoryUsage::from_files(v.files(), FileStorageIntent::OnDisk)
            }
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => {
                ComponentMemoryUsage::from_files(v.files(), FileStorageIntent::OnDisk)
            }

            // Appendable mmap dense variants: intent depends on populate config
            VectorStorageEnum::DenseAppendableMemmap(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }

            // Volatile sparse: in-memory
            VectorStorageEnum::SparseVolatile(v) => {
                ComponentMemoryUsage::ram_only(v.size_of_available_vectors_in_bytes() as u64)
            }
            // Mmap sparse: intent depends on storage config
            VectorStorageEnum::SparseMmap(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }

            // Volatile multi-dense: in-memory
            VectorStorageEnum::MultiDenseVolatile(v) => {
                ComponentMemoryUsage::ram_only(v.size_of_available_vectors_in_bytes() as u64)
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => {
                ComponentMemoryUsage::ram_only(v.size_of_available_vectors_in_bytes() as u64)
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => {
                ComponentMemoryUsage::ram_only(v.size_of_available_vectors_in_bytes() as u64)
            }

            // Appendable mmap multi-dense: intent depends on populate config
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                from_files_with_on_disk(v.files(), v.is_on_disk())
            }
        }
    }
}
