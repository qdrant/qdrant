use std::path::PathBuf;

use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent, MemoryReporter};
use crate::vector_storage::vector_storage_base::{
    VectorStorage as _, VectorStorageEnum, VectorStorageRead as _,
};

/// Determine the file storage intent for mmap-based vector storage.
///
/// `is_cold() == true` means data is not populated — rely on OS page cache.
/// `is_cold() == false` means data was populated on load — expected to be cached.
fn from_files_with_on_disk(files: Vec<PathBuf>, cold: bool) -> ComponentMemoryUsage {
    let intent = if cold {
        FileStorageIntent::OnDisk
    } else {
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
            VectorStorageEnum::DenseMemmap(v) => from_files_with_on_disk(v.files(), v.is_cold()),
            VectorStorageEnum::DenseMemmapByte(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::DenseMemmapHalf(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::DenseGraphInline(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::DenseGraphInlineByte(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::DenseGraphInlineHalf(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
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
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }

            VectorStorageEnum::DenseTurboMemmap(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::DenseTurboGraphInline(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(v) => {
                ComponentMemoryUsage::from_files(v.files(), FileStorageIntent::OnDisk)
            }
            VectorStorageEnum::DenseTurboAppendableMemmap(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::MultiDenseTurbo(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }

            // Volatile sparse: in-memory
            VectorStorageEnum::SparseVolatile(v) => {
                ComponentMemoryUsage::ram_only(v.size_of_available_vectors_in_bytes() as u64)
            }
            // Mmap sparse: intent depends on storage config
            VectorStorageEnum::SparseMmap(v) => from_files_with_on_disk(v.files(), v.is_cold()),

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
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                from_files_with_on_disk(v.files(), v.is_cold())
            }
            VectorStorageEnum::EmptyDense(_) => ComponentMemoryUsage::empty(),
            VectorStorageEnum::EmptySparse(_) => ComponentMemoryUsage::empty(),
        }
    }
}
