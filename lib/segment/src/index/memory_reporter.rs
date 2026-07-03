use crate::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent, MemoryReporter};
use crate::index::vector_index_base::{VectorIndex as _, VectorIndexEnum, VectorIndexRead as _};

impl MemoryReporter for VectorIndexEnum {
    fn memory_usage(&self) -> ComponentMemoryUsage {
        match self {
            // Plain index: no files, no extra memory (searches storage directly)
            VectorIndexEnum::Plain(_) => ComponentMemoryUsage::empty(),

            // HNSW: graph files, intent depends on how the links are actually held
            VectorIndexEnum::Hnsw(index) => {
                let links_heap_bytes = index.links_heap_size_bytes() as u64;
                if links_heap_bytes > 0 {
                    // Links are materialized in heap RAM (freshly built index,
                    // or a non-borrowable universal-IO backend): files are
                    // persistence only, not expected to be in page cache.
                    ComponentMemoryUsage::from_files_and_ram(
                        index.files(),
                        FileStorageIntent::OnDisk,
                        links_heap_bytes,
                    )
                } else {
                    // Links are backed by a live mmap handle: residency is
                    // tracked via the page cache, intent depends on on_disk config.
                    let intent = if index.is_on_disk() {
                        FileStorageIntent::OnDisk
                    } else {
                        FileStorageIntent::Cached
                    };
                    ComponentMemoryUsage::from_files(index.files(), intent)
                }
            }

            // Sparse RAM variants: inverted index is deserialized into heap.
            // Files are persistence only (OnDisk), actual RAM is extra_ram_bytes.
            VectorIndexEnum::SparseRam(index) => ComponentMemoryUsage::from_files_and_ram(
                index.files(),
                FileStorageIntent::OnDisk,
                index.size_of_searchable_vectors_in_bytes() as u64,
            ),
            VectorIndexEnum::SparseCompressedImmutableRamF32(index) => {
                ComponentMemoryUsage::from_files_and_ram(
                    index.files(),
                    FileStorageIntent::OnDisk,
                    index.size_of_searchable_vectors_in_bytes() as u64,
                )
            }
            VectorIndexEnum::SparseCompressedImmutableRamF16(index) => {
                ComponentMemoryUsage::from_files_and_ram(
                    index.files(),
                    FileStorageIntent::OnDisk,
                    index.size_of_searchable_vectors_in_bytes() as u64,
                )
            }
            VectorIndexEnum::SparseCompressedImmutableRamU8(index) => {
                ComponentMemoryUsage::from_files_and_ram(
                    index.files(),
                    FileStorageIntent::OnDisk,
                    index.size_of_searchable_vectors_in_bytes() as u64,
                )
            }

            // Sparse mmap variants: inverted index is mmap'd but not populated
            // (loaded with populate=false), relies on OS demand-paging
            VectorIndexEnum::SparseCompressedMmapF32(index) => {
                ComponentMemoryUsage::from_files(index.files(), FileStorageIntent::OnDisk)
            }
            VectorIndexEnum::SparseCompressedMmapF16(index) => {
                ComponentMemoryUsage::from_files(index.files(), FileStorageIntent::OnDisk)
            }
            VectorIndexEnum::SparseCompressedMmapU8(index) => {
                ComponentMemoryUsage::from_files(index.files(), FileStorageIntent::OnDisk)
            }
        }
    }
}
