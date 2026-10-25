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
                // Batched graphs have heap metadata while their links remain file-backed.
                let intent = index.links_file_storage_intent();
                if links_heap_bytes > 0 {
                    ComponentMemoryUsage::from_files_and_ram(
                        index.files(),
                        intent,
                        links_heap_bytes,
                    )
                } else {
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
