use common::universal_io::MmapFs;

use crate::common::operation_error::OperationResult;
use crate::index::VectorIndexEnum;
use crate::index::sparse_index::sparse_index_config::SparseIndexType;
use crate::index::sparse_index::sparse_vector_index::{
    SparseVectorIndex, SparseVectorIndexOpenArgs,
};
use crate::types::{Memory, VectorStorageDatatype};

#[cfg(feature = "testing")]
pub fn create_sparse_vector_index_test(
    args: SparseVectorIndexOpenArgs<MmapFs, impl FnMut()>,
) -> OperationResult<VectorIndexEnum> {
    open_or_create_sparse_vector_index(args)
}

pub(crate) fn open_or_create_sparse_vector_index(
    args: SparseVectorIndexOpenArgs<MmapFs, impl FnMut()>,
) -> OperationResult<VectorIndexEnum> {
    // Effective placement of the mmap index at load time, degraded by low-memory mode.
    // Cold and cached share the mmap index variant; cached additionally primes the page
    // cache after opening.
    let memory_placement = args.config.memory_placement().clamp_to_low_memory();

    let effective_index_type = match args.config.index_type {
        SparseIndexType::ImmutableRam => {
            // Low-memory mode downgrades `ImmutableRam` (which copies the inverted
            // index from mmap files into heap RAM at load) to `Mmap` (which keeps
            // it on disk). The two variants share the same on-disk file format, so
            // flipping at load time is safe without rebuild. The persisted
            // `SparseIndexConfig.index_type` is not modified — `try_load` re-reads
            // it from disk and the loaded config is kept for future persistence.
            if common::low_memory::low_memory_mode().prefer_disk() {
                SparseIndexType::Mmap
            } else {
                SparseIndexType::ImmutableRam
            }
        }
        SparseIndexType::MutableRam => SparseIndexType::MutableRam,
        SparseIndexType::Mmap => SparseIndexType::Mmap,
    };

    // Each family helper runs the load-vs-build orchestration for its concrete
    // inverted-index type; here we only pick the helper and wrap its result in the
    // matching `VectorIndexEnum` variant.
    let vector_index = match (
        effective_index_type,
        args.config.datatype.unwrap_or_default(),
    ) {
        (SparseIndexType::MutableRam, _) => {
            VectorIndexEnum::SparseRam(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32) => {
            VectorIndexEnum::SparseCompressedImmutableRamF32(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float32) => {
            VectorIndexEnum::SparseCompressedMmapF32(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float16) => {
            VectorIndexEnum::SparseCompressedImmutableRamF16(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float16) => {
            VectorIndexEnum::SparseCompressedMmapF16(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Uint8) => {
            VectorIndexEnum::SparseCompressedImmutableRamU8(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Uint8) => {
            VectorIndexEnum::SparseCompressedMmapU8(SparseVectorIndex::open(args)?)
        }
        (_, VectorStorageDatatype::Turbo4) => {
            unreachable!("Sparse index incompatible with turbo. Validated at API level.")
        }
    };

    if memory_placement == Memory::Cached {
        vector_index.populate()?;
    }

    Ok(vector_index)
}
