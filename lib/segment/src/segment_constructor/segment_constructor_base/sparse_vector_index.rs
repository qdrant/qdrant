use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::universal_io::{MmapFile, MmapFs};
use half::f16;
use sparse::common::types::{QuantizedU8, Weight};
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerEnum;
use crate::index::VectorIndexEnum;
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::index::sparse_index::sparse_vector_index::{
    SparseOpenPlan, SparseVectorIndex, SparseVectorIndexOpenArgs,
};
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::VectorStorageDatatype;
use crate::vector_storage::VectorStorageEnum;

#[cfg(feature = "testing")]
pub fn create_sparse_vector_index_test(
    args: SparseVectorIndexOpenArgs<impl FnMut()>,
) -> OperationResult<VectorIndexEnum> {
    open_or_create_sparse_vector_index(args)
}

pub(crate) fn open_or_create_sparse_vector_index(
    args: SparseVectorIndexOpenArgs<impl FnMut()>,
) -> OperationResult<VectorIndexEnum> {
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
    let SparseVectorIndexOpenArgs {
        config,
        id_tracker,
        vector_storage,
        payload_index,
        path,
        stopped,
        tick_progress,
    } = args;

    // Each family helper runs the load-vs-build orchestration for its concrete
    // inverted-index type; here we only pick the helper and wrap its result in the
    // matching `VectorIndexEnum` variant.
    let vector_index = match (effective_index_type, config.datatype.unwrap_or_default()) {
        (SparseIndexType::MutableRam, _) => VectorIndexEnum::SparseRam(open_sparse_mutable_ram(
            config,
            id_tracker,
            vector_storage,
            payload_index,
            path,
            stopped,
            tick_progress,
        )?),
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32) => {
            VectorIndexEnum::SparseCompressedImmutableRamF32(open_sparse_immutable_ram::<f32>(
                config,
                id_tracker,
                vector_storage,
                payload_index,
                path,
                stopped,
                tick_progress,
            )?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float32) => {
            VectorIndexEnum::SparseCompressedMmapF32(open_sparse_mmap::<f32>(
                config,
                id_tracker,
                vector_storage,
                payload_index,
                path,
                stopped,
                tick_progress,
            )?)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float16) => {
            VectorIndexEnum::SparseCompressedImmutableRamF16(open_sparse_immutable_ram::<f16>(
                config,
                id_tracker,
                vector_storage,
                payload_index,
                path,
                stopped,
                tick_progress,
            )?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float16) => {
            VectorIndexEnum::SparseCompressedMmapF16(open_sparse_mmap::<f16>(
                config,
                id_tracker,
                vector_storage,
                payload_index,
                path,
                stopped,
                tick_progress,
            )?)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Uint8) => {
            VectorIndexEnum::SparseCompressedImmutableRamU8(
                open_sparse_immutable_ram::<QuantizedU8>(
                    config,
                    id_tracker,
                    vector_storage,
                    payload_index,
                    path,
                    stopped,
                    tick_progress,
                )?,
            )
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Uint8) => {
            VectorIndexEnum::SparseCompressedMmapU8(open_sparse_mmap::<QuantizedU8>(
                config,
                id_tracker,
                vector_storage,
                payload_index,
                path,
                stopped,
                tick_progress,
            )?)
        }
        (_, VectorStorageDatatype::Turbo4) => {
            unreachable!("Sparse index incompatible with turbo. Validated at API level.")
        }
    };

    Ok(vector_index)
}

/// Load-or-build a mutable RAM sparse index ([`InvertedIndexRam`]).
///
/// `plan` decides load-vs-build generically; this runs the one type-specific
/// construction step and assembles via `finish`. Mutable RAM indexes are never
/// persisted, so the build path always reports `persist = false`.
fn open_sparse_mutable_ram(
    config: SparseIndexConfig,
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    path: &Path,
    stopped: &AtomicBool,
    tick_progress: impl FnMut(),
) -> OperationResult<SparseVectorIndex<InvertedIndexRam>> {
    let plan = SparseVectorIndex::<InvertedIndexRam>::plan(
        config,
        &id_tracker,
        &vector_storage,
        path,
        stopped,
        tick_progress,
    )?;
    let (inverted_index, config, indices_tracker, persist) = match plan {
        SparseOpenPlan::Load {
            config,
            indices_tracker,
        } => (
            InvertedIndexRam::open(&MmapFs, path)?,
            config,
            indices_tracker,
            false,
        ),
        SparseOpenPlan::Build {
            config,
            ram_index,
            indices_tracker,
            persist,
        } => (
            InvertedIndexRam::from_ram_index(&MmapFs, Cow::Owned(ram_index), path)?,
            config,
            indices_tracker,
            persist,
        ),
    };
    SparseVectorIndex::finish(
        config,
        id_tracker,
        vector_storage,
        payload_index,
        path,
        inverted_index,
        indices_tracker,
        persist,
    )
}

/// Load-or-build an immutable RAM compressed sparse index
/// ([`InvertedIndexCompressedImmutableRam<W>`]), generic over the weight type.
fn open_sparse_immutable_ram<W: Weight + 'static>(
    config: SparseIndexConfig,
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    path: &Path,
    stopped: &AtomicBool,
    tick_progress: impl FnMut(),
) -> OperationResult<SparseVectorIndex<InvertedIndexCompressedImmutableRam<W>>> {
    let plan = SparseVectorIndex::<InvertedIndexCompressedImmutableRam<W>>::plan(
        config,
        &id_tracker,
        &vector_storage,
        path,
        stopped,
        tick_progress,
    )?;
    let (inverted_index, config, indices_tracker, persist) = match plan {
        SparseOpenPlan::Load {
            config,
            indices_tracker,
        } => (
            InvertedIndexCompressedImmutableRam::<W>::open(&MmapFs, path)?,
            config,
            indices_tracker,
            false,
        ),
        SparseOpenPlan::Build {
            config,
            ram_index,
            indices_tracker,
            persist,
        } => (
            InvertedIndexCompressedImmutableRam::<W>::from_ram_index(
                &MmapFs,
                Cow::Owned(ram_index),
                path,
            )?,
            config,
            indices_tracker,
            persist,
        ),
    };
    SparseVectorIndex::finish(
        config,
        id_tracker,
        vector_storage,
        payload_index,
        path,
        inverted_index,
        indices_tracker,
        persist,
    )
}

/// Load-or-build an mmap compressed sparse index
/// ([`InvertedIndexCompressedMmap<W, MmapFile>`]), generic over the weight type.
fn open_sparse_mmap<W: Weight + 'static>(
    config: SparseIndexConfig,
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    path: &Path,
    stopped: &AtomicBool,
    tick_progress: impl FnMut(),
) -> OperationResult<SparseVectorIndex<InvertedIndexCompressedMmap<W, MmapFile>>> {
    let plan = SparseVectorIndex::<InvertedIndexCompressedMmap<W, MmapFile>>::plan(
        config,
        &id_tracker,
        &vector_storage,
        path,
        stopped,
        tick_progress,
    )?;
    let (inverted_index, config, indices_tracker, persist) = match plan {
        SparseOpenPlan::Load {
            config,
            indices_tracker,
        } => (
            InvertedIndexCompressedMmap::<W, MmapFile>::open(&MmapFs, path)?,
            config,
            indices_tracker,
            false,
        ),
        SparseOpenPlan::Build {
            config,
            ram_index,
            indices_tracker,
            persist,
        } => (
            InvertedIndexCompressedMmap::<W, MmapFile>::from_ram_index(
                &MmapFs,
                Cow::Owned(ram_index),
                path,
            )?,
            config,
            indices_tracker,
            persist,
        ),
    };
    SparseVectorIndex::finish(
        config,
        id_tracker,
        vector_storage,
        payload_index,
        path,
        inverted_index,
        indices_tracker,
        persist,
    )
}
