use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use rand::Rng;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerEnum;
use crate::index::VectorIndexEnum;
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use crate::index::plain_vector_index::PlainVectorIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::{HnswGlobalConfig, Indexes, VectorDataConfig};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

pub(crate) struct VectorIndexOpenArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
}

// Two lifetime parameters, not one: `gpu_device`'s `LockedGpuDevice<'g>` is locked once and
// reused across a whole loop of `build_vector_index()` calls (see segment_builder.rs) — its
// `'g` (governing the `MutexGuard` inside) legitimately spans that entire loop. But the `&'a
// mut` reference to it, like `old_indices`/`rng`, is only ever borrowed fresh for one call at
// a time. A single-lifetime version of this struct (originally `Option<&'a
// LockedGpuDevice<'a>>`) unified both under one 'a — harmless for a *shared* reference (shared
// borrows freely coexist across iterations even when their target's own drop-relevant lifetime
// is long), but a real conflict once this became `&mut` for GPU-device-recreation support:
// unifying 'a with 'g forced every iteration's exclusive borrow to be treated as live for the
// *entire* loop, so consecutive iterations' borrows overlapped and old_indices/rng's per-call
// borrows got dragged into the same false constraint. Keeping the outer reference under the
// same short 'a as old_indices/rng, while letting `LockedGpuDevice`'s own internal 'g stay
// long-lived and independent, is what actually fixes it — confirmed by hitting exactly this
// class of borrowck error (E0716/E0597/E0499, then E0597 again) in segment_builder.rs while
// iterating on this signature.
pub struct VectorIndexBuildArgs<'a, 'g, R: Rng + ?Sized> {
    pub permit: Arc<ResourcePermit>,
    /// Vector indices from other segments, used to speed up index building.
    /// May or may not contain the same vectors.
    pub old_indices: &'a [Arc<AtomicRefCell<VectorIndexEnum>>],
    pub gpu_device: Option<&'a mut LockedGpuDevice<'g>>,
    pub rng: &'a mut R,
    pub stopped: &'a AtomicBool,
    pub hnsw_global_config: &'a HnswGlobalConfig,
    pub feature_flags: FeatureFlags,
    pub progress: ProgressTracker,
}

pub(crate) fn open_vector_index(
    vector_config: &VectorDataConfig,
    open_args: VectorIndexOpenArgs,
) -> OperationResult<VectorIndexEnum> {
    let VectorIndexOpenArgs {
        path,
        id_tracker,
        vector_storage,
        payload_index,
        quantized_vectors,
    } = open_args;
    Ok(match &vector_config.index {
        Indexes::Plain {} => VectorIndexEnum::Plain(PlainVectorIndex::new(
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
        )),
        Indexes::Hnsw(hnsw_config) => VectorIndexEnum::Hnsw(HNSWIndex::open(HnswIndexOpenArgs {
            path,
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            hnsw_config: *hnsw_config,
        })?),
    })
}

pub(crate) fn build_vector_index<R: Rng + ?Sized>(
    vector_config: &VectorDataConfig,
    open_args: VectorIndexOpenArgs,
    build_args: VectorIndexBuildArgs<R>,
) -> OperationResult<VectorIndexEnum> {
    let VectorIndexOpenArgs {
        path,
        id_tracker,
        vector_storage,
        payload_index,
        quantized_vectors,
    } = open_args;
    Ok(match &vector_config.index {
        Indexes::Plain {} => VectorIndexEnum::Plain(PlainVectorIndex::new(
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
        )),
        Indexes::Hnsw(hnsw_config) => VectorIndexEnum::Hnsw(HNSWIndex::build(
            HnswIndexOpenArgs {
                path,
                id_tracker,
                vector_storage,
                quantized_vectors,
                payload_index,
                hnsw_config: *hnsw_config,
            },
            build_args,
        )?),
    })
}
