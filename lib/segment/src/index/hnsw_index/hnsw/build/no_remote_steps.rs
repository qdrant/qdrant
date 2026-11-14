//! Stands in for `remote_steps.rs` without the `remote-indexing` feature: the same items,
//! which do nothing. No remote build is ever possible, so the arguments of one can't be made.

use std::convert::Infallible;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::progress_tracker::ProgressTracker;

use super::block_filter::BlockSkip;
use super::graph_builder::GraphBuilder;
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::{PayloadFieldSchema, PayloadKeyType};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// Can't be constructed.
pub(super) struct RemoteBuildArgs<'a> {
    never: Infallible,
    _permit: PhantomData<&'a ResourcePermit>,
}

/// Can't be constructed.
pub(super) struct FallbackBlocks {
    never: Infallible,
}

pub(super) fn build_args<'a>(
    _path: &Path,
    _vector_storage: &Arc<AtomicRefCell<VectorStorageEnum>>,
    _vector_storage_ref: &VectorStorageEnum,
    _quantized_vectors: &Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    _quantized_vectors_ref: Option<&QuantizedVectors>,
    _permit: &'a ResourcePermit,
    _progress: &ProgressTracker,
) -> Option<RemoteBuildArgs<'a>> {
    None
}

pub(super) fn kept_local_for_healing() {}

pub(super) fn remote_builder<'a>(
    _cpu: GraphBuilder<'a>,
    args: RemoteBuildArgs<'a>,
    _has_payload_blocks: bool,
) -> GraphBuilder<'a> {
    match args.never {}
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "same signature as with the feature"
)]
pub(super) fn start<'f>(
    _builder: &mut GraphBuilder<'_>,
    _fields: impl IntoIterator<Item = (&'f PayloadKeyType, &'f PayloadFieldSchema)>,
    _payload_index: &StructPayloadIndex,
    _vector_storage: &VectorStorageEnum,
    _config: &HnswGraphConfig,
    _stopped: &AtomicBool,
) -> OperationResult<()> {
    Ok(())
}

pub(super) fn blocks_linked(_builder: &mut GraphBuilder<'_>) -> bool {
    false
}

pub(super) fn fallback_blocks(_builder: &mut GraphBuilder<'_>) -> Option<FallbackBlocks> {
    None
}

pub(super) fn build_fallback_field_blocks(
    _builder: &mut GraphBuilder<'_>,
    plan: &FallbackBlocks,
    _field: &PayloadKeyType,
    _progress: &ProgressTracker,
    _skip: BlockSkip,
) -> OperationResult<()> {
    match plan.never {}
}
