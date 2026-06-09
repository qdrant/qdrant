mod search;

use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::operation_time_statistics::OperationDurationsAggregator;
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::PayloadIndexRead;
use crate::index::field_index::FieldIndex;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::{QuantizedVectors, QuantizedVectorsRead};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Read-only view over a plain vector index.
///
/// The top-level id tracker / vector storage / quantized vectors are decoupled
/// from the payload index view (`P`), so read-only backends can be combined with
/// a payload index view built over different (e.g. still-mutable) backends.
pub struct PlainVectorIndexReadView<
    'a,
    I: IdTrackerRead,
    V: VectorStorageRead,
    Q: QuantizedVectorsRead,
    P: PayloadIndexRead,
> {
    pub(crate) id_tracker: &'a I,
    pub(crate) vector_storage: &'a V,
    pub(crate) quantized_vectors: Option<&'a Q>,
    pub(crate) payload_index: P,
    pub(crate) filtered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
    pub(crate) unfiltered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
}

/// Read-only view over a [`PlainVectorIndex`] backed by the in-memory
/// [`IdTrackerEnum`] / [`VectorStorageEnum`] / [`PayloadStorageEnum`] enums.
///
/// [`PlainVectorIndex`]: super::PlainVectorIndex
pub(crate) type PlainVectorIndexReadViewEnum<'a> = PlainVectorIndexReadView<
    'a,
    IdTrackerEnum,
    VectorStorageEnum,
    QuantizedVectors,
    StructPayloadIndexReadView<
        'a,
        PayloadStorageEnum,
        IdTrackerEnum,
        VectorStorageEnum,
        FieldIndex,
    >,
>;
