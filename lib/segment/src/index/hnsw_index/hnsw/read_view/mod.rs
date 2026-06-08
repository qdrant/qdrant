mod dispatch;
mod search;

use super::telemetry::HNSWSearchesTelemetry;
use crate::id_tracker::{IdTracker, IdTrackerEnum};
use crate::index::field_index::{FieldIndex, FieldIndexRead};
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::payload_storage::PayloadStorageRead;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsReadAccess,
};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

pub struct HNSWIndexReadView<
    'a,
    I: IdTracker,
    V: VectorStorageRead,
    P: PayloadStorageRead,
    F: FieldIndexRead,
    Q: QuantizedVectorsReadAccess,
> {
    pub(crate) id_tracker: &'a I,
    pub(crate) vector_storage: &'a V,
    pub(crate) quantized_vectors: Option<&'a Q>,
    pub(crate) payload_index: StructPayloadIndexReadView<'a, P, I, V, F>,
    pub(crate) config: &'a HnswGraphConfig,
    pub(crate) graph: &'a GraphLayers,
    pub(crate) searches_telemetry: &'a HNSWSearchesTelemetry,
}

/// Read-only view over an [`HNSWIndex`] backed by the in-memory
/// [`IdTrackerEnum`] / [`VectorStorageEnum`] / [`PayloadStorageEnum`] enums.
///
/// [`HNSWIndex`]: super::super::HNSWIndex
pub(crate) type HNSWIndexReadViewEnum<'a> = HNSWIndexReadView<
    'a,
    IdTrackerEnum,
    VectorStorageEnum,
    PayloadStorageEnum,
    FieldIndex,
    QuantizedVectors,
>;
