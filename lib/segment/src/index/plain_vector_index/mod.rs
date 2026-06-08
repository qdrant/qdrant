mod lifecycle;
mod read;

use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use parking_lot::Mutex;

use crate::common::operation_time_statistics::OperationDurationsAggregator;
use crate::id_tracker::IdTrackerEnum;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

#[derive(Debug)]
pub struct PlainVectorIndex {
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    filtered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
    unfiltered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
}

impl PlainVectorIndex {
    pub fn new(
        id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
        payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    ) -> PlainVectorIndex {
        PlainVectorIndex {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            filtered_searches_telemetry: OperationDurationsAggregator::new(),
            unfiltered_searches_telemetry: OperationDurationsAggregator::new(),
        }
    }
}
