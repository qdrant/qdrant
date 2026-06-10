mod lifecycle;
mod read;

pub mod read_only;
mod read_view;

use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use parking_lot::Mutex;

use crate::common::operation_time_statistics::OperationDurationsAggregator;
use crate::id_tracker::IdTrackerEnum;
use crate::index::plain_vector_index::read_view::{
    PlainVectorIndexReadView, PlainVectorIndexReadViewEnum,
};
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

    pub fn with_view<R>(&self, f: impl FnOnce(PlainVectorIndexReadViewEnum<'_>) -> R) -> R {
        let payload_index = self.payload_index.borrow();
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let quantized_vectors = self.quantized_vectors.borrow();
        let filtered_searches_telemetry = self.filtered_searches_telemetry.clone();
        let unfiltered_searches_telemetry = self.unfiltered_searches_telemetry.clone();

        payload_index.with_view(|payload_index_view| {
            let read_view = PlainVectorIndexReadView {
                id_tracker: &*id_tracker,
                vector_storage: &*vector_storage,
                quantized_vectors: quantized_vectors.as_ref(),
                payload_index: payload_index_view,
                filtered_searches_telemetry,
                unfiltered_searches_telemetry,
            };
            f(read_view)
        })
    }
}
