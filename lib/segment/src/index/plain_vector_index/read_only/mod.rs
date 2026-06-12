mod read;

use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;
use parking_lot::Mutex;

use crate::common::operation_error::OperationResult;
use crate::common::operation_time_statistics::OperationDurationsAggregator;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::plain_vector_index::read_view::PlainVectorIndexReadView;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

pub struct ReadOnlyPlainVectorIndex<S: UniversalRead> {
    id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
    payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
    filtered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
    unfiltered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
}

/// Read-only view over a [`ReadOnlyPlainVectorIndex`].
///
/// The top-level backends are read-only ([`ReadOnlyIdTrackerEnum`] /
/// [`VectorStorageReadEnum`] / [`ReadOnlyQuantizedVectors`]), while the payload
/// index view is still built over the in-memory enums of [`StructPayloadIndex`].
type ReadView<'a, S> = PlainVectorIndexReadView<
    'a,
    ReadOnlyIdTrackerEnum<S>,
    VectorStorageReadEnum<S>,
    ReadOnlyQuantizedVectors<S>,
    StructPayloadIndexReadView<
        'a,
        ReadOnlyPayloadStorage<S>,
        ReadOnlyIdTrackerEnum<S>,
        VectorStorageReadEnum<S>,
        ReadOnlyFieldIndex<S>,
    >,
>;

impl<S: UniversalRead> ReadOnlyPlainVectorIndex<S> {
    /// Read-only mirror of `PlainVectorIndex::new`: wires the shared backends.
    pub fn open(
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
        quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
        payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
    ) -> OperationResult<Self> {
        Ok(Self {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            filtered_searches_telemetry: OperationDurationsAggregator::new(),
            unfiltered_searches_telemetry: OperationDurationsAggregator::new(),
        })
    }

    pub fn with_view<R>(&self, f: impl FnOnce(ReadView<'_, S>) -> R) -> R {
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
