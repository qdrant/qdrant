use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;

use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::{StorageType, StructPayloadIndexReadView};
use crate::index::visited_pool::VisitedPool;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::{PayloadKeyType, VectorNameBuf};
use crate::vector_storage::read_only::VectorStorageReadEnum;

mod lifecycle;

type ReadOnlyIndexesMap<S> = HashMap<PayloadKeyType, Vec<ReadOnlyFieldIndex<S>>>;

#[allow(dead_code)] // `path`/`storage_type` are read once live-reload lands
pub struct ReadOnlyStructPayloadIndex<S: UniversalRead> {
    /// Payload storage
    pub(super) payload: Arc<AtomicRefCell<ReadOnlyPayloadStorage<S>>>,
    /// Used for `has_id` condition and estimating cardinality
    pub(super) id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    /// Vector storages for each field, used for `has_vector` condition
    pub(super) vector_storages:
        HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageReadEnum<S>>>>,
    /// Indexes, associated with fields
    pub field_indexes: ReadOnlyIndexesMap<S>,
    config: PayloadConfig,
    /// Root of index persistence dir
    path: PathBuf,
    /// Used to select unique point ids
    pub(super) visited_pool: VisitedPool,
    /// Desired storage type for payload indices, used in builder to pick correct type
    storage_type: StorageType,
}

impl<S: UniversalRead> ReadOnlyStructPayloadIndex<S> {
    pub fn with_view<R>(
        &self,
        f: impl FnOnce(
            StructPayloadIndexReadView<
                '_,
                ReadOnlyPayloadStorage<S>,
                ReadOnlyIdTrackerEnum<S>,
                VectorStorageReadEnum<S>,
                ReadOnlyFieldIndex<S>,
            >,
        ) -> R,
    ) -> R {
        let id_tracker = self.id_tracker.borrow();
        let view = StructPayloadIndexReadView {
            payload: &self.payload,
            id_tracker: &*id_tracker,
            vector_storages: &self.vector_storages,
            field_indexes: &self.field_indexes,
            config: &self.config,
            visited_pool: &self.visited_pool,
        };
        f(view)
    }
}
