use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalReadFs};
use futures::future::BoxFuture;

use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::UniversalReadExt;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::index::visited_pool::VisitedPool;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::{PayloadKeyType, VectorNameBuf};
use crate::vector_storage::read_only::VectorStorageReadEnum;

mod config_reload;
mod lifecycle;

pub use config_reload::PayloadIndexReloadDiff;

type ReadOnlyIndexesMap<S> = AHashMap<PayloadKeyType, Vec<ReadOnlyFieldIndex<S>>>;

pub struct ReadOnlyStructPayloadIndex<S: UniversalReadExt> {
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
}

impl<S: UniversalReadExt> ReadOnlyStructPayloadIndex<S> {
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

impl<S: UniversalReadExt> LiveReload for ReadOnlyStructPayloadIndex<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        let mut futs = Vec::new();
        for field_index in self.field_indexes.values().flatten() {
            futs.extend(field_index.live_preload(fs)?);
        }

        Ok(futs)
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        for field_index in self.field_indexes.values_mut().flatten() {
            field_index.live_reload(fs, deleted_points, new_points, hw_counter)?;
        }

        Ok(())
    }
}
