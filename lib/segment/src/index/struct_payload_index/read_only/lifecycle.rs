use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;

use super::{ReadOnlyIndexesMap, ReadOnlyStructPayloadIndex};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::StorageType;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::VectorNameBuf;
use crate::vector_storage::read_only::VectorStorageReadEnum;

impl<S: UniversalRead> ReadOnlyStructPayloadIndex<S> {
    /// Read-only mirror of `StructPayloadIndex::open`: loads the payload config
    /// and each persisted field index through `fs` (never builds/migrates/writes).
    pub fn open(
        fs: &S::Fs,
        payload: Arc<AtomicRefCell<ReadOnlyPayloadStorage<S>>>,
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageReadEnum<S>>>>,
        path: &Path,
    ) -> OperationResult<Self> {
        let config = PayloadConfig::load_universal(fs, &PayloadConfig::get_config_path(path))?
            .unwrap_or_default();

        let field_indexes = {
            let id_tracker = id_tracker.borrow();
            let total_point_count = id_tracker.total_point_count();
            let deleted_points = id_tracker.deleted_point_bitslice();

            let mut field_indexes: ReadOnlyIndexesMap<S> = HashMap::new();
            for (field, indexed) in config.indices.iter() {
                let mut indexes = Vec::with_capacity(indexed.types.len());
                for index_type in &indexed.types {
                    if let Some(index) = ReadOnlyFieldIndex::open(
                        fs,
                        path,
                        field,
                        &indexed.schema,
                        index_type,
                        total_point_count,
                        deleted_points,
                    )? {
                        indexes.push(index);
                    }
                }
                field_indexes.insert(field.clone(), indexes);
            }
            field_indexes
        };

        Ok(Self {
            payload,
            id_tracker,
            vector_storages,
            field_indexes,
            config,
            path: path.to_owned(),
            visited_pool: Default::default(),
            storage_type: StorageType::NonAppendable,
        })
    }
}
