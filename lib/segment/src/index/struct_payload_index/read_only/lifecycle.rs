use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::{CachedReadFs, UniversalReadFs};

use super::{ReadOnlyIndexesMap, ReadOnlyStructPayloadIndex};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::load_profile::LoadProfile;
use crate::id_tracker::IdTrackerRead;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::UniversalReadExt;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::payload_config::PayloadConfig;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::VectorNameBuf;
use crate::vector_storage::read_only::VectorStorageReadEnum;

impl<S: UniversalReadExt> ReadOnlyStructPayloadIndex<S> {
    /// A `load_profile` decides per field whether its indexes keep the
    /// persisted placement or are parked cold (see
    /// [`LoadProfile::payload_index_placement`]).
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        load_profile: Option<&LoadProfile>,
    ) -> OperationResult<PayloadConfig> {
        // Config
        let config_path = PayloadConfig::get_config_path(path);
        let config = PayloadConfig::load_universal(fs, &config_path)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "Read-only payload index missing config at {}",
                config_path.display()
            ))
        })?;

        // Payload indexes
        for (field, indexed) in config.indices.iter() {
            let populate_override =
                load_profile.and_then(|profile| profile.payload_index_placement(field));
            for index_type in &indexed.types {
                ReadOnlyFieldIndex::preopen(
                    fs,
                    path,
                    field,
                    &indexed.schema,
                    index_type,
                    populate_override,
                )?;
            }
        }

        Ok(config)
    }

    /// Read-only mirror of `StructPayloadIndex::open`: loads each persisted field
    /// index through `fs` (never builds/migrates/writes). `config` is the one
    /// [`preopen`](Self::preopen) already read; `load_profile` mirrors the
    /// preopen's per-field placement decisions.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        payload: Arc<AtomicRefCell<ReadOnlyPayloadStorage<S>>>,
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageReadEnum<S>>>>,
        path: &Path,
        config: PayloadConfig,
        load_profile: Option<&LoadProfile>,
    ) -> OperationResult<Self> {
        let field_indexes = {
            let id_tracker = id_tracker.borrow();
            let total_point_count = id_tracker.total_point_count();
            let deleted_points = id_tracker.deleted_point_bitslice();

            let mut field_indexes: ReadOnlyIndexesMap<S> = HashMap::new();
            for (field, indexed) in config.indices.iter() {
                let populate_override =
                    load_profile.and_then(|profile| profile.payload_index_placement(field));
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
                        populate_override,
                    )? {
                        indexes.push(index);
                    }
                }
                if !indexes.is_empty() {
                    field_indexes.insert(field.clone(), indexes);
                }
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
        })
    }
}
