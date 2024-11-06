use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use bincode;
use bitvec::prelude::{BitSlice, BitVec};
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_buffered_update_wrapper::DatabaseColumnScheduledUpdateWrapper;
use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, DB_MAPPING_CF, DB_VERSIONS_CF};
use crate::common::Flusher;
use crate::id_tracker::point_mappings::PointMappings;
use crate::id_tracker::IdTracker;
use crate::types::{ExtendedPointId, PointIdType, SeqNumberType};

/// Point Id type used for storing ids internally
/// Should be serializable by `bincode`, therefore is not untagged.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
enum StoredPointId {
    NumId(u64),
    Uuid(Uuid),
    String(String),
}

impl From<&ExtendedPointId> for PointIdType {
    fn from(point_id: &ExtendedPointId) -> Self {
        match point_id {
            ExtendedPointId::NumId(idx) => PointIdType::NumId(*idx),
            ExtendedPointId::Uuid(uuid) => PointIdType::Uuid(*uuid),
        }
    }
}

impl From<&ExtendedPointId> for StoredPointId {
    fn from(point_id: &ExtendedPointId) -> Self {
        match point_id {
            ExtendedPointId::NumId(idx) => StoredPointId::NumId(*idx),
            ExtendedPointId::Uuid(uuid) => StoredPointId::Uuid(*uuid),
        }
    }
}

impl From<ExtendedPointId> for StoredPointId {
    fn from(point_id: ExtendedPointId) -> Self {
        Self::from(&point_id)
    }
}

impl From<&StoredPointId> for ExtendedPointId {
    fn from(point_id: &StoredPointId) -> Self {
        match point_id {
            StoredPointId::NumId(idx) => ExtendedPointId::NumId(*idx),
            StoredPointId::Uuid(uuid) => ExtendedPointId::Uuid(*uuid),
            StoredPointId::String(str) => {
                unimplemented!("cannot convert internal string id '{str}' to external id")
            }
        }
    }
}

impl From<StoredPointId> for ExtendedPointId {
    fn from(point_id: StoredPointId) -> Self {
        match point_id {
            StoredPointId::NumId(idx) => ExtendedPointId::NumId(idx),
            StoredPointId::Uuid(uuid) => ExtendedPointId::Uuid(uuid),
            StoredPointId::String(str) => {
                unimplemented!("cannot convert internal string id '{str}' to external id")
            }
        }
    }
}

#[inline]
fn stored_to_external_id(point_id: StoredPointId) -> PointIdType {
    point_id.into()
}

#[inline]
fn external_to_stored_id(point_id: &PointIdType) -> StoredPointId {
    point_id.into()
}

#[derive(Debug)]
pub struct SimpleIdTracker {
    internal_to_version: Vec<SeqNumberType>,
    mapping_db_wrapper: DatabaseColumnScheduledUpdateWrapper,
    versions_db_wrapper: DatabaseColumnScheduledUpdateWrapper,
    mappings: PointMappings,
}

impl SimpleIdTracker {
    pub fn open(store: Arc<RwLock<DB>>) -> OperationResult<Self> {
        let mut deleted = BitVec::new();
        let mut internal_to_external: Vec<PointIdType> = Default::default();
        let mut external_to_internal_num: BTreeMap<u64, PointOffsetType> = Default::default();
        let mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType> = Default::default();

        let mapping_db_wrapper = DatabaseColumnScheduledUpdateWrapper::new(
            DatabaseColumnWrapper::new(store.clone(), DB_MAPPING_CF),
        );
        for (key, val) in mapping_db_wrapper.lock_db().iter()? {
            let external_id = Self::restore_key(&key);
            let internal_id: PointOffsetType =
                bincode::deserialize::<PointOffsetType>(&val).unwrap();
            if internal_id as usize >= internal_to_external.len() {
                internal_to_external.resize(internal_id as usize + 1, PointIdType::NumId(u64::MAX));
            }
            if internal_id as usize >= deleted.len() {
                deleted.resize(internal_id as usize + 1, true);
            }

            let replaced_id = internal_to_external[internal_id as usize];
            internal_to_external[internal_id as usize] = external_id;
            if !deleted[internal_id as usize] {
                // Fixing corrupted mapping - this id should be recovered from WAL
                // This should not happen in normal operation, but it can happen if
                // the database is corrupted.
                log::warn!(
                    "removing duplicated external id {} in internal id {}",
                    external_id,
                    replaced_id
                );
                match replaced_id {
                    PointIdType::NumId(idx) => {
                        external_to_internal_num.remove(&idx);
                    }
                    PointIdType::Uuid(uuid) => {
                        external_to_internal_uuid.remove(&uuid);
                    }
                }
            }
            deleted.set(internal_id as usize, false);

            match external_id {
                PointIdType::NumId(idx) => {
                    external_to_internal_num.insert(idx, internal_id);
                }
                PointIdType::Uuid(uuid) => {
                    external_to_internal_uuid.insert(uuid, internal_id);
                }
            }
        }

        let mut internal_to_version: Vec<SeqNumberType> = Default::default();
        let versions_db_wrapper = DatabaseColumnScheduledUpdateWrapper::new(
            DatabaseColumnWrapper::new(store, DB_VERSIONS_CF),
        );
        for (key, val) in versions_db_wrapper.lock_db().iter()? {
            let external_id = Self::restore_key(&key);
            let version: SeqNumberType = bincode::deserialize(&val).unwrap();
            let internal_id = match external_id {
                PointIdType::NumId(idx) => external_to_internal_num.get(&idx).copied(),
                PointIdType::Uuid(uuid) => external_to_internal_uuid.get(&uuid).copied(),
            };
            if let Some(internal_id) = internal_id {
                if internal_id as usize >= internal_to_version.len() {
                    internal_to_version.resize(internal_id as usize + 1, 0);
                }
                internal_to_version[internal_id as usize] = version;
            } else {
                log::debug!(
                    "Found version without internal id, external id: {}",
                    external_id
                );
            }
        }
        #[cfg(debug_assertions)]
        {
            for (idx, id) in external_to_internal_num.iter() {
                debug_assert!(
                    internal_to_external[*id as usize] == PointIdType::NumId(*idx),
                    "Internal id {id} is mapped to external id {}, but should be {}",
                    internal_to_external[*id as usize],
                    PointIdType::NumId(*idx)
                );
            }
        }
        let mappings = PointMappings::new(
            deleted,
            internal_to_external,
            external_to_internal_num,
            external_to_internal_uuid,
        );

        Ok(SimpleIdTracker {
            internal_to_version,
            mapping_db_wrapper,
            versions_db_wrapper,
            mappings,
        })
    }

    fn store_key(external_id: &PointIdType) -> Vec<u8> {
        bincode::serialize(&external_to_stored_id(external_id)).unwrap()
    }

    fn restore_key(data: &[u8]) -> PointIdType {
        let stored_external_id: StoredPointId = bincode::deserialize(data).unwrap();
        stored_to_external_id(stored_external_id)
    }

    fn delete_key(&self, external_id: &PointIdType) -> OperationResult<()> {
        self.mapping_db_wrapper
            .remove(Self::store_key(external_id))?;
        self.versions_db_wrapper
            .remove(Self::store_key(external_id))?;
        Ok(())
    }

    fn persist_key(&self, external_id: &PointIdType, internal_id: usize) -> OperationResult<()> {
        self.mapping_db_wrapper.put(
            Self::store_key(external_id),
            bincode::serialize(&internal_id).unwrap(),
        )
    }
}

impl IdTracker for SimpleIdTracker {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id as usize).copied()
    }

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        if let Some(external_id) = self.external_id(internal_id) {
            if internal_id as usize >= self.internal_to_version.len() {
                self.internal_to_version.resize(internal_id as usize + 1, 0);
            }
            self.internal_to_version[internal_id as usize] = version;
            self.versions_db_wrapper.put(
                Self::store_key(&external_id),
                bincode::serialize(&version).unwrap(),
            )?;
        }
        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.mappings.internal_id(&external_id)
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.mappings.external_id(internal_id)
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        self.mappings.set_link(external_id, internal_id);
        self.persist_key(&external_id, internal_id as _)?;
        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        self.mappings.drop(external_id);
        self.delete_key(&external_id)?;
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        self.mappings.iter_external()
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.mappings.iter_internal()
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        self.mappings.iter_from(external_id)
    }

    fn iter_random(&self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        self.mappings.iter_random()
    }

    fn total_point_count(&self) -> usize {
        self.mappings.total_point_count()
    }

    fn available_point_count(&self) -> usize {
        self.mappings.available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.total_point_count() - self.available_point_count()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
    }

    /// Creates a flusher function, that persists the removed points in the mapping database
    /// and flushes the mapping to disk.
    /// This function should be called _before_ flushing the version database.
    fn mapping_flusher(&self) -> Flusher {
        self.mapping_db_wrapper.flusher()
    }

    /// Creates a flusher function, that persists the removed points in the version database
    /// and flushes the version database to disk.
    /// This function should be called _after_ flushing the mapping database.
    fn versions_flusher(&self) -> Flusher {
        self.versions_db_wrapper.flusher()
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        self.mappings.is_deleted_point(key)
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        self.mappings.deleted()
    }

    fn cleanup_versions(&mut self) -> OperationResult<()> {
        let mut to_remove = Vec::new();
        for internal_id in self.iter_internal() {
            if self.internal_version(internal_id).is_none() {
                if let Some(external_id) = self.external_id(internal_id) {
                    to_remove.push(external_id);
                } else {
                    debug_assert!(false, "internal id {internal_id} has no external id");
                }
            }
        }
        for external_id in to_remove {
            self.drop(external_id)?;
            #[cfg(debug_assertions)] // Only for dev builds
            {
                log::debug!("dropped version for point {} without version", external_id);
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "simple id tracker"
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use serde::de::DeserializeOwned;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};

    fn check_bincode_serialization<
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    >(
        record: T,
    ) {
        let binary_entity = bincode::serialize(&record).expect("serialization ok");
        let de_record: T = bincode::deserialize(&binary_entity).expect("deserialization ok");

        assert_eq!(record, de_record);
    }

    #[test]
    fn test_serialization() {
        check_bincode_serialization(StoredPointId::NumId(123));
        check_bincode_serialization(StoredPointId::Uuid(Uuid::from_u128(123_u128)));
        check_bincode_serialization(StoredPointId::String("hello".to_string()));
    }

    #[test]
    fn test_iterator() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut id_tracker = SimpleIdTracker::open(db).unwrap();

        id_tracker.set_link(200.into(), 0).unwrap();
        id_tracker.set_link(100.into(), 1).unwrap();
        id_tracker.set_link(150.into(), 2).unwrap();
        id_tracker.set_link(120.into(), 3).unwrap();
        id_tracker.set_link(180.into(), 4).unwrap();
        id_tracker.set_link(110.into(), 5).unwrap();
        id_tracker.set_link(115.into(), 6).unwrap();
        id_tracker.set_link(190.into(), 7).unwrap();
        id_tracker.set_link(177.into(), 8).unwrap();
        id_tracker.set_link(118.into(), 9).unwrap();

        let first_four = id_tracker.iter_from(None).take(4).collect_vec();

        assert_eq!(first_four.len(), 4);
        assert_eq!(first_four[0].0, 100.into());

        let last = id_tracker.iter_from(Some(first_four[3].0)).collect_vec();
        assert_eq!(last.len(), 7);
    }

    #[test]
    fn test_mixed_types_iterator() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut id_tracker = SimpleIdTracker::open(db).unwrap();

        let mut values: Vec<PointIdType> = vec![
            100.into(),
            PointIdType::Uuid(Uuid::from_u128(123_u128)),
            PointIdType::Uuid(Uuid::from_u128(156_u128)),
            150.into(),
            120.into(),
            PointIdType::Uuid(Uuid::from_u128(12_u128)),
            180.into(),
            110.into(),
            115.into(),
            PointIdType::Uuid(Uuid::from_u128(673_u128)),
            190.into(),
            177.into(),
            PointIdType::Uuid(Uuid::from_u128(971_u128)),
        ];

        for (id, value) in values.iter().enumerate() {
            id_tracker.set_link(*value, id as PointOffsetType).unwrap();
        }

        let sorted_from_tracker = id_tracker.iter_from(None).map(|(k, _)| k).collect_vec();
        values.sort();

        assert_eq!(sorted_from_tracker, values);
    }
}
