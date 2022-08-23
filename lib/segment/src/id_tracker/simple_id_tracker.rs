use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bincode;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, DB_MAPPING_CF, DB_VERSIONS_CF};
use crate::common::Flusher;
use crate::entry::entry_point::OperationResult;
use crate::id_tracker::IdTracker;
use crate::types::{ExtendedPointId, PointIdType, PointOffsetType, SeqNumberType};

/// Point Id type used for storing ids internally
/// Should be serializable by `bincode`, therefore is not untagged.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
enum StoredPointId {
    NumId(u64),
    Uuid(Uuid),
    String(String),
}

impl From<&ExtendedPointId> for StoredPointId {
    fn from(point_id: &ExtendedPointId) -> Self {
        match point_id {
            ExtendedPointId::NumId(idx) => StoredPointId::NumId(*idx),
            ExtendedPointId::Uuid(uuid) => StoredPointId::Uuid(*uuid),
        }
    }
}

impl From<StoredPointId> for ExtendedPointId {
    fn from(point_id: StoredPointId) -> Self {
        match point_id {
            StoredPointId::NumId(idx) => ExtendedPointId::NumId(idx),
            StoredPointId::Uuid(uuid) => ExtendedPointId::Uuid(uuid),
            StoredPointId::String(_str) => unimplemented!(),
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

pub struct SimpleIdTracker {
    internal_to_external: HashMap<PointOffsetType, PointIdType>,
    external_to_internal: BTreeMap<PointIdType, PointOffsetType>,
    external_to_version: HashMap<PointIdType, SeqNumberType>,
    max_internal_id: PointOffsetType,
    mapping_db_wrapper: DatabaseColumnWrapper,
    versions_db_wrapper: DatabaseColumnWrapper,
}

impl SimpleIdTracker {
    pub fn open(store: Arc<RwLock<DB>>) -> OperationResult<Self> {
        let mut internal_to_external: HashMap<PointOffsetType, PointIdType> = Default::default();
        let mut external_to_internal: BTreeMap<PointIdType, PointOffsetType> = Default::default();
        let mut external_to_version: HashMap<PointIdType, SeqNumberType> = Default::default();
        let mut max_internal_id = 0;

        let mapping_db_wrapper = DatabaseColumnWrapper::new(store.clone(), DB_MAPPING_CF);
        for (key, val) in mapping_db_wrapper.lock_db().iter()? {
            let external_id = Self::restore_key(&key);
            let internal_id: PointOffsetType = bincode::deserialize(&val).unwrap();
            let replaced = internal_to_external.insert(internal_id, external_id);
            if let Some(replaced_id) = replaced {
                // Fixing corrupted mapping - this id should be recovered from WAL
                // This should not happen in normal operation, but it can happen if
                // the database is corrupted.
                log::warn!(
                    "removing duplicated external id {} in internal id {}",
                    external_id,
                    replaced_id
                );
                external_to_internal.remove(&replaced_id);
            }
            external_to_internal.insert(external_id, internal_id);
            max_internal_id = max_internal_id.max(internal_id);
        }

        let versions_db_wrapper = DatabaseColumnWrapper::new(store, DB_VERSIONS_CF);
        for (key, val) in versions_db_wrapper.lock_db().iter()? {
            let external_id = Self::restore_key(&key);
            let version: SeqNumberType = bincode::deserialize(&val).unwrap();
            external_to_version.insert(external_id, version);
        }

        Ok(SimpleIdTracker {
            internal_to_external,
            external_to_internal,
            external_to_version,
            max_internal_id,
            mapping_db_wrapper,
            versions_db_wrapper,
        })
    }

    fn store_key(external_id: &PointIdType) -> Vec<u8> {
        bincode::serialize(&external_to_stored_id(external_id)).unwrap()
    }

    fn restore_key(data: &[u8]) -> PointIdType {
        let stored_external_id: StoredPointId = bincode::deserialize(data).unwrap();
        stored_to_external_id(stored_external_id)
    }
}

impl IdTracker for SimpleIdTracker {
    fn version(&self, external_id: PointIdType) -> Option<SeqNumberType> {
        self.external_to_version.get(&external_id).copied()
    }

    fn set_version(
        &mut self,
        external_id: PointIdType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        self.external_to_version.insert(external_id, version);
        self.versions_db_wrapper.put(
            &Self::store_key(&external_id),
            &bincode::serialize(&version).unwrap(),
        )?;
        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.external_to_internal.get(&external_id).copied()
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.internal_to_external.get(&internal_id).copied()
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        self.external_to_internal.insert(external_id, internal_id);
        self.internal_to_external.insert(internal_id, external_id);
        self.max_internal_id = self.max_internal_id.max(internal_id);

        self.mapping_db_wrapper.put(
            &Self::store_key(&external_id),
            &bincode::serialize(&internal_id).unwrap(),
        )?;
        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        self.external_to_version.remove(&external_id);

        let internal_id = self.external_to_internal.remove(&external_id);
        match internal_id {
            Some(x) => self.internal_to_external.remove(&x),
            None => None,
        };
        self.mapping_db_wrapper
            .remove(&Self::store_key(&external_id))?;
        self.versions_db_wrapper
            .remove(&Self::store_key(&external_id))?;
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        Box::new(self.external_to_internal.keys().copied())
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(self.internal_to_external.keys().copied())
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        let range = match external_id {
            None => self.external_to_internal.range(..),
            Some(offset) => self.external_to_internal.range(offset..),
        };

        Box::new(range.map(|(key, value)| (*key, *value)))
    }

    fn points_count(&self) -> usize {
        self.internal_to_external.len()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
    }

    fn max_id(&self) -> PointOffsetType {
        self.max_internal_id
    }

    fn mapping_flusher(&self) -> Flusher {
        self.mapping_db_wrapper.flusher()
    }

    fn versions_flusher(&self) -> Flusher {
        self.versions_db_wrapper.flusher()
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
    fn test_serializaton() {
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
}
