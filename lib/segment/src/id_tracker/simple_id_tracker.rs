use std::collections::BTreeMap;
use std::sync::Arc;

use bincode;
use bitvec::prelude::{BitSlice, BitVec};
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
    deleted: BitVec,
    internal_to_external: Vec<PointIdType>,
    internal_to_version: Vec<SeqNumberType>,
    external_to_internal_num: BTreeMap<u64, PointOffsetType>,
    external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType>,
    mapping_db_wrapper: DatabaseColumnWrapper,
    versions_db_wrapper: DatabaseColumnWrapper,
}

impl SimpleIdTracker {
    pub fn open(store: Arc<RwLock<DB>>) -> OperationResult<Self> {
        let mut deleted = BitVec::new();
        let mut internal_to_external: Vec<PointIdType> = Default::default();
        let mut external_to_internal_num: BTreeMap<u64, PointOffsetType> = Default::default();
        let mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType> = Default::default();

        let mapping_db_wrapper = DatabaseColumnWrapper::new(store.clone(), DB_MAPPING_CF);
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
                match external_id {
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
        let versions_db_wrapper = DatabaseColumnWrapper::new(store, DB_VERSIONS_CF);
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

        Ok(SimpleIdTracker {
            deleted,
            internal_to_external,
            internal_to_version,
            external_to_internal_num,
            external_to_internal_uuid,
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
        match external_id {
            PointIdType::NumId(idx) => self.external_to_internal_num.get(&idx).copied(),
            PointIdType::Uuid(uuid) => self.external_to_internal_uuid.get(&uuid).copied(),
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        if let Some(deleted) = self.deleted.get(internal_id as usize) {
            if !deleted {
                return self.internal_to_external.get(internal_id as usize).copied();
            }
        }
        None
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        match external_id {
            PointIdType::NumId(idx) => {
                self.external_to_internal_num.insert(idx, internal_id);
            }
            PointIdType::Uuid(uuid) => {
                self.external_to_internal_uuid.insert(uuid, internal_id);
            }
        }

        let internal_id = internal_id as usize;
        if internal_id >= self.internal_to_external.len() {
            self.internal_to_external
                .resize(internal_id + 1, PointIdType::NumId(u64::MAX));
        }
        if internal_id >= self.deleted.len() {
            self.deleted.resize(internal_id + 1, true);
        }
        self.internal_to_external[internal_id] = external_id;
        self.deleted.set(internal_id, false);

        self.mapping_db_wrapper.put(
            Self::store_key(&external_id),
            bincode::serialize(&internal_id).unwrap(),
        )?;
        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        let internal_id = match &external_id {
            PointIdType::NumId(idx) => self.external_to_internal_num.remove(idx),
            PointIdType::Uuid(uuid) => self.external_to_internal_uuid.remove(uuid),
        };
        if let Some(internal_id) = internal_id {
            self.deleted.set(internal_id as usize, true);
            self.internal_to_external[internal_id as usize] = PointIdType::NumId(u64::MAX);
        }
        self.mapping_db_wrapper
            .remove(Self::store_key(&external_id))?;
        self.versions_db_wrapper
            .remove(Self::store_key(&external_id))?;
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        let iter_num = self
            .external_to_internal_num
            .keys()
            .copied()
            .map(PointIdType::NumId);
        let iter_uuid = self
            .external_to_internal_uuid
            .keys()
            .copied()
            .map(PointIdType::Uuid);
        // order is important here, we want to iterate over the u64 ids first
        Box::new(iter_num.chain(iter_uuid))
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(
            (0..self.internal_to_external.len() as PointOffsetType)
                .filter(move |i| !self.deleted[*i as usize]),
        )
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        let full_num_iter = || {
            self.external_to_internal_num
                .iter()
                .map(|(k, v)| (PointIdType::NumId(*k), *v))
        };
        let offset_num_iter = |offset: u64| {
            self.external_to_internal_num
                .range(offset..)
                .map(|(k, v)| (PointIdType::NumId(*k), *v))
        };
        let full_uuid_iter = || {
            self.external_to_internal_uuid
                .iter()
                .map(|(k, v)| (PointIdType::Uuid(*k), *v))
        };
        let offset_uuid_iter = |offset: Uuid| {
            self.external_to_internal_uuid
                .range(offset..)
                .map(|(k, v)| (PointIdType::Uuid(*k), *v))
        };

        match external_id {
            None => {
                let iter_num = full_num_iter();
                let iter_uuid = full_uuid_iter();
                // order is important here, we want to iterate over the u64 ids first
                Box::new(iter_num.chain(iter_uuid))
            }
            Some(offset) => match offset {
                PointIdType::NumId(idx) => {
                    // Because u64 keys are less that uuid key, we can just use the full iterator for uuid
                    let iter_num = offset_num_iter(idx);
                    let iter_uuid = full_uuid_iter();
                    // order is important here, we want to iterate over the u64 ids first
                    Box::new(iter_num.chain(iter_uuid))
                }
                PointIdType::Uuid(uuid) => {
                    // if offset is a uuid, we can only iterate over uuids
                    Box::new(offset_uuid_iter(uuid))
                }
            },
        }
    }

    fn points_count(&self) -> usize {
        self.external_to_internal_num.len() + self.external_to_internal_uuid.len()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
    }

    fn internal_size(&self) -> usize {
        self.internal_to_external.len()
    }

    fn mapping_flusher(&self) -> Flusher {
        self.mapping_db_wrapper.flusher()
    }

    fn versions_flusher(&self) -> Flusher {
        self.versions_db_wrapper.flusher()
    }

    fn is_deleted(&self, key: PointOffsetType) -> bool {
        let key = key as usize;
        if key >= self.deleted.len() {
            return true;
        }
        self.deleted[key]
    }

    fn deleted_bitslice(&self) -> &BitSlice {
        &self.deleted
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
