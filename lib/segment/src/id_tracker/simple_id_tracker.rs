use crate::common::rocksdb_operations::open_db_with_cf;
use crate::entry::entry_point::OperationResult;
use crate::id_tracker::points_iterator::PointsIterator;
use crate::id_tracker::IdTracker;
use crate::types::{ExtendedPointId, PointIdType, PointOffsetType, SeqNumberType};
use bincode;
use rocksdb::{IteratorMode, DB};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use uuid::Uuid;

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

const MAPPING_CF: &str = "mapping";
const VERSIONS_CF: &str = "versions";

pub struct SimpleIdTracker {
    internal_to_external: HashMap<PointOffsetType, PointIdType>,
    external_to_internal: BTreeMap<PointIdType, PointOffsetType>,
    external_to_version: HashMap<PointIdType, SeqNumberType>,
    max_internal_id: PointOffsetType,
    store: DB,
}

impl SimpleIdTracker {
    pub fn open(path: &Path) -> OperationResult<Self> {
        let store = open_db_with_cf(path, &[MAPPING_CF, VERSIONS_CF])?;

        let mut internal_to_external: HashMap<PointOffsetType, PointIdType> = Default::default();
        let mut external_to_internal: BTreeMap<PointIdType, PointOffsetType> = Default::default();
        let mut external_to_version: HashMap<PointIdType, SeqNumberType> = Default::default();
        let mut max_internal_id = 0;

        for (key, val) in
            store.iterator_cf(store.cf_handle(MAPPING_CF).unwrap(), IteratorMode::Start)
        {
            let external_id = Self::restore_key(&key);
            let internal_id: PointOffsetType = bincode::deserialize(&val).unwrap();
            internal_to_external.insert(internal_id, external_id);
            external_to_internal.insert(external_id, internal_id);
            max_internal_id = max_internal_id.max(internal_id);
        }

        for (key, val) in
            store.iterator_cf(store.cf_handle(VERSIONS_CF).unwrap(), IteratorMode::Start)
        {
            let external_id = Self::restore_key(&key);
            let version: SeqNumberType = bincode::deserialize(&val).unwrap();
            external_to_version.insert(external_id, version);
        }

        Ok(SimpleIdTracker {
            internal_to_external,
            external_to_internal,
            external_to_version,
            max_internal_id,
            store,
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
        self.store.put_cf(
            self.store.cf_handle(VERSIONS_CF).unwrap(),
            Self::store_key(&external_id),
            bincode::serialize(&version).unwrap(),
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

        self.store.put_cf(
            self.store.cf_handle(MAPPING_CF).unwrap(),
            Self::store_key(&external_id),
            bincode::serialize(&internal_id).unwrap(),
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
        self.store.delete_cf(
            self.store.cf_handle(MAPPING_CF).unwrap(),
            Self::store_key(&external_id),
        )?;
        self.store.delete_cf(
            self.store.cf_handle(VERSIONS_CF).unwrap(),
            Self::store_key(&external_id),
        )?;
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

    fn flush(&self) -> OperationResult<()> {
        self.store
            .flush_cf(self.store.cf_handle(MAPPING_CF).unwrap())?;
        self.store
            .flush_cf(self.store.cf_handle(VERSIONS_CF).unwrap())?;
        Ok(self.store.flush()?)
    }
}

impl PointsIterator for SimpleIdTracker {
    fn points_count(&self) -> usize {
        self.internal_to_external.len()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
    }

    fn max_id(&self) -> PointOffsetType {
        self.max_internal_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use serde::de::DeserializeOwned;
    use tempdir::TempDir;

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
        let dir = TempDir::new("storage_dir").unwrap();

        let mut id_tracker = SimpleIdTracker::open(dir.path()).unwrap();

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
