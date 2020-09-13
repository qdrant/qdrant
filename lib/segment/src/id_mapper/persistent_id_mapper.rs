use sled::Db;
use std::path::Path;
use std::collections::HashMap;
use crate::types::{PointOffsetType, PointIdType};
use crate::id_mapper::id_mapper::IdMapper;
use crate::entry::entry_point::OperationResult;
use bincode;

pub struct PersistentIdMapper {
    internal_to_external: HashMap<PointOffsetType, PointIdType>,
    external_to_internal: HashMap<PointIdType, PointOffsetType>,
    store: Db
}


impl PersistentIdMapper {
    pub fn new(path: &Path) -> Self {
        let store = sled::open(path).unwrap();

        let mut internal_to_external: HashMap<PointOffsetType, PointIdType> = Default::default();
        let mut external_to_internal: HashMap<PointIdType, PointOffsetType> = Default::default();

        for record in store.iter() {
            let (key, val) = record.unwrap();
            let external_id: PointIdType = bincode::deserialize(&key).unwrap();
            let internal_id: PointOffsetType = bincode::deserialize(&val).unwrap();
            internal_to_external.insert(internal_id, external_id);
            external_to_internal.insert(external_id, internal_id);
        }

        PersistentIdMapper {
            internal_to_external,
            external_to_internal,
            store
        }
    }
}

impl IdMapper for PersistentIdMapper {
    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.external_to_internal.get(&external_id).cloned()
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.internal_to_external.get(&internal_id).cloned()
    }

    fn set_link(&mut self, external_id: PointIdType, internal_id: PointOffsetType) -> OperationResult<()> {
        self.external_to_internal.insert(external_id, internal_id);
        self.internal_to_external.insert(internal_id, external_id);

        self.store.insert(
            bincode::serialize(&external_id).unwrap(),
            bincode::serialize(&internal_id).unwrap())?;

        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        let internal_id = self.external_to_internal.remove(&external_id);
        match internal_id {
            Some(x) => self.internal_to_external.remove(&x),
            None => None
        };
        self.store.remove(bincode::serialize(&external_id).unwrap())?;
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item=PointIdType> + '_> {
        Box::new(self.external_to_internal.keys().cloned())
    }
}