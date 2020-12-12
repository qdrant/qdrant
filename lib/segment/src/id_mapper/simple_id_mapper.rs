use std::collections::HashMap;
use crate::types::{PointOffsetType, PointIdType};
use crate::id_mapper::id_mapper::IdMapper;
use crate::entry::entry_point::OperationResult;
use bincode;
use std::path::Path;
use rocksdb::{Options, DB, IteratorMode};

/// Since sled is used for reading only during the initialization, large read cache is not required
const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb

pub struct SimpleIdMapper {
    internal_to_external: HashMap<PointOffsetType, PointIdType>,
    external_to_internal: HashMap<PointIdType, PointOffsetType>,
    store: DB,
}

impl SimpleIdMapper {

    pub fn open(path: &Path) -> OperationResult<Self> {
        let mut options: Options = Options::default();
        options.set_write_buffer_size(DB_CACHE_SIZE);
        options.create_if_missing(true);
        let store = DB::open(&options, path)?;

        let mut internal_to_external: HashMap<PointOffsetType, PointIdType> = Default::default();
        let mut external_to_internal: HashMap<PointIdType, PointOffsetType> = Default::default();

        for (key, val) in store.iterator(IteratorMode::Start) {
            let external_id: PointIdType = bincode::deserialize(&key).unwrap();
            let internal_id: PointOffsetType = bincode::deserialize(&val).unwrap();
            internal_to_external.insert(internal_id, external_id);
            external_to_internal.insert(external_id, internal_id);
        }

        Ok(SimpleIdMapper {
            internal_to_external,
            external_to_internal,
            store,
        })
    }
}


impl IdMapper for SimpleIdMapper {
    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.external_to_internal.get(&external_id).cloned()
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.internal_to_external.get(&internal_id).cloned()
    }

    fn set_link(&mut self, external_id: PointIdType, internal_id: PointOffsetType) -> OperationResult<()> {
        self.external_to_internal.insert(external_id, internal_id);
        self.internal_to_external.insert(internal_id, external_id);

        self.store.put(
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
        self.store.delete(bincode::serialize(&external_id).unwrap())?;
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item=PointIdType> + '_> {
        Box::new(self.external_to_internal.keys().cloned())
    }

    fn flush(&self) -> OperationResult<()> {
        Ok(self.store.flush()?)
    }
}

