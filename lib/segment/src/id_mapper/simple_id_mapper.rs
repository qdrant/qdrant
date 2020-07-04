use std::collections::HashMap;
use crate::types::{PointOffsetType, PointIdType};
use crate::id_mapper::id_mapper::IdMapper;

pub struct SimpleIdMapper {
    pub internal_to_external: HashMap<PointOffsetType, PointIdType>,
    pub external_to_internal: HashMap<PointIdType, PointOffsetType>,
}



impl IdMapper for SimpleIdMapper {
    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        return match self.external_to_internal.get(&external_id) {
            Some(x) => Some(*x),
            None => None
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        return match self.internal_to_external.get(&internal_id) {
            Some(x) => Some(*x),
            None => None
        }
    }

    fn set_link(&mut self, external_id: PointIdType, internal_id: PointOffsetType) {
        self.external_to_internal.insert(external_id, internal_id);
        self.internal_to_external.insert(internal_id, external_id);
    }

    fn drop(&mut self, external_id: PointIdType) {
        let internal_id = self.external_to_internal.remove(&external_id);
        match internal_id {
            Some(x) => self.internal_to_external.remove(&x),
            None => None
        };
    }
}

