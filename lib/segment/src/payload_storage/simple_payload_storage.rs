use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, DB_PAYLOAD_CF};
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::{Payload, PointOffsetType};
use crate::utils;

/// In-memory implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, but only uses this storage during the initial load
pub struct SimplePayloadStorage {
    pub(crate) payload: HashMap<PointOffsetType, Payload>,
    pub(crate) db_wrapper: DatabaseColumnWrapper,
}

impl SimplePayloadStorage {
    pub fn open(database: Arc<RwLock<DB>>) -> OperationResult<Self> {
        let mut payload_map: HashMap<PointOffsetType, Payload> = Default::default();

        let db_wrapper = DatabaseColumnWrapper::new(database, DB_PAYLOAD_CF);
        let mut mem = utils::mem::Mem::new();

        for (iter, (key, val)) in db_wrapper.lock_db().iter()?.enumerate() {
            if iter % 25 == 0 {
                utils::mem::assert_available_memory_during_segment_load(&mem, 0, 10)?;
                mem.refresh();
            }

            let point_id: PointOffsetType = serde_cbor::from_slice(&key)
                .map_err(|_| OperationError::service_error("cannot deserialize point id"))?;
            let payload: Payload = serde_cbor::from_slice(&val)
                .map_err(|_| OperationError::service_error("cannot deserialize payload"))?;
            payload_map.insert(point_id, payload);

            mem.refresh();
        }

        Ok(SimplePayloadStorage {
            payload: payload_map,
            db_wrapper,
        })
    }

    pub(crate) fn update_storage(&self, point_id: &PointOffsetType) -> OperationResult<()> {
        match self.payload.get(point_id) {
            None => self
                .db_wrapper
                .remove(serde_cbor::to_vec(&point_id).unwrap()),
            Some(payload) => self.db_wrapper.put(
                serde_cbor::to_vec(&point_id).unwrap(),
                serde_cbor::to_vec(payload).unwrap(),
            ),
        }
    }

    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }

    pub fn iter<F>(&self, mut callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        for (key, val) in self.payload.iter() {
            let do_continue = callback(*key, val)?;
            if !do_continue {
                return Ok(());
            }
        }
        Ok(())
    }
}
