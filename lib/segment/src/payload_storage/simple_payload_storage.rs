use crate::common::rocksdb_operations::{db_write_options, DB_PAYLOAD_CF};
use crate::types::{Payload, PointOffsetType};
use atomic_refcell::AtomicRefCell;
use std::collections::HashMap;
use std::sync::Arc;

use rocksdb::{IteratorMode, DB};

use crate::entry::entry_point::OperationResult;

/// In-memory implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, but only uses this storage during the initial load
pub struct SimplePayloadStorage {
    pub(crate) payload: HashMap<PointOffsetType, Payload>,
    pub(crate) store: Arc<AtomicRefCell<DB>>,
}

impl SimplePayloadStorage {
    pub fn open(store: Arc<AtomicRefCell<DB>>) -> OperationResult<Self> {
        let mut payload_map: HashMap<PointOffsetType, Payload> = Default::default();

        {
            let store_ref = store.borrow();
            let cf_handle = store_ref.cf_handle(DB_PAYLOAD_CF).unwrap();
            for (key, val) in store_ref.iterator_cf(cf_handle, IteratorMode::Start) {
                let point_id: PointOffsetType = serde_cbor::from_slice(&key).unwrap();
                let payload: Payload = serde_cbor::from_slice(&val).unwrap();
                payload_map.insert(point_id, payload);
            }
        }

        Ok(SimplePayloadStorage {
            payload: payload_map,
            store,
        })
    }

    pub(crate) fn update_storage(&self, point_id: &PointOffsetType) -> OperationResult<()> {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(DB_PAYLOAD_CF).unwrap();
        match self.payload.get(point_id) {
            None => store_ref.delete_cf(cf_handle, serde_cbor::to_vec(&point_id).unwrap())?,
            Some(payload) => store_ref.put_cf_opt(
                cf_handle,
                serde_cbor::to_vec(&point_id).unwrap(),
                serde_cbor::to_vec(payload).unwrap(),
                &db_write_options(),
            )?,
        };
        Ok(())
    }

    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }
}
