use crate::common::rocksdb_operations::{Database, DatabaseIterationResult, DB_PAYLOAD_CF};
use crate::types::{Payload, PointOffsetType};
use atomic_refcell::AtomicRefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::entry::entry_point::OperationResult;

/// In-memory implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, but only uses this storage during the initial load
pub struct SimplePayloadStorage {
    pub(crate) payload: HashMap<PointOffsetType, Payload>,
    pub(crate) database: Arc<AtomicRefCell<Database>>,
}

impl SimplePayloadStorage {
    pub fn open(database: Arc<AtomicRefCell<Database>>) -> OperationResult<Self> {
        let mut payload_map: HashMap<PointOffsetType, Payload> = Default::default();

        database
            .borrow()
            .iterate_over_column_family(DB_PAYLOAD_CF, |(key, val)| {
                let point_id: PointOffsetType = serde_cbor::from_slice(key).unwrap();
                let payload: Payload = serde_cbor::from_slice(val).unwrap();
                payload_map.insert(point_id, payload);
                DatabaseIterationResult::Continue
            })?;

        Ok(SimplePayloadStorage {
            payload: payload_map,
            database,
        })
    }

    pub(crate) fn update_storage(&self, point_id: &PointOffsetType) -> OperationResult<()> {
        match self.payload.get(point_id) {
            None => self
                .database
                .borrow()
                .remove(DB_PAYLOAD_CF, &serde_cbor::to_vec(&point_id).unwrap()),
            Some(payload) => self.database.borrow().put(
                DB_PAYLOAD_CF,
                &serde_cbor::to_vec(&point_id).unwrap(),
                &serde_cbor::to_vec(payload).unwrap(),
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
