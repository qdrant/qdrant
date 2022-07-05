use crate::common::rocksdb_operations::{
    Database, DatabaseColumn, DatabaseIterationResult, DB_PAYLOAD_CF,
};
use crate::types::{Payload, PayloadKeyTypeRef, PointOffsetType};

use serde_json::Value;

use crate::entry::entry_point::{OperationError, OperationResult};
use crate::payload_storage::PayloadStorage;

use atomic_refcell::AtomicRefCell;
use std::sync::Arc;

/// On-disk implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, does not keep payload in memory
pub struct OnDiskPayloadStorage {
    database: DatabaseColumn,
}

impl OnDiskPayloadStorage {
    pub fn open(database: Arc<AtomicRefCell<Database>>) -> OperationResult<Self> {
        let database = DatabaseColumn::new(database, DB_PAYLOAD_CF);
        Ok(OnDiskPayloadStorage { database })
    }

    pub fn remove_from_storage(&self, point_id: PointOffsetType) -> OperationResult<()> {
        self.database
            .remove(&serde_cbor::to_vec(&point_id).unwrap())
    }

    pub fn update_storage(
        &self,
        point_id: PointOffsetType,
        payload: &Payload,
    ) -> OperationResult<()> {
        self.database.put(
            &serde_cbor::to_vec(&point_id).unwrap(),
            &serde_cbor::to_vec(payload).unwrap(),
        )
    }

    pub fn read_payload(&self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let key = serde_cbor::to_vec(&point_id).unwrap();
        self.database
            .get_pinned(&key, |raw| serde_cbor::from_slice(raw))?
            .transpose()
            .map_err(OperationError::from)
    }

    pub fn iter<F>(&self, mut callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        self.database.iterate_over_column_family(|(key, val)| {
            let point_offset: PointOffsetType = match serde_cbor::from_slice(key) {
                Ok(p) => p,
                Err(err) => return DatabaseIterationResult::Break(Err(OperationError::from(err))),
            };
            let payload: Payload = match serde_cbor::from_slice(val) {
                Ok(p) => p,
                Err(err) => return DatabaseIterationResult::Break(Err(OperationError::from(err))),
            };
            match callback(point_offset, &payload) {
                Ok(do_continue) => {
                    if do_continue {
                        DatabaseIterationResult::Continue
                    } else {
                        DatabaseIterationResult::Break(Ok(()))
                    }
                }
                Err(err) => DatabaseIterationResult::Break(Err(err)),
            }
        })
    }
}

impl PayloadStorage for OnDiskPayloadStorage {
    fn assign_all(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        self.update_storage(point_id, payload)
    }

    fn assign(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        let stored_payload = self.read_payload(point_id)?;
        match stored_payload {
            Some(mut point_payload) => {
                point_payload.merge(payload);
                self.update_storage(point_id, &point_payload)?
            }
            None => self.update_storage(point_id, payload)?,
        }
        Ok(())
    }

    fn payload(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        let payload = self.read_payload(point_id)?;
        match payload {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<Value>> {
        let stored_payload = self.read_payload(point_id)?;

        match stored_payload {
            Some(mut payload) => {
                let res = payload.remove(key);
                if res.is_some() {
                    self.update_storage(point_id, &payload)?;
                }
                Ok(res)
            }
            None => Ok(None),
        }
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let payload = self.read_payload(point_id)?;
        self.remove_from_storage(point_id)?;
        Ok(payload)
    }

    fn wipe(&mut self) -> OperationResult<()> {
        self.database.recreate_column_family()
    }

    fn flush(&self) -> OperationResult<()> {
        self.database.flush()
    }
}
