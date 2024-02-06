use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, DB_PAYLOAD_CF};
use crate::common::Flusher;
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef};

/// On-disk implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, does not keep payload in memory
pub struct OnDiskPayloadStorage {
    db_wrapper: DatabaseColumnWrapper,
}

impl OnDiskPayloadStorage {
    pub fn open(database: Arc<RwLock<DB>>) -> OperationResult<Self> {
        let db_wrapper = DatabaseColumnWrapper::new(database, DB_PAYLOAD_CF);
        Ok(OnDiskPayloadStorage { db_wrapper })
    }

    pub fn remove_from_storage(&self, point_id: PointOffsetType) -> OperationResult<()> {
        self.db_wrapper
            .remove(serde_cbor::to_vec(&point_id).unwrap())
    }

    pub fn update_storage(
        &self,
        point_id: PointOffsetType,
        payload: &Payload,
    ) -> OperationResult<()> {
        self.db_wrapper.put(
            serde_cbor::to_vec(&point_id).unwrap(),
            serde_cbor::to_vec(payload).unwrap(),
        )
    }

    pub fn read_payload(&self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let key = serde_cbor::to_vec(&point_id).unwrap();
        self.db_wrapper
            .get_pinned(&key, |raw| serde_cbor::from_slice(raw))?
            .transpose()
            .map_err(OperationError::from)
    }

    pub fn iter<F>(&self, mut callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        for (key, val) in self.db_wrapper.lock_db().iter()? {
            let do_continue = callback(
                serde_cbor::from_slice(&key)?,
                &serde_cbor::from_slice(&val)?,
            )?;
            if !do_continue {
                return Ok(());
            }
        }
        Ok(())
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

    fn assign_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &str,
    ) -> OperationResult<()> {
        let stored_payload = self.read_payload(point_id)?;
        match stored_payload {
            Some(mut point_payload) => {
                point_payload.merge_by_key(payload, key)?;
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
    ) -> OperationResult<Vec<Value>> {
        let stored_payload = self.read_payload(point_id)?;

        match stored_payload {
            Some(mut payload) => {
                let res = payload.remove(key);
                if !res.is_empty() {
                    self.update_storage(point_id, &payload)?;
                }
                Ok(res)
            }
            None => Ok(vec![]),
        }
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let payload = self.read_payload(point_id)?;
        self.remove_from_storage(point_id)?;
        Ok(payload)
    }

    fn wipe(&mut self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }
}
