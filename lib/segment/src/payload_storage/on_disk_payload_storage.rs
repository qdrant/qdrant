use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use rocksdb::{IteratorMode, DB};
use serde_json::Value;

use crate::common::rocksdb_operations::{db_options, db_write_options, DB_PAYLOAD_CF};
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef, PointOffsetType};

/// On-disk implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, does not keep payload in memory
pub struct OnDiskPayloadStorage {
    store: Arc<AtomicRefCell<DB>>,
}

impl OnDiskPayloadStorage {
    pub fn open(store: Arc<AtomicRefCell<DB>>) -> OperationResult<Self> {
        Ok(OnDiskPayloadStorage { store })
    }

    pub fn remove_from_storage(&self, point_id: PointOffsetType) -> OperationResult<()> {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref
            .cf_handle(DB_PAYLOAD_CF)
            .ok_or_else(|| OperationError::service_error("Payload storage column not found"))?;
        store_ref.delete_cf(cf_handle, serde_cbor::to_vec(&point_id).unwrap())?;
        Ok(())
    }

    pub fn update_storage(
        &self,
        point_id: PointOffsetType,
        payload: &Payload,
    ) -> OperationResult<()> {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref
            .cf_handle(DB_PAYLOAD_CF)
            .ok_or_else(|| OperationError::service_error("Payload storage column not found"))?;
        store_ref.put_cf_opt(
            cf_handle,
            serde_cbor::to_vec(&point_id).unwrap(),
            serde_cbor::to_vec(payload).unwrap(),
            &db_write_options(),
        )?;
        Ok(())
    }

    pub fn read_payload(&self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let key = serde_cbor::to_vec(&point_id).unwrap();
        let store_ref = self.store.borrow();
        let cf_handle = store_ref
            .cf_handle(DB_PAYLOAD_CF)
            .ok_or_else(|| OperationError::service_error("Payload storage column not found"))?;
        let payload = store_ref
            .get_pinned_cf(cf_handle, key)?
            .map(|raw| serde_cbor::from_slice(raw.as_ref()))
            .transpose()?;
        Ok(payload)
    }

    pub fn iter<F>(&self, mut callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref
            .cf_handle(DB_PAYLOAD_CF)
            .ok_or_else(|| OperationError::service_error("Payload storage column not found"))?;
        let iterator = store_ref.iterator_cf(cf_handle, IteratorMode::Start);
        for item in iterator {
            let (key, val) = item?;
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
        let mut store_ref = self.store.borrow_mut();
        store_ref.drop_cf(DB_PAYLOAD_CF)?;
        store_ref.create_cf(DB_PAYLOAD_CF, &db_options())?;
        Ok(())
    }

    fn flush(&self) -> OperationResult<()> {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref
            .cf_handle(DB_PAYLOAD_CF)
            .ok_or_else(|| OperationError::service_error("Payload storage column not found"))?;
        Ok(store_ref.flush_cf(cf_handle)?)
    }
}
