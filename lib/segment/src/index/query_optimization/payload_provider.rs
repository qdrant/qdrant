use std::ops::Deref;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;

use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::types::{OwnedPayloadRef, Payload};

#[derive(Clone)]
pub struct PayloadProvider {
    payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
    empty_payload: Payload,
}

impl PayloadProvider {
    pub fn new(payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>) -> Self {
        Self {
            payload_storage,
            empty_payload: Default::default(),
        }
    }

    pub fn with_payload<F, G>(&self, point_id: PointOffsetType, callback: F) -> G
    where
        F: FnOnce(OwnedPayloadRef) -> G,
    {
        let payload_storage_guard = self.payload_storage.borrow();
        let payload_ptr_opt = match payload_storage_guard.deref() {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                s.payload_ptr(point_id).map(|x| x.into())
            }
            PayloadStorageEnum::SimplePayloadStorage(s) => {
                s.payload_ptr(point_id).map(|x| x.into())
            }
            // Warn: Possible panic here
            // Currently, it is possible that `read_payload` fails with Err,
            // but it seems like a very rare possibility which might only happen
            // if something is wrong with disk or storage is corrupted.
            //
            // In both cases it means that service can't be of use any longer.
            // It is as good as dead. Therefore it is tolerable to just panic here.
            // Downside is - API user won't be notified of the failure.
            // It will just timeout.
            //
            // The alternative:
            // Rewrite condition checking code to support error reporting.
            // Which may lead to slowdown and assumes a lot of changes.
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s
                .read_payload(point_id)
                .unwrap_or_else(|err| panic!("Payload storage is corrupted: {err}"))
                .map(|x| x.into()),
        };

        let payload = if let Some(payload_ptr) = payload_ptr_opt {
            payload_ptr
        } else {
            (&self.empty_payload).into()
        };

        callback(payload)
    }
}
