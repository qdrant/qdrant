use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::payload_storage::PayloadStorageRead;
use crate::types::OwnedPayloadRef;

pub struct PayloadProvider<P: PayloadStorageRead> {
    payload_storage: Arc<AtomicRefCell<P>>,
}

// Manual `Clone` impl: derive would require `P: Clone`, which is not the
// case for `PayloadStorageEnum`. Cloning the provider is just an `Arc`
// bump, no `P: Clone` needed.
impl<P: PayloadStorageRead> Clone for PayloadProvider<P> {
    fn clone(&self) -> Self {
        Self {
            payload_storage: Arc::clone(&self.payload_storage),
        }
    }
}

impl<P: PayloadStorageRead> PayloadProvider<P> {
    pub fn new(payload_storage: Arc<AtomicRefCell<P>>) -> Self {
        Self { payload_storage }
    }

    pub fn with_payload<F, G>(
        &self,
        point_id: PointOffsetType,
        callback: F,
        hw_counter: &HardwareCounterCell,
    ) -> G
    where
        F: FnOnce(OwnedPayloadRef) -> G,
    {
        let guard = self.payload_storage.borrow();
        // ToDo(uio): expose error instead of panic
        // Same panic-on-error policy as the previous Mmap branch -- preserves
        // behaviour. Currently it is possible for `payload_ref` to fail with
        // Err in the on-disk case, but this only happens if the disk or
        // storage is corrupted, in which case the service is no longer
        // usable. Tolerable to panic; downside is the API user is not
        // notified of the failure -- it just times out. The alternative
        // would be to rewrite condition checking to support error reporting,
        // which would require pervasive changes.
        let payload = guard
            .payload_ref(point_id, hw_counter)
            .unwrap_or_else(|err| panic!("Payload storage is corrupted: {err}"));
        callback(payload)
    }
}
