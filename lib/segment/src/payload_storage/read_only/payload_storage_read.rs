use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{Random, Sequential};
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::payload_storage::PayloadStorageRead;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::{OwnedPayloadRef, Payload};

impl<S: UniversalRead> PayloadStorageRead for ReadOnlyPayloadStorage<S> {
    fn get(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self.storage.get_value::<Random>(point_offset, hw_counter)? {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn get_sequential(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self
            .storage
            .get_value::<Sequential>(point_offset, hw_counter)?
        {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn payload_ref(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<OwnedPayloadRef<'_>> {
        let payload = self.get(point_offset, hw_counter)?;
        Ok(OwnedPayloadRef::from(payload))
    }

    fn iter<F>(&self, mut callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        let max_id = self.storage.max_point_offset();
        self.storage.iter(
            max_id,
            |point_id, payload| callback(point_id, &payload),
            hw_counter.ref_payload_io_read_counter(),
        )
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        Ok(self.storage.get_storage_size_bytes())
    }

    fn is_on_disk(&self) -> bool {
        !self.populate
    }
}
