use common::ambient::hw::HwMetric;
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::payload_storage::PayloadStorageRead;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::{IoBackend, OwnedPayloadRef, Payload};

impl<S: UniversalRead> PayloadStorageRead for ReadOnlyPayloadStorage<S> {
    fn io_backend(&self) -> Option<IoBackend> {
        IoBackend::from_universal_kind(S::kind())
    }

    fn get(&self, point_offset: PointOffsetType) -> OperationResult<Payload> {
        match self.storage.get_value::<Random>(point_offset)? {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn get_sequential(&self, point_offset: PointOffsetType) -> OperationResult<Payload> {
        match self.storage.get_value::<Sequential>(point_offset)? {
            Some(payload) => Ok(payload),
            None => Ok(Default::default()),
        }
    }

    fn payload_ref(&self, point_offset: PointOffsetType) -> OperationResult<OwnedPayloadRef<'_>> {
        let payload = self.get(point_offset)?;
        Ok(OwnedPayloadRef::from(payload))
    }

    fn read_payloads<P: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, Payload) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // TODO: `hw_counter`!?

        self.storage.read_values::<P, _, _>(
            point_offsets,
            |user_data, _, payload| {
                let payload = payload.unwrap_or_default();
                callback(user_data, payload)
            },
            Some(HwMetric::PayloadIoRead),
        )
    }

    fn read_payloads_raw<P: AccessPattern, U: common::universal_io::UserData>(
        &self,
        point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, Option<&[u8]>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.storage.read_values_bytes::<P, _, _>(
            point_offsets,
            |user_data, _, bytes| callback(user_data, bytes),
            Some(HwMetric::PayloadIoRead),
        )
    }

    fn iter<F>(&self, mut callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        let max_id = self.storage.max_point_offset()?;
        self.storage.iter(
            max_id,
            |point_id, payload| callback(point_id, &payload),
            HwMetric::PayloadIoRead,
        )
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        Ok(self.storage.get_storage_size_bytes())
    }

    fn is_on_disk(&self) -> bool {
        self.storage.is_on_disk()
    }
}
