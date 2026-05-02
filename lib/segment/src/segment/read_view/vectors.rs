use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;

use crate::common::check_vector_name;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::VectorInternal;
use crate::id_tracker::IdTrackerRead;
use crate::index::{PayloadIndexRead, VectorIndexRead};
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{PointIdType, VectorName};
use crate::vector_storage::VectorStorageRead;

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Retrieve vector by internal ID.
    ///
    /// Returns `None` if the vector does not exist or is deleted.
    pub fn vector_by_offset(
        &self,
        vector_name: &VectorName,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        let mut result = None;
        self.vectors_by_offsets(
            vector_name,
            std::iter::once(point_offset),
            hw_counter,
            |_, vector_internal| {
                result = Some(vector_internal);
            },
        )?;
        Ok(result)
    }

    /// Retrieve multiple vectors by internal ID.
    pub fn vectors_by_offsets(
        &self,
        vector_name: &VectorName,
        point_offsets: impl IntoIterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        mut callback: impl FnMut(PointOffsetType, VectorInternal),
    ) -> OperationResult<()> {
        check_vector_name(vector_name, self.segment_config)?;
        let vector_data = self
            .vector_data
            .get(vector_name)
            .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))?;
        let vector_storage = vector_data.vector_storage();
        let total_vectors = vector_storage.total_vector_count();

        let id_tracker = self.id_tracker;
        let non_deleted_offsets = point_offsets.into_iter().filter(|&point_offset| {
            if total_vectors <= point_offset as usize {
                debug_assert!(
                    false,
                    "Vector storage is inconsistent, total_vector_count: {total_vectors}, point_offset: {point_offset}, external_id: {:?}",
                    id_tracker.external_id(point_offset),
                );
                return false;
            }

            let is_vector_deleted = vector_storage.is_deleted_vector(point_offset);
            let is_point_deleted = id_tracker.is_deleted_point(point_offset);
            !is_vector_deleted && !is_point_deleted
        });

        vector_storage.read_vectors::<Random>(non_deleted_offsets, |point_offset, cow_vector| {
            if vector_storage.is_on_disk() {
                hw_counter
                    .vector_io_read()
                    .incr_delta(cow_vector.estimate_size_in_bytes());
            }
            callback(point_offset, cow_vector.to_owned());
        });

        Ok(())
    }

    /// Retrieve named vectors for a list of external point IDs, invoking the
    /// callback for each (point_id, vector) pair.
    pub fn read_vectors(
        &self,
        vector_name: &VectorName,
        point_ids: &[PointIdType],
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
        mut callback: impl FnMut(PointIdType, VectorInternal),
    ) -> OperationResult<()> {
        let mut error = None;
        let internal_ids = point_ids
            .iter()
            .copied()
            .stop_if(is_stopped)
            .filter_map(|point_id| match self.lookup_internal_id(point_id) {
                Ok(point_offset) => Some(point_offset),
                Err(err) => {
                    error = Some(err);
                    None
                }
            });
        self.vectors_by_offsets(
            vector_name,
            internal_ids,
            hw_counter,
            |point_offset, vector_internal| {
                if let Some(point_id) = self.id_tracker.external_id(point_offset) {
                    callback(point_id, vector_internal);
                }
            },
        )?;
        if let Some(err) = error {
            return Err(err);
        }
        Ok(())
    }

    /// Retrieve a named vector for an external point ID.
    pub fn vector(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.vector_by_offset(vector_name, internal_id, hw_counter)
    }

    /// Size in bytes of all available vectors for the given vector name.
    pub fn available_vectors_size_in_bytes(
        &self,
        vector_name: &VectorName,
    ) -> OperationResult<usize> {
        check_vector_name(vector_name, self.segment_config)?;
        let vector_data = self
            .vector_data
            .get(vector_name)
            .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))?;
        let size = vector_data
            .vector_index()
            .size_of_searchable_vectors_in_bytes();
        Ok(size)
    }
}
