use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::types::{DeferredBehavior, PointOffsetType};

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
            std::iter::once(((), point_offset)),
            hw_counter,
            |(), _, vector_internal| {
                result = Some(vector_internal);
            },
        )?;
        Ok(result)
    }

    /// Retrieve multiple vectors by internal ID.
    ///
    /// Each input is tagged with caller-supplied user data `U` (any `Copy`
    /// type — the external id, the input position, …). The data is threaded
    /// back to the callback unchanged, so callers can map results into a
    /// parallel input array without keeping a separate `offset → ...` lookup
    /// table. Deleted points are filtered out lazily — entries with deleted
    /// vectors or deleted points are simply not delivered to the callback.
    pub fn vectors_by_offsets<U: Copy + common::universal_io::UserData>(
        &self,
        vector_name: &VectorName,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        hw_counter: &HardwareCounterCell,
        mut callback: impl FnMut(U, PointOffsetType, VectorInternal),
    ) -> OperationResult<()> {
        let vector_storage = self.vector_storage_for(vector_name)?;
        let live_keys = self.filter_live_keys(&*vector_storage, keys);

        vector_storage.read_vectors::<Random, U>(
            live_keys,
            |user_data, point_offset, cow_vector| {
                if vector_storage.is_on_disk() {
                    hw_counter
                        .vector_io_read()
                        .incr_delta(cow_vector.estimate_size_in_bytes());
                }
                callback(user_data, point_offset, cow_vector.to_owned());
            },
        );

        Ok(())
    }

    /// Byte-blob analogue of [`Self::vectors_by_offsets`]: yields each vector as
    /// storage-native bytes (`Vec<u8>`) instead of a decoded [`VectorInternal`],
    /// avoiding a lossy round-trip. Deleted vectors/points and offsets without
    /// a stored value are skipped lazily, like in the decoded twin; storage
    /// read failures propagate as errors.
    pub fn vector_bytes_by_offsets<U: Copy + common::universal_io::UserData>(
        &self,
        vector_name: &VectorName,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        hw_counter: &HardwareCounterCell,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        let vector_storage = self.vector_storage_for(vector_name)?;
        let live_keys = self.filter_live_keys(&*vector_storage, keys);

        vector_storage
            .read_vector_bytes::<Random, U>(live_keys, |user_data, point_offset, bytes| {
                if vector_storage.is_on_disk() {
                    hw_counter.vector_io_read().incr_delta(bytes.len());
                }
                callback(user_data, point_offset, bytes);
            })
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to read raw bytes of vector {vector_name:?}: {err}"
                ))
            })
    }

    /// Resolve the vector storage for a named vector, validating the name.
    fn vector_storage_for(&self, vector_name: &VectorName) -> OperationResult<TVD::StorageRef<'_>> {
        check_vector_name(vector_name, self.segment_config)?;
        let vector_data = self
            .vector_data
            .get(vector_name)
            .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))?;
        Ok(vector_data.vector_storage())
    }

    /// Shared liveness filter of [`Self::vectors_by_offsets`] and
    /// [`Self::vector_bytes_by_offsets`]: lazily drops out-of-bounds offsets
    /// (with a debug assert) and offsets whose vector or point is deleted.
    /// The user-data tag rides alongside each offset, so no parallel
    /// `Vec<(orig_idx, offset)>` is needed.
    fn filter_live_keys<'a, U: Copy, S: VectorStorageRead + ?Sized>(
        &'a self,
        vector_storage: &'a S,
        keys: impl IntoIterator<Item = (U, PointOffsetType), IntoIter: 'a>,
    ) -> impl Iterator<Item = (U, PointOffsetType)> + 'a {
        let total_vectors = vector_storage.total_vector_count();
        let id_tracker = self.id_tracker;
        keys.into_iter().filter(move |&(_, point_offset)| {
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
        })
    }

    /// Retrieve a named vector for an external point ID.
    pub fn vector(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        // Single-point retrieval observes the visible snapshot; deferred
        // mutations stay hidden until the optimizer rolls a fresh segment.
        self.vector_with_behavior(
            vector_name,
            point_id,
            DeferredBehavior::VisibleOnly,
            hw_counter,
        )
    }

    /// Retrieve a named vector for an external point ID with explicit deferred
    /// semantics. With [`DeferredBehavior::WithDeferred`] this also resolves
    /// points whose only head is a deferred mutation (invisible to reads) —
    /// used by the copy-on-write move path which must relocate deferred points.
    pub fn vector_with_behavior(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        let internal_id = self.lookup_internal_id(point_id, deferred_behavior)?;
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
