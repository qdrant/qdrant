//! The read surface: [`DiskMappingsSource`] and [`IdTrackerRead`] impls.

use common::bitvec::BitSlice;
use common::generic_consts::{Random, Sequential};
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{ReadRange, UniversalRead};

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::mappings::{
    DiskMappingsSource, log_lookup_err, log_lookup_err_batch, resolve_external_ids_batch,
};
use crate::id_tracker::disk_id_tracker::reader::DiskMappingReader;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

impl<S: UniversalRead> DiskMappingsSource for ReadOnlyDiskIdTracker<S> {
    type Backend = S;

    fn mapping_reader(&self) -> &DiskMappingReader<S> {
        &self.reader
    }

    /// A single lazy `get_bit` on the on-disk deleted file — never loads the
    /// full set. Out-of-range offsets count as deleted; storage errors
    /// propagate.
    fn point_deleted(&self, offset: PointOffsetType) -> OperationResult<bool> {
        Ok(self
            .deleted_file
            .get_bit(u64::from(offset))?
            .unwrap_or(true))
    }

    /// One pipelined pass over the on-disk deleted file (shared `u64` elements
    /// deduplicated) instead of a `get_bit` round-trip per point. Still no
    /// full-set load. Out-of-range offsets are treated as deleted.
    fn points_deleted_batch(&self, offsets: &[PointOffsetType]) -> OperationResult<Vec<bool>> {
        let bit_indices: Vec<u64> = offsets.iter().map(|&offset| u64::from(offset)).collect();
        Ok(self
            .deleted_file
            .get_bits_batch(&bit_indices)?
            .into_iter()
            .map(|bit| bit.unwrap_or(true))
            .collect())
    }

    fn deleted_bitslice(&self) -> OperationResult<&BitSlice> {
        Ok(self.deleted_full()?.as_bitslice())
    }
}

impl<S: UniversalRead> IdTrackerRead for ReadOnlyDiskIdTracker<S> {
    type Backend = S;

    fn point_mappings(&self) -> PointMappingsRefEnum<'_, Self::Backend> {
        PointMappingsRefEnum::Disk(self.mappings_ref_lossy())
    }

    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        if u64::from(internal_id) >= self.versions_len {
            return None;
        }
        match self.versions.read::<Random>(ReadRange {
            byte_offset: u64::from(internal_id) * size_of::<SeqNumberType>() as u64,
            length: 1,
        }) {
            Ok(values) => values.first().copied(),
            Err(err) => {
                log::error!("disk id tracker version read failed: {err}");
                None
            }
        }
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        _deferred_behavior: DeferredBehavior,
    ) -> Option<PointOffsetType> {
        log_lookup_err(self.resolve_internal(external_id))
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        log_lookup_err(self.resolve_external(internal_id))
    }

    fn external_ids_batch(&self, internal_ids: &[PointOffsetType]) -> Vec<Option<PointIdType>> {
        log_lookup_err_batch(
            self.resolve_external_batch(internal_ids),
            internal_ids.len(),
        )
    }

    /// One pipelined pass over the versions file instead of a read per point.
    /// Out-of-range offsets yield `None`; on a storage error the remaining
    /// slots stay `None` (the batch analogue of the single-lookup fallback).
    fn internal_versions_batch(
        &self,
        internal_ids: &[PointOffsetType],
    ) -> Vec<Option<SeqNumberType>> {
        let mut results: Vec<Option<SeqNumberType>> = vec![None; internal_ids.len()];

        let ranges = internal_ids
            .iter()
            .enumerate()
            .filter(|&(_, &internal_id)| u64::from(internal_id) < self.versions_len)
            .map(|(slot, &internal_id)| {
                let range = ReadRange {
                    byte_offset: u64::from(internal_id) * size_of::<SeqNumberType>() as u64,
                    length: 1,
                };
                (slot, range)
            });
        let read = self
            .versions
            .read_batch::<Random, usize>(ranges, |slot, values| {
                results[slot] = values.first().copied();
                Ok(())
            });
        if let Err(err) = read {
            log::error!("disk id tracker batch version read failed: {err}");
        }

        results
    }

    /// Batched external→internal resolution; the behavior argument is ignored
    /// (as in [`internal_id_with_behavior`](IdTrackerRead::internal_id_with_behavior)).
    fn resolve_external_ids(
        &self,
        point_ids: &[PointIdType],
        _deferred_behavior: DeferredBehavior,
    ) -> (Vec<PointIdType>, Vec<PointOffsetType>) {
        resolve_external_ids_batch(self, point_ids)
    }

    fn total_point_count(&self) -> usize {
        self.reader.total_point_count() as usize
    }

    fn deleted_point_count(&self) -> usize {
        self.mappings_ref_lossy().deleted().count_ones()
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        self.mappings_ref_lossy().deleted()
    }

    fn is_deleted_point(&self, internal_id: PointOffsetType) -> bool {
        // Fail-safe on a storage error: treat the point as deleted (hide it).
        self.point_deleted(internal_id).unwrap_or_else(|err| {
            log::error!("disk id tracker deleted check failed: {err}");
            true
        })
    }

    fn name(&self) -> &'static str {
        "read-only disk id tracker"
    }

    /// Reads the whole versions file at once: this runs only on the
    /// cleanup-on-open path, which drains the iteration anyway.
    fn iter_internal_versions(
        &self,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        let versions = self
            .versions
            .read::<Sequential>(ReadRange {
                byte_offset: 0,
                length: self.versions_len,
            })?
            .into_owned();
        Ok(Box::new(versions.into_iter().enumerate().map(
            |(offset, version)| (offset as PointOffsetType, version),
        )))
    }
}
