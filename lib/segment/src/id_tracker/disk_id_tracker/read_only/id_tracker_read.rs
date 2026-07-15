//! The read surface: [`DiskMappingsSource`] and [`IdTrackerRead`] impls.

use common::bitvec::BitSlice;
use common::generic_consts::{Random, Sequential};
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{ReadRange, UniversalRead};

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::mappings::{
    DiskMappingsSource, log_lookup_err, log_lookup_err_batch,
};
use crate::id_tracker::disk_id_tracker::reader::DiskMappingReader;
use crate::id_tracker::{IdTrackerRead, PointIdBatch, PointMappingsRefEnum};
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

    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
    ) -> Vec<Option<PointIdType>> {
        let internal_ids: Vec<PointOffsetType> = internal_ids.into_iter().collect();
        log_lookup_err_batch(
            self.resolve_external_batch(&internal_ids),
            internal_ids.len(),
        )
    }

    /// One pipelined pass over the versions file instead of a read per point,
    /// streaming each `(internal_id, version)` to `callback` as its read
    /// completes. The input is walked once and nothing is buffered; out-of-range
    /// offsets are skipped and a storage error propagates.
    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        mut callback: impl FnMut(PointOffsetType, SeqNumberType),
    ) -> OperationResult<()> {
        // Each read is tagged with its `internal_id` so the callback can pair it
        // with the version; the range iterator stays lazy (no collect).
        let ranges = internal_ids
            .into_iter()
            .filter(|&internal_id| u64::from(internal_id) < self.versions_len)
            .map(|internal_id| {
                let range = ReadRange {
                    byte_offset: u64::from(internal_id) * size_of::<SeqNumberType>() as u64,
                    length: 1,
                };
                (internal_id, range)
            });
        self.versions
            .read_batch::<Random, PointOffsetType>(ranges, |internal_id, values| {
                if let Some(&version) = values.first() {
                    callback(internal_id, version);
                }
                Ok(())
            })?;

        Ok(())
    }

    /// Batched external→internal resolution; the behavior argument is ignored
    /// (as in [`internal_id_with_behavior`](IdTrackerRead::internal_id_with_behavior)).
    fn resolve_external_ids(
        &self,
        point_ids: impl PointIdBatch,
        _deferred_behavior: DeferredBehavior,
        callback: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        self.resolve_internal_batch(point_ids, callback)
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
