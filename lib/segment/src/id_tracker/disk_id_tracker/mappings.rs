//! Mapping reads written once over the `(mapping reader, deleted set)` pair:
//! point lookups ([`DiskMappingsSource`]) and ordered/random iteration
//! ([`DiskMappingsRef`]). Deleted points are never returned from either.

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::reader::{self, DiskMappingReader};
use crate::common::operation_error::OperationResult;
use crate::types::PointIdType;

/// The state a disk-resident mapping needs to answer reads: the lazy mapping
/// reader and a deleted source. The provided lookup helpers never return
/// deleted points and propagate storage errors.
pub trait DiskMappingsSource {
    type Backend: UniversalRead;

    /// The lazy mapping read core (`i2e`/`e2i`).
    fn mapping_reader(&self) -> &DiskMappingReader<Self::Backend>;

    /// Per-point deletion check. Must stay lazy — implementations must not
    /// materialize the full deleted set here. Out-of-range offsets count as
    /// deleted; storage errors propagate.
    fn point_deleted(
        &self,
        offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    /// The whole deleted set as a slice; may materialize it lazily, so its
    /// storage error propagates here.
    fn deleted_bitslice(&self, hw_counter: &HardwareCounterCell) -> OperationResult<&BitSlice>;

    /// A borrowed `(reader, deleted)` view for ordered/random iteration. Fails if
    /// the deleted set cannot be materialized.
    fn mappings_ref(
        &self,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<DiskMappingsRef<'_, Self::Backend>> {
        Ok(DiskMappingsRef::new(
            self.mapping_reader(),
            self.deleted_bitslice(hw_counter)?,
        ))
    }

    /// Like [`mappings_ref`](Self::mappings_ref), but infallible: on a
    /// deleted-set load error, logs and falls back to an empty deleted set.
    fn mappings_ref_lossy(
        &self,
        hw_counter: &HardwareCounterCell,
    ) -> DiskMappingsRef<'_, Self::Backend> {
        self.mappings_ref(hw_counter).unwrap_or_else(|err| {
            log::error!("disk id tracker deleted set load failed: {err}");
            DiskMappingsRef::new(self.mapping_reader(), BitSlice::empty())
        })
    }

    /// External→internal, excluding deleted points. Storage errors propagate.
    fn resolve_internal(
        &self,
        external_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PointOffsetType>> {
        let Some(offset) = self.mapping_reader().lookup(external_id, hw_counter)? else {
            return Ok(None);
        };
        // The immutable runs still carry points deleted after build; re-check.
        Ok((!self.point_deleted(offset, hw_counter)?).then_some(offset))
    }

    /// Internal→external, excluding deleted points. Storage errors propagate.
    fn resolve_external(
        &self,
        offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<PointIdType>> {
        if self.point_deleted(offset, hw_counter)? {
            Ok(None)
        } else {
            self.mapping_reader().external_id(offset, hw_counter)
        }
    }

    /// Batched external→internal resolution streamed through `on_live`: one
    /// pipelined mapping-lookup pass ([`lookup_batch`](DiskMappingReader::lookup_batch)),
    /// with a per-point deleted check dropping points tombstoned after build.
    /// Each surviving `(id, offset)` is delivered in read-completion order; no
    /// buffer is built.
    ///
    /// The deleted check is not batched: the set is resident (or the on-disk
    /// file is prefetched whole for the read-only tracker, since other reads
    /// need it too), so each check is a cheap in-memory bit test — pipelining
    /// it would buy nothing.
    fn resolve_internal_batch(
        &self,
        external_ids: impl IntoIterator<Item = PointIdType>,
        on_live: impl FnMut(PointIdType, PointOffsetType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;
}

/// A borrowed `(reader, deleted)` view; a `Copy` pair of references whose
/// iteration never yields deleted points.
pub struct DiskMappingsRef<'a, S: UniversalRead> {
    reader: &'a DiskMappingReader<S>,
    deleted: &'a BitSlice,
}

// Hand-written so `Copy` doesn't require `S: Copy` (this only holds references).
impl<S: UniversalRead> Clone for DiskMappingsRef<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<S: UniversalRead> Copy for DiskMappingsRef<'_, S> {}

impl<'a, S: UniversalRead> DiskMappingsRef<'a, S> {
    pub fn new(reader: &'a DiskMappingReader<S>, deleted: &'a BitSlice) -> Self {
        Self { reader, deleted }
    }

    pub fn deleted(self) -> &'a BitSlice {
        self.deleted
    }

    pub fn total_point_count(self) -> usize {
        self.reader.total_point_count() as usize
    }

    pub fn iter_external(
        self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = PointIdType> + 'a {
        self.reader
            .iter_from(None, hw_counter)
            .filter(live_filter(self.deleted))
            .map(|(external_id, _)| external_id)
    }

    pub fn iter_internal(self) -> impl Iterator<Item = PointOffsetType> + 'a {
        let deleted = self.deleted;
        let total = self.reader.total_point_count() as PointOffsetType;
        (0..total).filter(move |&offset| !deleted.get_bit(offset as usize).unwrap_or(false))
    }

    pub fn iter_from(
        self,
        external_id: Option<PointIdType>,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        self.reader
            .iter_from(external_id, hw_counter)
            .filter(live_filter(self.deleted))
    }

    pub fn iter_random(
        self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        reader::iter_random(self.reader, self.deleted, hw_counter)
    }
}

/// Predicate keeping only `(id, offset)` pairs whose offset is not deleted.
fn live_filter(deleted: &BitSlice) -> impl Fn(&(PointIdType, PointOffsetType)) -> bool + '_ {
    move |&(_, offset)| !deleted.get_bit(offset as usize).unwrap_or(false)
}

/// The one place a disk lookup error is dropped: logged and turned into `None`
/// to satisfy the `Option`-returning `IdTrackerRead` boundary.
pub fn log_lookup_err<T>(result: OperationResult<Option<T>>) -> Option<T> {
    result.unwrap_or_else(|err| {
        log::error!("disk id tracker lookup failed: {err}");
        None
    })
}
