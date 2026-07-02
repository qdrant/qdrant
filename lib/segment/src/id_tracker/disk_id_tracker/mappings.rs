//! Shared mapping-read behaviour for the two disk-resident id trackers.
//!
//! [`DiskIdTracker`](super::DiskIdTracker) and
//! [`ReadOnlyDiskIdTracker`](super::read_only::ReadOnlyDiskIdTracker) both hold a
//! [`DiskMappingReader`] plus a deleted set; they differ only in how the deleted
//! set is stored (a resident bitvec mutated in place vs a lazy `get_bit` over the
//! on-disk file) and how versions are kept.
//!
//! Everything that depends purely on `(mapping reader, deleted set)` is written
//! here once:
//!
//! - point lookups — [`resolve_internal`](DiskMappingsSource::resolve_internal) /
//!   [`resolve_external`](DiskMappingsSource::resolve_external), shared via the
//!   [`DiskMappingsSource`] trait each tracker implements;
//! - ordered/random iteration — [`DiskMappingsRef`], a concrete `(reader, deleted)`
//!   view held directly by [`PointMappingsRefEnum::Disk`], so there is no trait
//!   object at the mapping boundary.
//!
//! [`PointMappingsRefEnum::Disk`]: crate::id_tracker::PointMappingsRefEnum::Disk

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::reader::{self, DiskMappingReader};
use crate::common::operation_error::OperationResult;
use crate::types::PointIdType;

/// The state a disk-resident mapping needs to answer reads: the lazy mapping
/// reader and the deleted set. Each disk tracker implements it, gaining the
/// shared point-lookup helpers and a uniform way to build a [`DiskMappingsRef`].
pub trait DiskMappingsSource {
    type Backend: UniversalRead;

    /// The lazy mapping read core (`i2e`/`e2i`).
    fn mapping_reader(&self) -> &DiskMappingReader<Self::Backend>;

    /// Per-point deletion check. The writable tracker reads its resident bitvec
    /// (infallible); the read-only tracker does a single lazy `get_bit` over the
    /// on-disk file (no full-set load), whose storage error propagates here.
    fn point_deleted(&self, offset: PointOffsetType) -> OperationResult<bool>;

    /// The whole deleted set as a slice, used by iteration and vector search. The
    /// read-only tracker materializes it lazily behind this call, so its storage
    /// error propagates here.
    fn deleted_bitslice(&self) -> OperationResult<&BitSlice>;

    /// A borrowed `(reader, deleted)` view for ordered/random iteration. Fails if
    /// the deleted set cannot be materialized.
    fn mappings_ref(&self) -> OperationResult<DiskMappingsRef<'_, Self::Backend>> {
        Ok(DiskMappingsRef::new(
            self.mapping_reader(),
            self.deleted_bitslice()?,
        ))
    }

    /// A `(reader, deleted)` view for the infallible `IdTrackerRead` boundary: on
    /// a deleted-set load error, log and fall back to an empty deleted set (the
    /// same best-effort behaviour as before the error surface was threaded in).
    fn mappings_ref_lossy(&self) -> DiskMappingsRef<'_, Self::Backend> {
        self.mappings_ref().unwrap_or_else(|err| {
            log::error!("disk id tracker deleted set load failed: {err}");
            DiskMappingsRef::new(self.mapping_reader(), BitSlice::empty())
        })
    }

    /// External→internal, excluding deleted points. Storage errors propagate.
    fn resolve_internal(
        &self,
        external_id: PointIdType,
    ) -> OperationResult<Option<PointOffsetType>> {
        let Some(offset) = self.mapping_reader().lookup(external_id)? else {
            return Ok(None);
        };
        // The immutable runs still carry points deleted after build; re-check.
        Ok((!self.point_deleted(offset)?).then_some(offset))
    }

    /// Internal→external, excluding deleted points. Storage errors propagate.
    fn resolve_external(&self, offset: PointOffsetType) -> OperationResult<Option<PointIdType>> {
        if self.point_deleted(offset)? {
            Ok(None)
        } else {
            self.mapping_reader().external_id(offset)
        }
    }
}

/// A borrowed view over a disk-resident mapping and its deleted set, providing
/// the ordered/random iteration used by [`PointMappingsRefEnum`].
///
/// Held by value (it is two references) inside
/// [`PointMappingsRefEnum::Disk`](crate::id_tracker::PointMappingsRefEnum::Disk),
/// so the mapping is reached by static dispatch rather than a trait object.
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

    pub fn iter_external(self) -> Box<dyn Iterator<Item = PointIdType> + 'a> {
        Box::new(
            self.reader
                .iter_from(None)
                .filter(live_filter(self.deleted))
                .map(|(external_id, _)| external_id),
        )
    }

    pub fn iter_internal(self) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        let deleted = self.deleted;
        let total = self.reader.total_point_count() as PointOffsetType;
        Box::new(
            (0..total).filter(move |&offset| !deleted.get_bit(offset as usize).unwrap_or(false)),
        )
    }

    pub fn iter_from(
        self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + 'a> {
        Box::new(
            self.reader
                .iter_from(external_id)
                .filter(live_filter(self.deleted)),
        )
    }

    pub fn iter_random(self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + 'a> {
        reader::iter_random(self.reader, self.deleted)
    }
}

/// Predicate keeping only `(id, offset)` pairs whose offset is not deleted.
fn live_filter(deleted: &BitSlice) -> impl Fn(&(PointIdType, PointOffsetType)) -> bool + '_ {
    move |&(_, offset)| !deleted.get_bit(offset as usize).unwrap_or(false)
}

/// Swallow a disk lookup error at the [`IdTrackerRead`](crate::id_tracker::IdTrackerRead)
/// boundary — whose methods must return `Option` — by logging it and yielding
/// `None`. The fallible surface stops at [`DiskMappingsSource`]; this is the one
/// place the error is dropped.
///
/// ToDo: this should be removed after we have a batch-read interface for id tracker lookups
pub fn log_lookup_err<T>(result: OperationResult<Option<T>>) -> Option<T> {
    result.unwrap_or_else(|err| {
        log::error!("disk id tracker lookup failed: {err}");
        None
    })
}
