//! Mapping reads written once over the `(mapping reader, deleted set)` pair:
//! point lookups ([`DiskMappingsSource`]) and ordered/random iteration
//! ([`DiskMappingsRef`]). Deleted points are never returned from either.

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::reader::{self, DiskMappingReader};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::PointIdBatch;
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
    fn point_deleted(&self, offset: PointOffsetType) -> OperationResult<bool>;

    /// The whole deleted set as a slice; may materialize it lazily, so its
    /// storage error propagates here.
    fn deleted_bitslice(&self) -> OperationResult<&BitSlice>;

    /// A borrowed `(reader, deleted)` view for ordered/random iteration. Fails if
    /// the deleted set cannot be materialized.
    fn mappings_ref(&self) -> OperationResult<DiskMappingsRef<'_, Self::Backend>> {
        Ok(DiskMappingsRef::new(
            self.mapping_reader(),
            self.deleted_bitslice()?,
        ))
    }

    /// Like [`mappings_ref`](Self::mappings_ref), but infallible: on a
    /// deleted-set load error, logs and falls back to an empty deleted set.
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
        external_ids: impl PointIdBatch,
        mut on_live: impl FnMut(PointIdType, PointOffsetType),
    ) -> OperationResult<()> {
        // Use `first_err` instead of `?` to avoid UniversalIoError vs OperationResult problems
        let mut first_err = None;
        self.mapping_reader()
            .lookup_batch(external_ids, |id, offset| {
                match self.point_deleted(offset) {
                    Ok(false) => on_live(id, offset),
                    Ok(true) => {}
                    Err(err) => {
                        first_err.get_or_insert(err);
                    }
                }
            })?;
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Batch counterpart of [`resolve_external`](Self::resolve_external): a
    /// per-point deleted check (see [`resolve_internal_batch`](Self::resolve_internal_batch)
    /// on why it is not batched), then one pipelined mapping-read pass over the
    /// surviving offsets. Results are in input order.
    fn resolve_external_batch(
        &self,
        offsets: &[PointOffsetType],
    ) -> OperationResult<Vec<Option<PointIdType>>> {
        let deleted: Vec<bool> = offsets
            .iter()
            .map(|&offset| self.point_deleted(offset))
            .collect::<OperationResult<_>>()?;

        let live: Vec<PointOffsetType> = offsets
            .iter()
            .zip(&deleted)
            .filter(|&(_, &deleted)| !deleted)
            .map(|(&offset, _)| offset)
            .collect();
        let external_ids = self.mapping_reader().external_ids_batch(&live)?;

        let mut external_ids = external_ids.into_iter();
        Ok(deleted
            .into_iter()
            .map(|deleted| {
                if deleted {
                    None
                } else {
                    external_ids
                        .next()
                        .expect("one external id per live offset")
                }
            })
            .collect())
    }
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

    pub fn iter_external(self) -> impl Iterator<Item = PointIdType> + 'a {
        self.reader
            .iter_from(None)
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
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        self.reader
            .iter_from(external_id)
            .filter(live_filter(self.deleted))
    }

    pub fn iter_random(self) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
        reader::iter_random(self.reader, self.deleted)
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

/// Batch analogue of [`log_lookup_err`]: on a storage error, log it and yield
/// all-`None` (`len` entries), since the `IdTrackerRead` batch methods must
/// return one slot per input.
pub fn log_lookup_err_batch<T>(
    result: OperationResult<Vec<Option<T>>>,
    len: usize,
) -> Vec<Option<T>> {
    result.unwrap_or_else(|err| {
        log::error!("disk id tracker batch lookup failed: {err}");
        std::iter::repeat_with(|| None).take(len).collect()
    })
}
