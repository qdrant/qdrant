//! Disk-resident, read-only id tracker over the [on-disk format], for
//! serverless / object-storage followers.
//!
//! Keeps only a small fixed index resident (via [`DiskMappingReader`]) and
//! answers every lookup with at most one data-block read through the backing
//! [`UniversalRead`] handle, so RAM usage does not scale with point count.
//!
//! Deletion status is checked per point via [`StoredBitSlice::get_bit`] on the hot
//! read-by-id path (no full load). The whole deleted set is materialized lazily,
//! once, only when a path that needs the entire slice runs — vector search
//! ([`deleted_point_bitslice`](IdTrackerRead::deleted_point_bitslice)), scroll
//! iteration, counts, or the [`live_reload`](Self::live_reload) diff baseline.
//!
//! [on-disk format]: super::on_disk_format

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use common::bitvec::{BitSlice, BitVec};
use common::generic_consts::{Random, Sequential};
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{
    OpenOptions, Populate, ReadRange, TypedStorage, UniversalRead, UniversalReadFs,
};

use super::mappings::{DiskMappingsSource, log_lookup_err};
use super::on_disk_format::{e2i_path, i2e_path};
use super::reader::DiskMappingReader;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::immutable_id_tracker::{deleted_path, version_mapping_path};
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

/// Read-only id tracker backed by the on-disk format files, streamed lazily
/// through a [`UniversalRead`] backend.
pub struct ReadOnlyDiskIdTracker<S: UniversalRead> {
    path: PathBuf,

    /// Lazy mapping read core (resident: headers + sparse index).
    reader: DiskMappingReader<S>,

    versions: TypedStorage<S, SeqNumberType>,
    versions_len: u64,
    /// Kept for per-point `get_bit` and for `live_reload` reopen.
    deleted_file: StoredBitSlice<S>,

    /// Full deleted set. NOT loaded on open or by point lookups. Materialized on
    /// the first search/scroll/count/reload and reused; invalidated by `live_reload`.
    deleted_full: OnceLock<BitVec>,
}

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    /// Open a read-only disk id tracker at `segment_path`. Reads only the two
    /// headers and the e2i sparse block index into RAM; all per-point data stays
    /// on the backing store.
    ///
    /// Errors if the segment is not in the on-disk format; use
    /// [`try_open`](Self::try_open) to probe without erroring.
    pub fn open(fs: &impl UniversalReadFs<File = S>, segment_path: &Path) -> OperationResult<Self> {
        Self::try_open(fs, segment_path)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "on-disk id tracker not found in segment {}",
                segment_path.display(),
            ))
        })
    }

    /// Like [`open`](Self::open), but returns `Ok(None)` when the segment is not
    /// in the on-disk format (`i2e` absent). Probing happens by opening the
    /// mapping directly, so no separate existence check is issued.
    pub fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<Option<Self>> {
        let Some(reader) = DiskMappingReader::try_open(fs, segment_path)? else {
            return Ok(None);
        };

        let options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        };
        let versions = TypedStorage::<S, SeqNumberType>::new(fs.open(
            version_mapping_path(segment_path),
            options,
            Default::default(),
        )?);
        let versions_len = versions.len()?;

        let deleted_file =
            StoredBitSlice::open(fs, deleted_path(segment_path), options, Default::default())?;

        Ok(Some(Self {
            path: segment_path.to_path_buf(),
            reader,
            versions,
            versions_len,
            deleted_file,
            deleted_full: OnceLock::new(),
        }))
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            i2e_path(&self.path),
            e2i_path(&self.path),
            version_mapping_path(&self.path),
            deleted_path(&self.path),
        ]
    }

    /// Re-read the on-disk deleted bitslice and report points deleted since the
    /// last reload. Mappings are immutable, so nothing is ever inserted.
    ///
    /// The full deleted set (`deleted_full`) doubles as the diff baseline: if it
    /// was materialized (by a prior search/scroll/count/reload) we diff against
    /// it; otherwise this is the first baseline and every currently-deleted
    /// offset is reported (an idempotent replay downstream).
    pub fn live_reload(&mut self) -> OperationResult<LiveReloadResult> {
        self.deleted_file.reopen()?;
        let new: BitVec = self.deleted_file.read_all()?.into_owned();

        let baseline = self.deleted_full.take();
        let deleted: Vec<PointOffsetType> = match baseline {
            Some(old) => new
                .iter_ones()
                .filter(|&i| !old.get(i).is_some_and(|b| *b))
                .map(|i| i as PointOffsetType)
                .collect(),
            None => new.iter_ones().map(|i| i as PointOffsetType).collect(),
        };
        debug_assert!(deleted.is_sorted());

        // `take` above emptied the cell, so this refreshes it to the new state
        // and serves both the next search view and the next reload baseline.
        let _ = self.deleted_full.set(new);

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        })
    }

    /// Lazily materialize the full deleted set. Used only by paths that need the
    /// whole slice (search / scroll / counts); never by point lookups. The read
    /// error propagates instead of being swallowed here.
    ///
    /// Manual fallible init (std `OnceLock` has no stable `get_or_try_init`): on a
    /// race both threads read the same on-disk state (`live_reload` needs `&mut`,
    /// so it can't interleave), so the loser's `set` failing is harmless.
    fn deleted_full(&self) -> OperationResult<&BitVec> {
        if let Some(materialized) = self.deleted_full.get() {
            return Ok(materialized);
        }
        let materialized = self.deleted_file.read_all()?.into_owned();
        let _ = self.deleted_full.set(materialized);
        Ok(self.deleted_full.get().expect("just set"))
    }

    /// Whether the full deleted set has been materialized. Test-only: used to
    /// assert that read-by-id lookups do not trigger a full load.
    #[cfg(test)]
    pub(crate) fn deleted_full_materialized(&self) -> bool {
        self.deleted_full.get().is_some()
    }
}

impl<S: UniversalRead> DiskMappingsSource for ReadOnlyDiskIdTracker<S> {
    type Backend = S;

    fn mapping_reader(&self) -> &DiskMappingReader<S> {
        &self.reader
    }

    /// A single lazy `get_bit` on the on-disk deleted file — no full-set load, so
    /// read-by-id stays lazy. Out-of-range offsets are treated as deleted; storage
    /// errors propagate.
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

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        let ranges = ReadRange {
            byte_offset: 0,
            length: self.versions_len,
        }
        .iter_autochunks::<SeqNumberType>()
        .map(|range| ((), range));

        match self.versions.read_iter::<Sequential, ()>(ranges) {
            Ok(iter) => {
                let mut offset: PointOffsetType = 0;
                Box::new(iter.flat_map(move |result| {
                    let chunk = match result {
                        Ok((_, cow)) => cow.into_owned(),
                        Err(err) => {
                            log::error!("disk id tracker versions stream failed: {err}");
                            Vec::new()
                        }
                    };
                    let start = offset;
                    offset += chunk.len() as PointOffsetType;
                    chunk
                        .into_iter()
                        .enumerate()
                        .map(move |(i, version)| (start + i as PointOffsetType, version))
                }))
            }
            Err(err) => {
                log::error!("disk id tracker versions read_iter failed: {err}");
                Box::new(std::iter::empty())
            }
        }
    }
}
