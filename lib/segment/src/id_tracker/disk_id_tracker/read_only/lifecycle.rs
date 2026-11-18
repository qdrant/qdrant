//! Opening and preopening (prefetch scheduling) of the read-only tracker.

use std::path::{Path, PathBuf};

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, OpenOptions, Populate, UniversalRead, UniversalReadFs};

use super::ReadOnlyDiskIdTracker;
use super::versions::ReadOnlyVersions;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::delete_versions::{DeleteGating, GatedDeleteLog};
use crate::id_tracker::disk_id_tracker::reader::DiskMappingReader;
use crate::id_tracker::immutable_id_tracker::deleted_path;

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    /// No commit mark: the format is fully committed once present.
    pub fn commit_mark_path(_segment_path: &Path) -> Option<PathBuf> {
        None
    }

    /// No commit mark, so no bound: see [`Self::commit_mark_path`].
    pub fn max_committed_offset(
        _fs: &impl CachedReadFs,
        _segment_path: &Path,
    ) -> Option<PointOffsetType> {
        None
    }

    /// An `Auto` populate preloads, as search reads versions per result.
    pub(super) fn open_options(populate: Populate) -> OpenOptions {
        let populate = match populate {
            Populate::Auto => Populate::PreferBackground,
            Populate::No
            | Populate::Blocking
            | Populate::PreferBackground
            | Populate::Partial(_) => populate,
        };
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate,
            advice: AdviceSetting::Global,
        }
    }

    pub(super) fn deleted_open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            // Prefetch, as the open reads it whole
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of every file [`try_open`](Self::try_open)
    /// will read. Returns `false` (nothing scheduled) when the tracker is not
    /// in the on-disk format. `populate` is the placement of the per-point
    /// data, see [`open`](Self::open).
    pub fn try_preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        if !DiskMappingReader::try_preopen(fs, segment_path, populate)? {
            return Ok(false);
        }

        ReadOnlyVersions::schedule_preopen(fs, segment_path, populate)?;
        fs.schedule_open(
            &deleted_path(segment_path),
            Some(Self::deleted_open_options()),
            None,
        );
        GatedDeleteLog::preopen(fs, segment_path);

        Ok(true)
    }

    /// Open a read-only disk id tracker at `segment_path`; all per-point data
    /// except the `is_uuid` bitmap stays on the backing store. A populating
    /// `populate` primes the page cache with the mapping and versions.
    ///
    /// Errors if the segment is not in the on-disk format; use
    /// [`try_open`](Self::try_open) to probe without erroring.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
    ) -> OperationResult<Self> {
        Self::try_open(fs, segment_path, populate, DeleteGating::Immediate)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "on-disk id tracker not found in segment {}",
                segment_path.display(),
            ))
        })
    }

    /// Like [`open`](Self::open), but returns `Ok(None)` when the segment is
    /// not in the on-disk format (`i2e` absent). The deletes are the union of
    /// the mask and the delete log. With [`DeleteGating::Held`], the log's
    /// deletes at or below the gate are applied, newer ones held.
    pub fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
        gating: DeleteGating,
    ) -> OperationResult<Option<Self>> {
        let Some(reader) = DiskMappingReader::try_open(fs, segment_path, populate)? else {
            return Ok(None);
        };

        let versions = ReadOnlyVersions::open(fs, segment_path, populate)?;

        let deleted_file = StoredBitSlice::<S>::open(
            fs,
            deleted_path(segment_path),
            Self::deleted_open_options(),
            Default::default(),
        )?;

        let mut delete_log = GatedDeleteLog::new(segment_path, gating);
        let logged = delete_log.read_new(fs)?;
        let deleted = deleted_file.read_all()?.into_owned();

        let mut tracker = Self {
            path: segment_path.to_path_buf(),
            reader,
            versions,
            deleted,
            delete_log,
        };
        tracker.delete_slots(logged);
        Ok(Some(tracker))
    }
}
