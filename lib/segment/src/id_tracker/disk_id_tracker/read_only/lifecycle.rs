//! Opening and preopening (prefetch scheduling) of the read-only tracker.

use std::path::Path;
use std::sync::OnceLock;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::{CachedReadFs, OpenOptions, Populate, UniversalRead, UniversalReadFs};

use super::ReadOnlyDiskIdTracker;
use super::versions::ReadOnlyVersions;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::disk_id_tracker::reader::DiskMappingReader;
use crate::id_tracker::immutable_id_tracker::deleted_path;

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
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
            // Prefetch because external_ids_batch checks point_deleted() one-by-one
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
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        if !DiskMappingReader::try_preopen(fs, segment_path, populate, hw_counter)? {
            return Ok(false);
        }

        ReadOnlyVersions::schedule_preopen(fs, segment_path, populate, hw_counter)?;
        fs.schedule_open(
            &deleted_path(segment_path),
            Some(Self::deleted_open_options()),
            None,
            hw_counter,
        );

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
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        Self::try_open(fs, segment_path, populate, hw_counter)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "on-disk id tracker not found in segment {}",
                segment_path.display(),
            ))
        })
    }

    /// Like [`open`](Self::open), but returns `Ok(None)` when the segment is
    /// not in the on-disk format (`i2e` absent).
    pub fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Self>> {
        let Some(reader) = DiskMappingReader::try_open(fs, segment_path, populate, hw_counter)?
        else {
            return Ok(None);
        };

        let versions = ReadOnlyVersions::open(fs, segment_path, populate, hw_counter)?;

        let deleted_file = StoredBitSlice::open(
            fs,
            deleted_path(segment_path),
            Self::deleted_open_options(),
            Default::default(),
            hw_counter,
        )?;

        Ok(Some(Self {
            path: segment_path.to_path_buf(),
            reader,
            versions,
            deleted_file,
            deleted_full: OnceLock::new(),
        }))
    }
}
