//! Opening and preopening (prefetch scheduling) of the read-only tracker.

use std::path::Path;
use std::sync::OnceLock;

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::universal_io::{
    CachedReadFs, OpenOptions, Populate, TypedStorage, UniversalRead, UniversalReadFs,
};

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::disk_id_tracker::reader::DiskMappingReader;
use crate::id_tracker::immutable_id_tracker::{deleted_path, version_mapping_path};
use crate::types::SeqNumberType;

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    pub(super) fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of every file [`try_open`](Self::try_open)
    /// will read. Returns `false` (nothing scheduled) when the tracker is not
    /// in the on-disk format.
    pub fn try_preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<bool> {
        if !DiskMappingReader::try_preopen(fs, segment_path)? {
            return Ok(false);
        }

        let options = Self::open_options();

        fs.schedule_prefetch(&version_mapping_path(segment_path), Some(options), None)?;
        fs.schedule_prefetch(&deleted_path(segment_path), Some(options), None)?;

        Ok(true)
    }

    /// Open a read-only disk id tracker at `segment_path`; all per-point data
    /// except the `is_uuid` bitmap stays on the backing store.
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

    /// Like [`open`](Self::open), but returns `Ok(None)` when the segment is
    /// not in the on-disk format (`i2e` absent).
    pub fn try_open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<Option<Self>> {
        let Some(reader) = DiskMappingReader::try_open(fs, segment_path)? else {
            return Ok(None);
        };

        let options = Self::open_options();

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
}
