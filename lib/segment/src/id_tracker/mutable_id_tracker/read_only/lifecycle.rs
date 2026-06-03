use std::io::Cursor;
use std::path::{Path, PathBuf};

use common::generic_consts::Sequential;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{OkNotFound, OpenOptions, Populate, ReadRange, UniversalRead};

use super::ReadOnlyAppendableIdTracker;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::mutable_id_tracker::mappings_storage::{mappings_path, read_mappings};
use crate::id_tracker::mutable_id_tracker::versions_storage::{
    VERSION_ELEMENT_SIZE, versions_path,
};
use crate::types::SeqNumberType;

impl<S: UniversalRead> ReadOnlyAppendableIdTracker<S> {
    /// Open a read-only view over the appendable ID tracker data at `segment_path`, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Both the mappings and versions files must already exist, this opens an existing appendable
    /// storage and never creates one. A missing file is an error rather than an empty tracker.
    ///
    /// Unlike [`MutableIdTracker::open`](crate::id_tracker::mutable_id_tracker::MutableIdTracker::open)
    /// this never writes to the storage: a partial trailing mapping entry is simply not consumed
    /// (rather than truncated), so it can be picked up later via [`Self::live_reload`] once the
    /// writer finished flushing it.
    pub fn open(
        fs: &S::Fs,
        segment_path: impl Into<PathBuf>,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        let segment_path = segment_path.into();

        let mappings_path = mappings_path(&segment_path);
        let versions_path = versions_path(&segment_path);

        let mappings_file = open_read_only::<S>(fs, &mappings_path)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "Cannot open read-only appendable ID tracker, mappings file is missing: {}",
                mappings_path.display(),
            ))
        })?;
        let versions_file = open_read_only::<S>(fs, &versions_path)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "Cannot open read-only appendable ID tracker, versions file is missing: {}",
                versions_path.display(),
            ))
        })?;

        let (mappings, mappings_read_to) = {
            let bytes = mappings_file.read_whole::<u8>()?;
            let mut reader = Cursor::new(bytes.as_ref());
            let mappings = read_mappings(&mut reader, deferred_internal_id).map_err(|err| {
                OperationError::service_error(format!("Failed to load ID tracker mappings: {err}"))
            })?;
            (mappings, reader.position())
        };

        // Floor the raw byte length to whole elements so a partial trailing entry (from a torn
        // flush) is ignored, only fully-written versions are loaded. Reading the byte length avoids
        // `len::<SeqNumberType>()`, which some backends debug-assert is a whole number of elements.
        let versions_count = versions_file.len::<u8>()? / VERSION_ELEMENT_SIZE;
        let internal_to_version = versions_file
            .read::<Sequential, SeqNumberType>(ReadRange {
                byte_offset: 0,
                length: versions_count,
            })?
            .into_owned();

        // Compare internal point mappings and versions count, report warning if we don't
        debug_assert!(
            mappings.total_point_count() >= internal_to_version.len(),
            "can never have more versions than internal point mappings",
        );
        if mappings.total_point_count() != internal_to_version.len() {
            log::warn!(
                "Read-only appendable ID tracker mappings and versions count mismatch, could have been partially flushed, assuming automatic recovery by WAL ({} mappings, {} versions)",
                mappings.total_point_count(),
                internal_to_version.len(),
            );
        }

        #[cfg(debug_assertions)]
        mappings.assert_mappings();

        Ok(Self {
            segment_path,
            internal_to_version,
            mappings,
            mappings_read_to,
            mappings_file,
            versions_file,
        })
    }
}

/// Open a file for reading, returning [`None`] if it does not exist yet.
pub(super) fn open_read_only<S: UniversalRead>(
    fs: &S::Fs,
    path: &Path,
) -> OperationResult<Option<S>> {
    use common::universal_io::UniversalReadFs;

    let options = OpenOptions {
        writeable: false,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Global,
    };
    Ok(fs.open(path, options, Default::default()).ok_not_found()?)
}
