use std::path::Path;

use common::storage_version::{StorageVersion as _, VERSION_FILE};
use common::universal_io::{CachedFs, CachedReadFs, FileInfo, UniversalReadFsAsync, read_json_via};

use super::TrackerLookup;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::delete_versions::DeleteGating;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::segment::update_only::lookup::WRITER_POPULATE;
use crate::segment::{SEGMENT_STATE_FILE, SegmentVersion};
use crate::types::{SegmentConfig, SegmentState};

/// Build a per-segment [`CachedFs`] probing only statically known id-tracker files
/// via HEADs instead of issuing a directory LIST.
fn build_cached_fs<Fs: UniversalReadFsAsync>(
    fs: Fs,
    segment_path: &Path,
) -> OperationResult<CachedFs<Fs>> {
    futures::executor::block_on(build_cached_fs_async(fs, segment_path))
}

async fn build_cached_fs_async<Fs: UniversalReadFsAsync>(
    fs: Fs,
    segment_path: &Path,
) -> OperationResult<CachedFs<Fs>> {
    let mut cached_fs = CachedFs::new(fs, segment_path)?;

    // Probe id-tracker commit marks first, so we have an accurate commit mark available.
    let commit_marks = ReadOnlyIdTrackerEnum::<Fs::File>::commit_mark_paths(segment_path);
    let probed_files = cached_fs.inner().select_files_async(&commit_marks).await?;

    // Schedule static metadata files to overlap with the HEAD wave. Absence is tolerated here:
    // the subsequent read reports it gracefully.
    for file_name in [VERSION_FILE, SEGMENT_STATE_FILE] {
        cached_fs.schedule_open(&segment_path.join(file_name), None, None);
    }

    // Selectively probe the remaining statically known tracker files (skipping commit marks
    // already probed above).
    let tracker_paths: Vec<_> = ReadOnlyIdTrackerEnum::<Fs::File>::static_paths(segment_path)
        .into_iter()
        .filter(|path| !commit_marks.contains(path))
        .collect();
    cached_fs
        .select_cache_file_info_async(&tracker_paths)
        .await?;

    // Inject id-tracker commit-mark probe into cached_fs.
    for path in commit_marks {
        let info = probed_files
            .iter()
            .find(|f| f.path == path)
            .map(FileInfo::from);
        cached_fs.set_file_info(path, info);
    }

    Ok(cached_fs)
}

impl<Fs: UniversalReadFsAsync> TrackerLookup<Fs> {
    /// Open the segment's id tracker, prefetching its files first
    /// ([`preopen`](Self::preopen)).
    pub fn open(fs: Fs, segment_path: &Path) -> OperationResult<Self> {
        let cached_fs = build_cached_fs(fs, segment_path)?;
        let config = Self::preopen(&cached_fs, segment_path)?;
        Self::open_via(cached_fs, segment_path, config)
    }

    /// Open the id tracker over `fs`, once the files [`preopen`](Self::preopen)
    /// scheduled have arrived. `config` is the one `preopen` returned.
    pub fn open_via(
        mut fs: CachedFs<Fs>,
        segment_path: &Path,
        config: SegmentConfig,
    ) -> OperationResult<Self> {
        futures::executor::block_on(fs.wait_all());

        if SegmentVersion::load_universal(&fs, segment_path)?.is_none() {
            // The version file is written last: the segment is incomplete or gone.
            return Err(OperationError::FileNotFound {
                path: segment_path.join(VERSION_FILE),
            });
        }

        let max_committed_offset =
            ReadOnlyIdTrackerEnum::<Fs::File>::max_committed_offset(&fs, segment_path);
        let id_tracker = ReadOnlyIdTrackerEnum::detect_and_load(
            &fs,
            segment_path,
            None,
            max_committed_offset,
            WRITER_POPULATE,
            // A writer resolves against every delete, versioned or not
            DeleteGating::Immediate,
        )?;

        fs.rotate_cache_file_info();

        Ok(Self {
            fs,
            segment_path: segment_path.to_path_buf(),
            id_tracker,
            appendable: config.is_appendable(),
            segment_config: config,
        })
    }

    /// Schedule the prefetch of the id tracker's files. Returns the segment
    /// config, read along the way.
    pub fn preopen(fs: &CachedFs<Fs>, segment_path: &Path) -> OperationResult<SegmentConfig> {
        let SegmentState {
            initial_version: _,
            version: _,
            config,
        } = read_json_via(fs, segment_path.join(SEGMENT_STATE_FILE))?;

        ReadOnlyIdTrackerEnum::preopen(fs, segment_path, WRITER_POPULATE)?;

        Ok(config)
    }
}
