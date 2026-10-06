use std::path::Path;

use common::storage_version::{StorageVersion as _, VERSION_FILE};
use common::universal_io::{CachedFs, CachedReadFs, UniversalReadFsAsync, read_json_via};

use super::TrackerLookup;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::segment::update_only::lookup::{WRITER_POPULATE, build_cached_fs};
use crate::segment::{SEGMENT_STATE_FILE, SegmentVersion};
use crate::types::{SegmentConfig, SegmentState};

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
