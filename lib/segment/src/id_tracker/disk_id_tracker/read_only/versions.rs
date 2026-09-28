//! Per-point versions of the read-only tracker, in either the flat or the
//! [`compact_versions`] format.

use std::path::{Path, PathBuf};

use common::generic_consts::{Random, Sequential};
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OpenOptions, Populate, ReadRange, TypedStorage, UioResult, UniversalRead,
    UniversalReadFs,
};

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::disk_id_tracker::compact_versions::{self, compact_versions_path};
use crate::id_tracker::immutable_id_tracker::version_mapping_path;
use crate::types::SeqNumberType;

pub(super) enum ReadOnlyVersions<S: UniversalRead> {
    /// Flat `id_tracker.versions`, read per point.
    Flat {
        file: TypedStorage<S, SeqNumberType>,
        len: u64,
    },
    /// [`compact_versions`] file, read into RAM whole on open.
    Compact(CompressedVersions),
}

/// The compact versions file is read whole at open.
fn compact_open_options(populate: Populate) -> OpenOptions {
    OpenOptions {
        writeable: false,
        need_sequential: true,
        populate,
        advice: AdviceSetting::Global,
    }
}

impl<S: UniversalRead> ReadOnlyVersions<S> {
    /// Schedule background prefetch of the file [`open`](Self::open) will read.
    pub fn schedule_preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
    ) -> OperationResult<()> {
        let compact_path = compact_versions_path(segment_path);
        if UniversalReadFs::exists(fs, &compact_path)? {
            let options = compact_open_options(Populate::PreferBackground);
            fs.schedule_open(&compact_path, Some(options), None);
        } else {
            let options = ReadOnlyDiskIdTracker::<S>::open_options(populate);
            fs.schedule_open(&version_mapping_path(segment_path), Some(options), None);
        }
        Ok(())
    }

    /// Open the versions file of the segment at `segment_path` in whichever
    /// format it was written. `populate` applies to the flat file only.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        populate: Populate,
    ) -> OperationResult<Self> {
        let compact_options = compact_open_options(Populate::Blocking);
        if let Some(versions) = compact_versions::load(fs, segment_path, compact_options)? {
            return Ok(Self::Compact(versions));
        }
        let file = TypedStorage::<S, SeqNumberType>::new(fs.open(
            version_mapping_path(segment_path),
            ReadOnlyDiskIdTracker::<S>::open_options(populate),
            Default::default(),
        )?);
        let len = file.len()?;
        Ok(Self::Flat { file, len })
    }

    pub fn path(&self, segment_path: &Path) -> PathBuf {
        match self {
            Self::Flat { file: _, len: _ } => version_mapping_path(segment_path),
            Self::Compact(_) => compact_versions_path(segment_path),
        }
    }

    pub fn get(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        let (file, len) = match self {
            Self::Flat { file, len } => (file, *len),
            Self::Compact(versions) => return versions.get(internal_id),
        };
        if u64::from(internal_id) >= len {
            return None;
        }
        match file.read(
            ReadRange::one(u64::from(internal_id) * size_of::<SeqNumberType>() as u64),
            Random,
        ) {
            Ok(values) => values.first().copied(),
            Err(err) => {
                log::error!("disk id tracker version read failed: {err}");
                None
            }
        }
    }

    /// One pipelined pass over the flat versions file instead of a read per
    /// point, streaming each `(internal_id, version)` to `callback` as its read
    /// completes. The input is walked once and nothing is buffered; out-of-range
    /// offsets are skipped and a storage error propagates. Resident compact
    /// versions need no IO.
    pub fn get_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        mut callback: impl FnMut(PointOffsetType, SeqNumberType),
    ) -> OperationResult<()> {
        let (file, len) = match self {
            Self::Flat { file, len } => (file, *len),
            Self::Compact(versions) => {
                for internal_id in internal_ids {
                    if let Some(version) = versions.get(internal_id) {
                        callback(internal_id, version);
                    }
                }
                return Ok(());
            }
        };
        // Each read is tagged with its `internal_id` so the callback can pair it
        // with the version; the range iterator stays lazy (no collect).
        let ranges = internal_ids
            .into_iter()
            .filter(|&internal_id| u64::from(internal_id) < len)
            .map(|internal_id| {
                let range = ReadRange {
                    byte_offset: u64::from(internal_id) * size_of::<SeqNumberType>() as u64,
                    length: 1,
                };
                (internal_id, range)
            });
        file.read_batch(ranges, Random, |internal_id, values| {
            if let Some(&version) = values.first() {
                callback(internal_id, version);
            }
            UioResult::Ok(())
        })?;
        Ok(())
    }

    /// Reads the whole flat versions file at once: this runs only on the
    /// cleanup-on-open path, which drains the iteration anyway.
    pub fn iter(
        &self,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        let (file, len) = match self {
            Self::Flat { file, len } => (file, *len),
            Self::Compact(versions) => return Ok(Box::new(versions.iter())),
        };
        let versions = file.read(ReadRange::new(0, len), Sequential)?.into_owned();
        Ok(Box::new(versions.into_iter().enumerate().map(
            |(offset, version)| (offset as PointOffsetType, version),
        )))
    }
}
