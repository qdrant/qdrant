//! Persistence of the writable tracker's resident versions, in either the flat
//! or the [`compact_versions`] format.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::flags::feature_flags;
use common::mmap::{AdviceSetting, create_and_ensure_length};
use common::types::PointOffsetType;
use common::universal_io::{
    OpenOptions, Populate, SliceBufferedUpdateWrapper, TypedStorage, UniversalWrite,
};

use super::compact_versions::{self, compact_versions_path};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::immutable_id_tracker::version_mapping_path;
use crate::types::SeqNumberType;

/// Format of a newly created versions file. Existing files are opened in
/// whichever format they were written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionsFormat {
    Flat,
    Compact,
}

impl VersionsFormat {
    /// [`Compact`](Self::Compact) in serverless-compatible deployments.
    pub fn from_feature_flags() -> Self {
        if feature_flags().serverless_compatible() {
            Self::Compact
        } else {
            Self::Flat
        }
    }
}

#[derive(Debug)]
pub(super) enum VersionsFile<S: UniversalWrite> {
    /// Flat `id_tracker.versions`, one `u64` per point, updated in place.
    Flat(SliceBufferedUpdateWrapper<S, SeqNumberType>),
    /// [`compact_versions`] file, replaced whole on flush once `dirty`.
    ///
    /// `dirty` is shared with the flushers, so that a failed flush can mark it again.
    Compact { fs: S::Fs, dirty: Arc<AtomicBool> },
}

impl<S: UniversalWrite + Send + Sync + 'static> VersionsFile<S> {
    /// Open the versions file of the segment at `segment_path` in whichever
    /// format it was written, and read all versions into RAM.
    pub fn open(fs: &S::Fs, segment_path: &Path) -> OperationResult<(CompressedVersions, Self)> {
        let options = OpenOptions {
            writeable: true,
            need_sequential: false,
            populate: Populate::Blocking,
            advice: AdviceSetting::Global,
        };
        if let Some(versions) = compact_versions::load(fs, segment_path, options)? {
            return Ok((versions, Self::compact(fs)));
        }
        let file = TypedStorage::<S, SeqNumberType>::open(
            fs,
            version_mapping_path(segment_path),
            options,
            Default::default(),
        )?;
        let versions = CompressedVersions::from_slice(&file.read_whole()?);
        Ok((
            versions,
            Self::Flat(SliceBufferedUpdateWrapper::new(file.inner)?),
        ))
    }

    /// Create the versions file of the segment at `segment_path` holding
    /// `versions`, zero-padded to `count`.
    pub fn create(
        fs: &S::Fs,
        segment_path: &Path,
        versions: &[SeqNumberType],
        count: usize,
        format: VersionsFormat,
    ) -> OperationResult<(CompressedVersions, Self)> {
        match format {
            VersionsFormat::Compact => {
                let mut padded = versions.to_vec();
                padded.resize(count, 0);
                let versions = CompressedVersions::from_slice(&padded);
                compact_versions::save(fs, segment_path, &versions)?;
                Ok((versions, Self::compact(fs)))
            }
            VersionsFormat::Flat => {
                let path = version_mapping_path(segment_path);
                create_and_ensure_length(&path, count * size_of::<SeqNumberType>())?;
                let mut file = TypedStorage::<S, SeqNumberType>::open(
                    fs,
                    &path,
                    OpenOptions {
                        writeable: true,
                        need_sequential: false,
                        populate: Populate::No,
                        advice: AdviceSetting::Global,
                    },
                    Default::default(),
                )?;
                file.write(0, versions)?;
                let versions = CompressedVersions::from_slice(&file.read_whole()?);
                let wrapper = SliceBufferedUpdateWrapper::new(file.inner)?;
                wrapper.flusher()()?;
                Ok((versions, Self::Flat(wrapper)))
            }
        }
    }

    fn compact(fs: &S::Fs) -> Self {
        Self::Compact {
            fs: fs.clone(),
            dirty: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn path(&self, segment_path: &Path) -> PathBuf {
        match self {
            Self::Flat(_) => version_mapping_path(segment_path),
            Self::Compact { fs: _, dirty: _ } => compact_versions_path(segment_path),
        }
    }

    /// Record a version change of `internal_id`, already applied to the
    /// resident versions.
    pub fn set(&mut self, internal_id: PointOffsetType, version: SeqNumberType, is_deleted: bool) {
        match self {
            Self::Flat(wrapper) => wrapper.set(internal_id, version),
            // Lookups skip deleted points, so their versions need not be persisted
            Self::Compact { fs: _, dirty } => {
                if !is_deleted {
                    dirty.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    /// Flush pending changes; `versions` are the resident versions of the
    /// segment at `segment_path`.
    pub fn flusher(&self, segment_path: &Path, versions: &CompressedVersions) -> Flusher {
        match self {
            Self::Flat(wrapper) => {
                let flusher = wrapper.flusher();
                Box::new(move || flusher().map_err(OperationError::from))
            }
            Self::Compact { fs, dirty } => {
                if !dirty.swap(false, Ordering::Relaxed) {
                    return Box::new(|| Ok(()));
                }
                let fs = fs.clone();
                let dirty = Arc::clone(dirty);
                let segment_path = segment_path.to_path_buf();
                let versions = versions.clone();
                Box::new(move || {
                    compact_versions::save(&fs, &segment_path, &versions)
                        .inspect_err(|_| dirty.store(true, Ordering::Relaxed))
                })
            }
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Flat(wrapper) => wrapper.clear_cache()?,
            // Read into RAM whole
            Self::Compact { fs: _, dirty: _ } => {}
        }
        Ok(())
    }
}
