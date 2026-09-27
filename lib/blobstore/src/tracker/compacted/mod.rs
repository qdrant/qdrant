//! [`CompactedTracker`]: tracker for the append-only mode that keeps all mappings in RAM and
//! persists them as one compact file, rewritten whole on every flush.
//!
//! Intended for storages that are written once and then only read, such as the segments an
//! optimizer builds. A reader decodes the file once on open and serves every lookup from RAM,
//! instead of reading one 16 byte entry per lookup from the flat file of
//! [`AppendOnlyTracker`](super::append_only::AppendOnlyTracker), which is expensive on storage
//! where every read is a round trip.
//!
//! The file format is described in [`format`].

mod format;
mod read;
#[cfg(test)]
mod tests;

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::generic_consts::Sequential;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    CachedReadFs, IsNotFound, OpenOptions, Populate, UioResult, UniversalRead, UniversalReadFs,
    UniversalWriteFs,
};

use crate::Result;
use crate::blobstore::Flusher;
use crate::error::BlobstoreError;
use crate::tracker::{PointOffset, TrackerRead, ValuePointer};

/// File name of the compacted tracker file
///
/// Deliberately different from the other tracker file names, so that one tracker never attempts
/// to load the incompatible file format of another.
const FILE_NAME: &str = "compacted_tracker.dat";

/// The file operation a writable tracker needs from the filesystem it was given.
///
/// Object safe, so that the tracker type does not depend on the filesystem type: writers
/// receive their filesystem as a generic parameter, not as the file type's `S::Fs`.
trait TrackerFs: fmt::Debug + Send + Sync {
    /// Atomically replace the whole file at `path`
    fn save(&self, path: &Path, bytes: &[u8]) -> UioResult<()>;
}

impl<Fs: UniversalWriteFs> TrackerFs for Fs {
    fn save(&self, path: &Path, bytes: &[u8]) -> UioResult<()> {
        self.atomic_save(path, bytes)
    }
}

/// Tracker of value pointers for the append-only storage mode, held entirely in RAM.
///
/// Mappings can be set in any order and replaced. Skipped point offsets read as `None`.
///
/// Flushing rewrites the whole file from a copy of the mappings, through the filesystem the
/// tracker was created or opened writable with, see [`Self::flusher`]. Opening decodes the whole
/// file once; reads never touch the disk afterwards.
#[derive(Debug)]
pub struct CompactedTracker {
    /// Path to the tracker file
    path: PathBuf,
    /// Entry `i` is the mapping for point offset `i`
    pointers: Vec<Option<ValuePointer>>,
    /// Whether mappings were set since the last flusher took its copy.
    ///
    /// Shared with the flushers, so that a failed flush can mark the tracker dirty again.
    dirty: Arc<AtomicBool>,
    /// Filesystem the file is rewritten through on flush. `None` if opened read-only.
    fs: Option<Arc<dyn TrackerFs>>,
}

impl CompactedTracker {
    fn tracker_file_name(dir: &Path) -> PathBuf {
        dir.join(FILE_NAME)
    }

    /// The file is always read whole right after opening, so it is populated regardless of how
    /// the storage populates its other files. This makes a [`preopen`](Self::preopen) fetch the
    /// whole file.
    fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: true,
            populate: Populate::Blocking,
            advice: AdviceSetting::Advice(Advice::Sequential),
        }
    }

    /// Whether the given directory holds a compacted tracker file.
    pub fn exists<Fs: UniversalReadFs>(fs: &Fs, dir: &Path) -> Result<bool> {
        Ok(fs.exists(&Self::tracker_file_name(dir))?)
    }

    /// Schedule a prefetch for the file a subsequent [`open`](Self::open) reads.
    pub fn preopen<Fs: CachedReadFs>(fs: &Fs, dir: &Path) {
        fs.schedule_open(
            &Self::tracker_file_name(dir),
            Some(Self::open_options()),
            None,
        );
    }

    /// Open an existing tracker in the given directory read-only, decoding the whole file into
    /// RAM.
    ///
    /// If the file does not exist or does not decode, return an error.
    pub fn open<Fs: UniversalReadFs>(fs: &Fs, dir: &Path) -> Result<Self> {
        let path = Self::tracker_file_name(dir);
        let file = fs
            .open(&path, Self::open_options(), Default::default())
            .map_err(|err| {
                if err.is_not_found() {
                    // If config exists and this file doesn't, it should be treated as
                    // inconsistent storage rather than a missing one
                    BlobstoreError::service_error(format!(
                        "Compacted tracker file does not exist: {}",
                        path.display(),
                    ))
                } else {
                    BlobstoreError::from(err)
                }
            })?;

        let pointers = format::decode(&file.read_whole::<u8>()?).map_err(|err| {
            BlobstoreError::service_error(format!(
                "Invalid compacted tracker file {}: {err}",
                path.display(),
            ))
        })?;

        Ok(Self {
            path,
            pointers,
            dirty: Arc::new(AtomicBool::new(false)),
            fs: None,
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Number of mappings: one past the highest point offset that was ever set.
    pub fn pointer_count(&self) -> PointOffset {
        self.pointers.len() as PointOffset
    }

    /// Heap RAM held by the mappings. The file is only read on open, not kept.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            path: _,
            pointers,
            dirty: _,
            fs: _,
        } = self;
        pointers.capacity() * size_of::<Option<ValuePointer>>()
    }

    /// Set the mapping for the given point offset, replacing any previous one.
    ///
    /// Point offsets can be set in any order. Skipped offsets read as `None`.
    pub fn set(&mut self, point_offset: PointOffset, pointer: ValuePointer) {
        let index = point_offset as usize;
        if index >= self.pointers.len() {
            self.pointers.resize(index + 1, None);
        }
        self.pointers[index] = Some(pointer);
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Whether mappings were set since the last flusher took its copy.
    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }
}

impl CompactedTracker {
    /// Create a new empty tracker in the given directory, replacing the file if it already
    /// exists.
    ///
    /// The directory must exist already.
    pub fn new<Fs: UniversalWriteFs>(fs: &Fs, dir: &Path) -> Result<Self> {
        let path = Self::tracker_file_name(dir);
        fs.atomic_save(&path, &format::encode(&[])?)?;
        Ok(Self {
            path,
            pointers: Vec::new(),
            dirty: Arc::new(AtomicBool::new(false)),
            fs: Some(Arc::new(fs.clone())),
        })
    }

    /// Convert `source`, the tracker of the storage in the given directory, holding all of its
    /// mappings. Nothing is written, the first flush saves the file.
    pub fn from_tracker<Fs: UniversalWriteFs>(
        fs: &Fs,
        dir: &Path,
        source: &impl TrackerRead,
    ) -> Result<Self> {
        Ok(Self {
            path: Self::tracker_file_name(dir),
            pointers: source.get_range::<Sequential>(0..source.max_point_offset()?)?,
            dirty: Arc::new(AtomicBool::new(true)),
            fs: Some(Arc::new(fs.clone())),
        })
    }

    /// Open an existing tracker in the given directory for writing, decoding the whole file into
    /// RAM. Flushes rewrite the file through `fs`.
    ///
    /// If the file does not exist or does not decode, return an error.
    pub fn open_writable<Fs: UniversalReadFs + UniversalWriteFs>(
        fs: &Fs,
        dir: &Path,
    ) -> Result<Self> {
        let mut tracker = Self::open(fs, dir)?;
        tracker.fs = Some(Arc::new(fs.clone()));
        Ok(tracker)
    }

    /// Create a flusher that rewrites the whole file from a copy of the mappings below
    /// `target`, a point offset count.
    ///
    /// A clean tracker gets a flusher that does nothing. Taking the copy marks the tracker
    /// clean, unless mappings at or past `target` are left out of it, so mappings set while a
    /// flush is in progress are left for the next flush. A failed flush marks it dirty again, so
    /// the next flush retries. The file is replaced atomically.
    ///
    /// Rewriting is linear in the number of mappings, which suits a storage that is flushed
    /// once after being built, not one that is flushed after every batch.
    pub fn flusher(&self, target: PointOffset) -> Flusher {
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return Box::new(|| Ok(()));
        }

        let end = (target as usize).min(self.pointers.len());
        if end < self.pointers.len() {
            self.dirty.store(true, Ordering::Relaxed);
        }

        let fs = self.fs.clone();
        let path = self.path.clone();
        let pointers = self.pointers[..end].to_vec();
        let dirty = Arc::clone(&self.dirty);

        Box::new(move || {
            let Some(fs) = &fs else {
                dirty.store(true, Ordering::Relaxed);
                return Err(BlobstoreError::service_error(format!(
                    "compacted tracker {} was opened read-only and cannot be flushed",
                    path.display(),
                )));
            };
            let result = format::encode(&pointers)
                .map_err(BlobstoreError::from)
                .and_then(|bytes| Ok(fs.save(&path, &bytes)?));
            if result.is_err() {
                dirty.store(true, Ordering::Relaxed);
            }
            result?;
            Ok(())
        })
    }
}
