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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    IsNotFound, OpenOptions, Populate, UniversalRead, UniversalReadFs, UniversalWriteFs,
};

use crate::Result;
use crate::blobstore::Flusher;
use crate::error::BlobstoreError;
use crate::tracker::{PointOffset, ValuePointer};

/// File name of the compacted tracker file
///
/// Deliberately different from the other tracker file names, so that one tracker never attempts
/// to load the incompatible file format of another.
const FILE_NAME: &str = "compacted_tracker.dat";

/// Tracker of value pointers for the append-only storage mode, held entirely in RAM.
///
/// Mappings can be set in any order and replaced. Skipped point offsets read as `None`.
///
/// Flushing rewrites the whole file from a copy of the mappings, see [`Self::flusher`].
/// Opening decodes the whole file once; reads never touch the disk afterwards.
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
}

impl CompactedTracker {
    fn tracker_file_name(dir: &Path) -> PathBuf {
        dir.join(FILE_NAME)
    }

    /// Create a new empty tracker in the given directory, replacing the file if it already
    /// exists.
    ///
    /// The directory must exist already.
    pub fn new<Fs: UniversalWriteFs>(fs: &Fs, dir: &Path) -> Result<Self> {
        let tracker = Self {
            path: Self::tracker_file_name(dir),
            pointers: Vec::new(),
            dirty: Arc::new(AtomicBool::new(false)),
        };
        fs.atomic_save(&tracker.path, &format::encode(&tracker.pointers)?)?;
        Ok(tracker)
    }

    /// Open an existing tracker in the given directory, decoding the whole file into RAM.
    ///
    /// If the file does not exist or does not decode, return an error.
    pub fn open<Fs: UniversalReadFs>(fs: &Fs, dir: &Path) -> Result<Self> {
        let path = Self::tracker_file_name(dir);
        let options = OpenOptions {
            writeable: false,
            need_sequential: true,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Sequential),
        };
        let file = fs.open(&path, options, Default::default()).map_err(|err| {
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
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Number of mappings: one past the highest point offset that was ever set.
    pub fn pointer_count(&self) -> PointOffset {
        self.pointers.len() as PointOffset
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

    /// Create a flusher that rewrites the whole file from a copy of the current mappings.
    ///
    /// A clean tracker gets a flusher that does nothing. Taking the copy marks the tracker
    /// clean, so mappings set while a flush is in progress are left for the next flush, and a
    /// failed flush marks it dirty again, so the next flush retries. The file is replaced
    /// atomically.
    ///
    /// Rewriting is linear in the number of mappings, which suits a storage that is flushed
    /// once after being built, not one that is flushed after every batch.
    pub fn flusher<Fs>(&self, fs: Fs) -> Flusher
    where
        Fs: UniversalWriteFs + Send + 'static,
    {
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return Box::new(|| Ok(()));
        }

        let path = self.path.clone();
        let pointers = self.pointers.clone();
        let dirty = Arc::clone(&self.dirty);

        Box::new(move || {
            let result = format::encode(&pointers)
                .map_err(BlobstoreError::from)
                .and_then(|bytes| Ok(fs.atomic_save(&path, &bytes)?));
            if result.is_err() {
                dirty.store(true, Ordering::Relaxed);
            }
            result
        })
    }
}
