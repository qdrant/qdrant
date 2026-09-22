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

use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    IsNotFound, OpenOptions, Populate, UniversalRead, UniversalReadFs, UniversalWriteFileOps,
};
use parking_lot::Mutex;

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
/// Mappings must be set in monotonically increasing point offset order, like in
/// [`AppendOnlyTracker`](super::append_only::AppendOnlyTracker). Skipped point offsets read as
/// `None`.
///
/// Flushing rewrites the whole file from a snapshot of the mappings, see [`Self::flusher`].
/// Opening decodes the whole file once; reads never touch the disk afterwards.
#[derive(Debug)]
pub struct CompactedTracker {
    /// Path to the tracker file
    path: PathBuf,
    /// Entry `i` is the mapping for point offset `i`
    pointers: Vec<Option<ValuePointer>>,
    /// Number of mappings the file holds, held while the file is being written.
    ///
    /// Shared with the flushers, so that a stale flush never replaces a newer file.
    persisted_count: Arc<Mutex<PointOffset>>,
}

impl CompactedTracker {
    fn tracker_file_name(dir: &Path) -> PathBuf {
        dir.join(FILE_NAME)
    }

    /// Create a new empty tracker in the given directory, replacing the file if it already
    /// exists.
    ///
    /// The directory must exist already.
    pub fn new<Fs: UniversalWriteFileOps>(fs: &Fs, dir: &Path) -> Result<Self> {
        let tracker = Self {
            path: Self::tracker_file_name(dir),
            pointers: Vec::new(),
            persisted_count: Arc::new(Mutex::new(0)),
        };
        fs.atomic_save(&tracker.path, &format::encode(&tracker.pointers))?;
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

        let count = pointers.len() as PointOffset;
        Ok(Self {
            path,
            pointers,
            persisted_count: Arc::new(Mutex::new(count)),
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Number of mappings.
    ///
    /// This is one past the highest point offset that was ever set, which makes it the next point
    /// offset that is allowed to be set.
    pub fn pointer_count(&self) -> PointOffset {
        self.pointers.len() as PointOffset
    }

    /// Set the mapping for the given point offset.
    ///
    /// Point offsets must be set in monotonically increasing order: each offset must be larger
    /// than every offset set before it. Skipped offsets are backfilled as `None` entries.
    pub fn set(&mut self, point_offset: PointOffset, pointer: ValuePointer) -> Result<()> {
        // Defensive re-check: the storage validates this before appending any value data, see
        // Logstore::put_value
        let next = self.pointer_count();
        if point_offset < next {
            return Err(BlobstoreError::unsupported_operation(format!(
                "cannot set mapping for point offset {point_offset}, the tracker requires \
                 monotonically increasing point offsets, the next allowed point offset is {next}",
            )));
        }

        self.pointers.resize(point_offset as usize, None);
        self.pointers.push(Some(pointer));

        Ok(())
    }

    /// Create a flusher that rewrites the whole file from the mappings set up to this point.
    ///
    /// The mappings are encoded right away, so that mappings set while a flush is in progress
    /// are left for the next flush. The file is replaced atomically, and a stale flush, whose
    /// snapshot holds no more mappings than the file already does, is a no-op: a flush must
    /// never make the file lose mappings a more recent flush persisted.
    ///
    /// Rewriting is linear in the number of mappings, which suits a storage that is flushed
    /// once after being built, not one that is flushed after every batch.
    pub fn flusher<Fs>(&self, fs: Fs) -> Flusher
    where
        Fs: UniversalWriteFileOps + Send + 'static,
    {
        let path = self.path.clone();
        let count = self.pointer_count();
        let bytes = format::encode(&self.pointers);
        let persisted_count = Arc::clone(&self.persisted_count);

        Box::new(move || {
            let mut persisted_count = persisted_count.lock();
            if count <= *persisted_count {
                return Ok(());
            }
            fs.atomic_save(&path, &bytes)?;
            *persisted_count = count;
            Ok(())
        })
    }
}
