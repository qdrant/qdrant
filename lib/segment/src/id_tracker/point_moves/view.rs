//! The read half of a move log, held by the read-only id trackers that resolve moves.

use std::path::{Path, PathBuf};

use common::generic_consts::Sequential;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    CachedReadFs, OkNotFound as _, OpenOptions, Populate, ReadRange, UniversalRead, UniversalReadFs,
};
use futures::FutureExt;
use futures::future::BoxFuture;

use super::format::decode_entries;
use super::{MoveEntry, point_moves_path};
use crate::common::operation_error::OperationResult;

/// A move log as a follower reads it: entries are consumed in order, and a read never goes past
/// the last complete entry, so an entry still in flight is picked up by a later read.
///
/// The file may be absent: the writer creates it with its first record. A missing file is an empty
/// log, opened lazily once it appears.
#[derive(Debug)]
pub struct PointMovesView<S: UniversalRead> {
    path: PathBuf,
    /// Handle for reads bounded by the listing snapshot; `None` until the file exists.
    file: Option<S>,
    /// Byte offset just past the last entry consumed.
    read_to: u64,
}

impl<S: UniversalRead> PointMovesView<S> {
    pub fn new(segment_path: &Path) -> Self {
        Self {
            path: point_moves_path(segment_path),
            file: None,
            read_to: 0,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Byte offset just past the last entry consumed.
    pub fn read_to(&self) -> u64 {
        self.read_to
    }

    fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Advice(Advice::Normal),
        }
    }

    /// Schedule the prefetch of the log for [`read_new`](Self::read_new); absence is tolerated.
    pub fn preopen(fs: &impl CachedReadFs<File = S>, segment_path: &Path) {
        fs.schedule_open(
            &point_moves_path(segment_path),
            Some(Self::open_options()),
            None,
        );
    }

    /// Stage the next [`read_new`](Self::read_new): a reopen of the held handle, or a prefetch of a
    /// file not opened yet.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        match &self.file {
            Some(file) => Ok(file
                .live_preload(|path| fs.cached_file_info(path))
                .ok_not_found()?
                .map(FutureExt::boxed)
                .into_iter()
                .collect()),
            None => {
                fs.schedule_open(&self.path, Some(Self::open_options()), None);
                Ok(Vec::new())
            }
        }
    }

    /// Entries appended since the last read, up to the length the handle knows: on a caching
    /// backend, the size in the listing snapshot of `fs`.
    pub fn read_new(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<Vec<MoveEntry>> {
        match self.file.as_mut() {
            // A lazily-opened handle whose object does not exist yet reports `NotFound`
            Some(file) => file.live_reload().ok_not_found().map(|_| ())?,
            None => {
                self.file = fs
                    .open(&self.path, Self::open_options(), Default::default())
                    .ok_not_found()?;
            }
        }

        let Some(file) = self.file.as_ref() else {
            return Ok(Vec::new());
        };
        let Some(file_len) = file.len::<u8>().ok_not_found()? else {
            return Ok(Vec::new());
        };
        let start = self.read_to.min(file_len);
        if start >= file_len {
            return Ok(Vec::new());
        }

        let bytes = file
            .read::<_, u8>(ReadRange::new(start, file_len - start), Sequential)?
            .into_owned();
        Ok(self.consume(start, &bytes))
    }

    /// Consume `bytes`, read from the log at byte offset `start`: the entries in it, up to the last
    /// complete one. Ignored unless `start` is where the last read ended.
    pub fn consume(&mut self, start: u64, bytes: &[u8]) -> Vec<MoveEntry> {
        if start != self.read_to {
            return Vec::new();
        }
        let (entries, consumed) = decode_entries(bytes);
        self.read_to += consumed as u64;
        entries
    }
}

/// Read the move log at `path` from byte offset `start` up to its current end, through a fresh
/// handle from `fs`, so the length comes from the backend rather than a listing snapshot. Empty if
/// the file does not exist.
///
/// This is the read that classifies a tombstone: it starts after the tombstone was read, so it
/// covers every record the writer appended before that tombstone.
pub fn read_point_moves_tail<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    path: &Path,
    start: u64,
) -> OperationResult<Vec<u8>> {
    let options = OpenOptions {
        writeable: false,
        need_sequential: false,
        // Only the tail is read, populating would fetch the whole file
        populate: Populate::No,
        advice: AdviceSetting::Advice(Advice::Normal),
    };
    let Some(file) = fs.open(path, options, Default::default()).ok_not_found()? else {
        return Ok(Vec::new());
    };
    let Some(file_len) = file.len::<u8>().ok_not_found()? else {
        return Ok(Vec::new());
    };
    if start >= file_len {
        return Ok(Vec::new());
    }
    let bytes = file.read::<_, u8>(ReadRange::new(start, file_len - start), Sequential)?;
    Ok(bytes.into_owned())
}
