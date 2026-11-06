//! The read half of a delete log.

use std::path::{Path, PathBuf};

use common::generic_consts::Sequential;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    CachedReadFs, OkNotFound as _, OpenOptions, Populate, PreloadFuture, ReadRange, UniversalRead,
    UniversalReadFs,
};
use futures::FutureExt;

use super::log_format::{HEADER_LEN, decode_frames, has_header};
use super::{DeleteLogEntry, delete_log_path};
use crate::common::operation_error::OperationResult;

/// A delete log as a reader follows it: frames are consumed in order, and a read never goes past
/// the last complete frame, so a frame still in flight is picked up by a later read.
///
/// The file may be absent, or hold an incomplete header while its first append is in flight: the
/// log then holds no deletes yet.
#[derive(Debug)]
pub struct DeleteLogReader<S: UniversalRead> {
    path: PathBuf,
    /// Handle for reads bounded by the listing snapshot; `None` until the file exists.
    file: Option<S>,
    /// Byte offset just past the header or the last frame consumed, 0 before the header was read.
    read_to: u64,
}

impl<S: UniversalRead> DeleteLogReader<S> {
    pub fn new(segment_path: &Path) -> Self {
        Self {
            path: delete_log_path(segment_path),
            file: None,
            read_to: 0,
        }
    }

    /// Whether the header was read.
    pub fn is_started(&self) -> bool {
        self.read_to > 0
    }

    fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Advice(Advice::Normal),
        }
    }

    /// Schedule the prefetch of the log for the first [`read_new`](Self::read_new); absence is
    /// tolerated.
    pub fn preopen(fs: &impl CachedReadFs<File = S>, segment_path: &Path) {
        fs.schedule_open(
            &delete_log_path(segment_path),
            Some(Self::open_options()),
            None,
        );
    }

    /// Stage the next [`read_new`](Self::read_new): a reopen of the held handle, or a prefetch of a
    /// file not opened yet.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<PreloadFuture>> {
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
    /// backend, the size in the listing snapshot of `fs`. Empty while the log has no header.
    pub fn read_new(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<Vec<DeleteLogEntry>> {
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

        let bytes = file.read::<_, u8>(ReadRange::new(start, file_len - start), Sequential)?;
        let mut frames = bytes.as_ref();
        let mut consumed = 0;
        if self.read_to == 0 {
            if !has_header(frames)? {
                return Ok(Vec::new());
            }
            frames = &frames[HEADER_LEN..];
            consumed = HEADER_LEN;
        }
        let (entries, frames_len) = decode_frames(frames);
        self.read_to = start + (consumed + frames_len) as u64;
        Ok(entries)
    }
}
