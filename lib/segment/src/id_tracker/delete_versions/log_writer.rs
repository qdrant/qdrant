//! The write half of a delete log, used by the update-only trackers of immutable and
//! disk-resident segments when they record delete versions.

use std::path::{Path, PathBuf};

use common::generic_consts::Sequential;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    IsNotFound as _, OkNotFound as _, OpenOptions, Populate, UniversalAppend, UniversalAppendFs,
    UniversalFlush as _, UniversalRead,
};

use super::log_format::{HEADER_LEN, decode_frames, encode_frame, encode_header, has_header};
use super::{DeleteLogEntry, delete_log_path};
use crate::common::operation_error::{OperationError, OperationResult};

/// Appends frames of deletes to the delete log of one segment.
///
/// Every append is durable when it returns: it appends and then runs the handle's flusher.
///
/// Appends land at the end of the last complete frame, with that offset as compare-and-swap, as in
/// the appendable mappings log. The writer learns the end on its first append. On object stores an
/// append lands whole or not at all, so the file's size always ends on a frame boundary. On local
/// files a crash can tear an append: there the log is scanned for the end of its last valid frame,
/// and a torn tail is cut off first, since readers stop at it and would never see what follows.
#[derive(Debug)]
pub struct DeleteLogWriter {
    path: PathBuf,
    /// Byte offset just past the last complete frame, where the next append lands; 0 while the log
    /// has no header. `None` until the first append has found it.
    end: Option<u64>,
}

impl DeleteLogWriter {
    pub fn new(segment_path: &Path) -> Self {
        Self {
            path: delete_log_path(segment_path),
            end: None,
        }
    }

    /// Durably append `entries` as one frame, writing the header first if the log has none yet.
    /// Nothing is written for no entries.
    pub fn append<Fs: UniversalAppendFs>(
        &mut self,
        fs: &Fs,
        entries: &[DeleteLogEntry],
    ) -> OperationResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let end = match self.end {
            Some(end) => end,
            None => self.find_end(fs)?,
        };
        let mut buffer = Vec::new();
        if end == 0 {
            encode_header(&mut buffer);
        }
        encode_frame(entries, &mut buffer);

        let mut file = open_append(fs, &self.path)?;
        if let Err(err) = file.append(end, &buffer) {
            // Either another writer appended, or an earlier append of this writer landed without
            // being acknowledged. A fresh writer finds the real end again.
            if err.is_append_offset_conflict() {
                self.end = None;
            }
            return Err(err.into());
        }
        (file.flusher())()?;

        self.end = Some(end + buffer.len() as u64);
        Ok(())
    }

    /// Find where the next append lands, cutting a torn tail off the log first. 0 when the log has
    /// no complete header, so the next append starts it over.
    fn find_end<Fs: UniversalAppendFs>(&mut self, fs: &Fs) -> OperationResult<u64> {
        let file = match fs.open_append(&self.path, open_options()) {
            Ok(file) => file,
            Err(err) if err.is_not_found() => {
                self.end = Some(0);
                return Ok(0);
            }
            Err(err) => return Err(err.into()),
        };
        let (end, file_len) = valid_end(&file)?;
        if end < file_len {
            log::warn!(
                "Delete log holds a torn header or frame in its last {} bytes, dropping them: {}",
                file_len - end,
                self.path.display(),
            );
            let healthy = file
                .read_bytes(0..end, Sequential, align_of::<u8>())?
                .to_vec();
            // Release the mmap before replacing the path, Windows refuses otherwise
            drop(file);
            fs.atomic_save(&self.path, &healthy)?;
        }
        self.end = Some(end);
        Ok(end)
    }
}

/// The end of the last complete frame of the log behind `file`, 0 without a complete header, and
/// the file's length.
fn valid_end<F: UniversalAppend>(file: &F) -> OperationResult<(u64, u64)> {
    // `NotFound` means a lazy backend has not materialized the object yet, so it is empty
    let file_len = file.len::<u8>().ok_not_found()?.unwrap_or(0);
    if file_len == 0 {
        return Ok((0, 0));
    }
    // Where appends land whole or not at all, the size always ends on a frame boundary
    if <F as UniversalRead>::kind().appends_atomically() {
        return Ok((file_len, file_len));
    }

    let bytes = file.read_bytes(0..file_len, Sequential, align_of::<u8>())?;
    if !has_header(&bytes)? {
        return Ok((0, file_len));
    }
    let (_, frames) = decode_frames(&bytes[HEADER_LEN..]);
    Ok(((HEADER_LEN + frames) as u64, file_len))
}

fn open_options() -> OpenOptions {
    OpenOptions {
        writeable: true,
        need_sequential: false,
        // Appends never read back, and on a remote backend populating would fetch the whole file
        populate: Populate::No,
        advice: AdviceSetting::Advice(Advice::Normal),
    }
}

/// Open the append handle for `path`, creating the file if it is not there yet.
fn open_append<Fs: UniversalAppendFs>(fs: &Fs, path: &Path) -> OperationResult<Fs::AppendFile> {
    match fs.open_append(path, open_options()) {
        Ok(file) => Ok(file),
        Err(err) if err.is_not_found() => {
            fs.create(path, 0)?;
            Ok(fs.open_append(path, open_options())?)
        }
        Err(err) => Err(OperationError::from(err)),
    }
}
