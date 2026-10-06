//! The write half of a move log, owned by the update-only id trackers.

use std::path::{Path, PathBuf};

use common::generic_consts::Sequential;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    IsNotFound as _, OkNotFound as _, OpenOptions, Populate, UniversalAppend, UniversalAppendFs,
    UniversalFlush as _, UniversalKind, UniversalRead,
};

use super::format::{decode_entries, encode_entry};
use super::{MoveEntry, point_moves_path};
use crate::common::operation_error::{OperationError, OperationResult};

/// Appends entries to the move log of one segment.
///
/// Every append is durable when it returns, like the other update-only tracker files: it appends
/// and then runs the handle's flusher. The file is created by the first append.
///
/// Appends land at the end of the last complete entry, with that offset as compare-and-swap, as in
/// the mappings log. The writer learns the end on its first append. On object stores an append
/// lands whole or not at all, so the file's size always ends on an entry boundary. On local files a
/// crash can tear an append: there the log is scanned for the end of its last valid entry, and a
/// torn tail is cut off first, since readers stop at it and would never see what follows.
#[derive(Debug)]
pub struct PointMovesWriter {
    path: PathBuf,
    /// Byte offset just past the last complete entry, where the next append lands. `None` until the
    /// first append has found it.
    end: Option<u64>,
}

impl PointMovesWriter {
    pub fn new(segment_path: &Path) -> Self {
        Self {
            path: point_moves_path(segment_path),
            end: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Durably append `entries` in order, in one append. Entries without pairs are skipped.
    pub fn append<Fs: UniversalAppendFs>(
        &mut self,
        fs: &Fs,
        entries: &[MoveEntry],
    ) -> OperationResult<()> {
        let buffers: Vec<Vec<u8>> = entries
            .iter()
            .filter(|entry| !entry.pairs.is_empty())
            .map(|entry| {
                let mut buffer = Vec::new();
                encode_entry(entry, &mut buffer);
                buffer
            })
            .collect();
        if buffers.is_empty() {
            return Ok(());
        }

        let mut file = open_append(fs, &self.path)?;
        let end = match self.end {
            Some(end) => end,
            None => {
                let (end, file_len) = find_end(&file)?;
                if end < file_len {
                    log::warn!(
                        "Move log holds a torn entry in its last {} bytes, dropping them: {}",
                        file_len - end,
                        self.path.display(),
                    );
                    let healthy = file
                        .read_bytes(0..end, Sequential, align_of::<u8>())?
                        .to_vec();
                    // Release the mmap before replacing the path, Windows refuses otherwise
                    drop(file);
                    fs.atomic_save(&self.path, &healthy)?;
                    file = open_append(fs, &self.path)?;
                }
                end
            }
        };

        if let Err(err) = file.append_batch(end, buffers.iter().map(Vec::as_slice)) {
            // Either another writer appended, or an earlier append of this writer landed without
            // being acknowledged. A fresh writer finds the real end again.
            if err.is_append_offset_conflict() {
                self.end = None;
            }
            return Err(err.into());
        }
        (file.flusher())()?;

        let appended: usize = buffers.iter().map(Vec::len).sum();
        self.end = Some(end + appended as u64);
        Ok(())
    }
}

/// The end of the last complete entry of the log behind `file`, and the file's length.
fn find_end<F: UniversalAppend>(file: &F) -> OperationResult<(u64, u64)> {
    // `NotFound` means a lazy backend has not materialized the object yet, so it is empty
    let file_len = file.len::<u8>().ok_not_found()?.unwrap_or(0);
    if file_len == 0 || appends_are_atomic(<F as UniversalRead>::kind()) {
        return Ok((file_len, file_len));
    }

    let bytes = file.read_bytes(0..file_len, Sequential, align_of::<u8>())?;
    let (_, end) = decode_entries(&bytes);
    Ok((end as u64, file_len))
}

/// Whether an append on this backend lands whole or not at all, so a file's size always ends on an
/// entry boundary. Unknown backends are scanned, which is always correct.
fn appends_are_atomic(kind: UniversalKind) -> bool {
    match kind {
        UniversalKind::S3
        | UniversalKind::Gcs
        | UniversalKind::Azure
        | UniversalKind::CachedBlob => true,
        UniversalKind::Mmap
        | UniversalKind::IoUring
        | UniversalKind::DiskCache
        | UniversalKind::SimpleDiskCache
        | UniversalKind::UioGrpc => false,
    }
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
