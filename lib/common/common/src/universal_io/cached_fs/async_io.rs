//! The [`UniversalReadFsAsync`] impl for [`CachedFs`]: prefetch-pool-aware
//! async opens, delegating to the inner filesystem's `open_async`.

use std::path::PathBuf;

use super::{CachedFs, ScheduledFile};
use crate::universal_io::{
    OpenExtra, OpenOptions, UioResult, UniversalIoError, UniversalReadFsAsync,
};

impl<Fs: UniversalReadFsAsync> UniversalReadFsAsync for CachedFs<Fs> {
    async fn open_async(
        &self,
        path: PathBuf,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> UioResult<Self::File> {
        if options.writeable {
            return Err(UniversalIoError::Uninitialized {
                description:
                    "CachedReadFs only supports read-only files, writeable option is not allowed"
                        .to_string(),
            });
        }

        let scheduled_file = self.files_prefetched.lock().remove(&path);
        if let Some(file) = scheduled_file {
            return match file {
                ScheduledFile::Future(future) => future.await,
                ScheduledFile::Ready(result) => result,
                ScheduledFile::Unchanged => Err(UniversalIoError::UnchangedOpen {
                    since: self.file_info(&path).and_then(|info| info.last_modified),
                    path,
                }),
            };
        }

        // With a snapshot, unlisted paths fail locally — probing for
        // optional files never reaches the inner filesystem.
        if let Some(files_info) = &self.files_info
            && !files_info.contains_key(&path)
        {
            return Err(UniversalIoError::NotFound { path });
        }

        // The path was never scheduled for prefetch. If a snapshot was taken it
        // still carries the file's size, so thread it into the open as a known
        // length — this lets the backend skip a remote `len`/HEAD round-trip
        // (e.g. `DiskCacheFs` opens straight into `State::Ready`). Without a
        // snapshot this is a plain cache-bypass open.
        let extra = match self.file_info(&path) {
            Some(info) => extra.with_known_len(info.size),
            None => extra,
        };
        self.fs.open_async(path, options, extra).await
    }
}
