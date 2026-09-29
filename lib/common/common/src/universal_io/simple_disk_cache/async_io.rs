//! The async surface of the simple disk cache: async opens with async
//! prefill ([`UniversalReadFsAsync`] for [`DiskCacheFs`]) and async
//! miss-fetching reads ([`UniversalReadAsync`] for [`DiskCache`]). Both fetch
//! from the remote via [`UniversalReadAsync::read_bytes_async`], guaranteed
//! by the [`DiskCacheRemote`] bundle.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::file::{DiskCache, State};
use super::fs::{DiskCacheFs, unique_local_path};
use super::local_state::LocalState;
use super::pipeline::{REMOTE_READ_ALIGNMENT, Source, pick_source, read_local};
use super::{DiskCacheRemote, block_aligned_fetch};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Random, Sequential};
use crate::universal_io::{
    ListedFile, OpenExtra, OpenOptions, Populate, UioResult, UniversalIoError, UniversalReadAsync,
    UniversalReadFsAsync,
};

impl<R> UniversalReadFsAsync for DiskCacheFs<R>
where
    R: DiskCacheRemote,
{
    async fn open_async(
        &self,
        path: PathBuf,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> UioResult<Self::File> {
        let populate = if crate::low_memory::low_memory_mode().skip_populate() {
            Populate::No
        } else {
            options.populate
        };

        let local_path = unique_local_path(self.config.local_path_for(path.as_ref())?);

        let remote_extra = extra.remote_extra.clone().with_prevent_caching(true);

        let state = match populate {
            Populate::Auto | Populate::No => match extra.known_len {
                Some(len) => State::ready(
                    self.open_remote(&path, remote_extra.clone())?,
                    LocalState::new(&local_path, len, options)?,
                ),
                None => State::Uninit,
            },
            Populate::PreferBackground | Populate::Blocking => {
                let remote = self.open_remote(&path, remote_extra.clone())?;
                let len = match extra.known_len {
                    Some(len) => len,
                    None => remote.len::<u8>()?,
                };
                // An empty object has nothing to populate, and a bounded `0..0`
                // range is rejected by the backend rather than answered with an
                // empty body (see `AsyncRead::read_range`), so skip the fetch.
                let local = if len > 0 {
                    prefill_whole(&remote, &local_path, len, options).await?
                } else {
                    LocalState::new(&local_path, len, options)?
                };
                State::ready(remote, local)
            }
            Populate::Partial(read_range) => {
                let remote = self.open_remote(&path, remote_extra.clone())?;
                let file_len = match extra.known_len {
                    Some(len) => len,
                    None => remote.len::<u8>()?,
                };
                let byte_range = read_range.into_byte_range::<u8>();
                let (block_range, fetch_range) =
                    block_aligned_fetch(byte_range, file_len).expect("range should not be empty");

                let content = remote
                    .read_bytes_async(fetch_range.clone(), Sequential, REMOTE_READ_ALIGNMENT)
                    .await?;

                let local = LocalState::new(&local_path, file_len, options)?;
                unsafe { local.write_mmap_bytes(&content, block_range) };
                State::ready(remote, local)
            }
        };

        Ok(DiskCache::new(
            self,
            remote_extra,
            path,
            local_path,
            options,
            state,
            extra.known_etag,
        ))
    }

    async fn list_files_async(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        self.remote_fs.list_files_async(prefix_path).await
    }
}

/// Build the mirror of a `len`-byte remote file, streaming the whole file into it: each
/// chunk is written as it arrives, so copying overlaps the transfer.
async fn prefill_whole<R: DiskCacheRemote>(
    remote: &R,
    local_path: &Path,
    len: u64,
    options: OpenOptions,
) -> UioResult<LocalState> {
    let local = Arc::new(LocalState::new(local_path, len, options)?);
    let sink_local = Arc::clone(&local);
    let fetched_len = remote
        .read_whole_into_async(move |offset, bytes| {
            // SAFETY: nothing else accesses the mirror until the open returns, and the sink
            // is called sequentially.
            unsafe { sink_local.write_mmap_at(offset, bytes) }
        })
        .await?;
    if fetched_len != len {
        return Err(UniversalIoError::Io(std::io::Error::other(format!(
            "remote file changed size during prefill: expected {len} bytes, got {fetched_len}",
        ))));
    }
    local.mark_fully_fetched();
    Ok(Arc::into_inner(local).expect("the sink is dropped once the read resolves"))
}

impl<R> UniversalReadAsync for DiskCache<R>
where
    R: DiskCacheRemote,
{
    async fn read_bytes_async<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        _align: usize,
    ) -> UioResult<ACow<'_>> {
        // warn: first-touch init (`state()`) still does blocking I/O; only the
        // block fetch itself is async.
        //
        // TODO(uio): This is a targeted use of async, but maybe later we'd want
        // a proper async pipeline
        let state = self.state()?;
        match pick_source::<P>(state.local, range.clone())? {
            Source::Local {
                range,
                is_sequential,
            } => {
                // SAFETY: Source::Local confirms the range is local (or empty).
                let bytes = unsafe { read_local::<R>(self, range, is_sequential)? };
                Ok(ACow::Borrowed(bytes))
            }
            Source::Remote {
                blocks_range,
                blocks_byte_range,
            } => {
                let bytes = state
                    .remote
                    .read_bytes_async(blocks_byte_range, access_pattern, REMOTE_READ_ALIGNMENT)
                    .await?;
                // SAFETY: `bytes` is the remote content of `blocks_range`
                // (clamped to EOF), which covers `range`.
                unsafe {
                    state.local.write_mmap_bytes(&bytes, blocks_range);
                    Ok(ACow::Borrowed(
                        state.local.read_mmap_bytes::<Random>(range)?,
                    ))
                }
            }
        }
    }
}
