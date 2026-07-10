use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use super::config::DiskCacheConfig;
use super::file::{DiskCache, State};
use super::pipeline::REMOTE_READ_ALIGNMENT;
use super::{DiskCacheRemote, block_aligned_fetch};
use crate::generic_consts::Sequential;
use crate::mmap::AdviceSetting;
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::{
    ListedFile, OpenExtra, OpenOptions, OwnedPipeline, Populate, Result, UniversalIoError,
    UniversalRead, UniversalReadFileOps, UniversalReadFs, UniversalWriteFileOps,
};

/// Construction context for [`DiskCacheFs`]: carries the
/// remote/local-mirror layout config alongside the remote filesystem's own
/// construction context. Always constructed explicitly — there is no
/// global / default lookup.
pub struct DiskCacheFsContext<C> {
    pub config: Arc<DiskCacheConfig>,
    pub remote: C,
}

#[derive(Default, Debug)]
pub struct DiskCacheFsOpenExtra<RemoteExtra: OpenExtra> {
    /// Extra options passed to the remote
    remote_extra: RemoteExtra,
    /// The length of the file, if known
    known_len: Option<u64>,
}

impl<RemoteExtra: OpenExtra> OpenExtra for DiskCacheFsOpenExtra<RemoteExtra> {
    fn with_prevent_caching(self, _prevent_caching: bool) -> Self {
        self
    }

    fn with_known_len(self, known_len: u64) -> Self {
        Self {
            remote_extra: self.remote_extra,
            known_len: Some(known_len),
        }
    }
}

/// Filesystem handle for the simple disk cache. Carries the remote-side
/// filesystem used to fetch missing blocks plus the shared
/// [`DiskCacheConfig`] that maps remote paths to local mirror paths.
///
/// `DiskCacheFs` is parameterized by the remote *file* type `R`; the
/// underlying remote filesystem is `R::Fs`. This indirection lets the
/// `UniversalRead::Fs = DiskCacheFs<R>` constraint on
/// [`DiskCache<R>`] line up without an extra generic parameter on the
/// file type.
pub struct DiskCacheFs<R>
where
    R: UniversalRead,
{
    config: Arc<DiskCacheConfig>,
    remote_fs: R::Fs,
}

impl<R> Clone for DiskCacheFs<R>
where
    R: UniversalRead,
    R::Fs: Clone,
{
    fn clone(&self) -> Self {
        let Self { config, remote_fs } = self;
        Self {
            config: config.clone(),
            remote_fs: remote_fs.clone(),
        }
    }
}

impl<R> Debug for DiskCacheFs<R>
where
    R: UniversalRead,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { config, remote_fs } = self;
        f.debug_struct("DiskCacheFs")
            .field("config", config)
            .field("remote_fs", remote_fs)
            .finish()
    }
}

impl<R: UniversalRead> DiskCacheFs<R> {
    fn open_remote(
        &self,
        path: impl AsRef<Path>,
        extra: <R::Fs as UniversalReadFs>::OpenExtra,
    ) -> Result<R> {
        let extra = extra.with_prevent_caching(true);
        self.remote_fs.open(
            path.as_ref(),
            OpenOptions {
                writeable: false,
                need_sequential: true,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            extra,
        )
    }
}

impl<R> UniversalReadFileOps for DiskCacheFs<R>
where
    R: UniversalRead,
{
    type ContextConfig = DiskCacheFsContext<<R::Fs as UniversalReadFileOps>::ContextConfig>;

    fn from_context(ctx: Self::ContextConfig) -> Result<Self> {
        let DiskCacheFsContext { config, remote } = ctx;
        Ok(Self {
            config,
            remote_fs: R::Fs::from_context(remote)?,
        })
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<ListedFile>> {
        self.remote_fs.list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        self.remote_fs.exists(path)
    }
}

impl<R> UniversalWriteFileOps for DiskCacheFs<R>
where
    R: UniversalRead,
    R::Fs: UniversalWriteFileOps,
{
    fn create(&self, path: &Path, expected_length: usize) -> Result<()> {
        self.remote_fs.create(path, expected_length)
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        self.remote_fs.create_dir(path)
    }

    fn remove(&self, path: &Path) -> Result<()> {
        self.remote_fs.remove(path)
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        self.remote_fs.remove_dir(path)
    }

    fn atomic_save(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        self.remote_fs.atomic_save(path, bytes)
    }
}

impl<R> UniversalReadFs for DiskCacheFs<R>
where
    R: DiskCacheRemote,
{
    type File = DiskCache<R>;
    type OpenExtra = DiskCacheFsOpenExtra<<R::Fs as UniversalReadFs>::OpenExtra>;

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> Result<DiskCache<R>> {
        if options.writeable {
            return Err(UniversalIoError::Uninitialized {
                description:
                    "DiskCache only supports immutable files, writeable option is not allowed"
                        .to_string(),
            });
        }

        let remote_extra = extra.remote_extra.with_prevent_caching(true);
        let local_path = self.config.local_path_for(path.as_ref())?;

        let populate = if crate::low_memory::low_memory_mode().skip_populate() {
            Populate::No
        } else {
            options.populate
        };

        let state = match (extra.known_len, populate) {
            (None, Populate::Auto | Populate::No) => State::Uninit,
            // We know file length, initialize local state.
            (Some(len), Populate::Auto | Populate::No) => {
                let remote = self.open_remote(path.as_ref(), remote_extra.clone())?;

                let local = LocalState::new(&local_path, len, options)?;

                State::Ready { remote, local }
            }
            // Even if we know length, we don't need it to do `schedule_whole`.
            (None | Some(_), Populate::Blocking | Populate::PreferBackground) => {
                let remote = self.open_remote(path.as_ref(), remote_extra.clone())?;

                let mut pipeline = OwnedPipeline::new(remote)?;

                // FIXME: check `can_schedule` in a loop first
                pipeline.schedule_whole((), 0)?;

                State::OpenPrefill { pipeline }
            }
            // Special case of no known length and empty range. Don't initialize.
            (None, Populate::Partial(range)) if range.into_byte_range::<u8>().is_empty() => {
                State::Uninit
            }
            // Schedule a partial block-aligned read.
            (known_len, Populate::Partial(range)) => {
                let remote = self.open_remote(path.as_ref(), remote_extra.clone())?;

                // `Partial` must create the local file, so we need the length up front.
                let file_len = match known_len {
                    Some(len) => len,
                    None => remote.len::<u8>()?,
                };

                let requested_byte_range = range.into_byte_range::<u8>();

                if let Some((blocks_range, byte_range)) =
                    block_aligned_fetch(requested_byte_range.clone(), file_len)
                {
                    let mut pipeline = OwnedPipeline::new(remote)?;

                    // FIXME: check `can_schedule` in a loop first
                    pipeline.schedule::<Sequential>(
                        blocks_range,
                        byte_range,
                        REMOTE_READ_ALIGNMENT,
                    )?;

                    State::PartialPrefill {
                        pipeline,
                        len: file_len,
                    }
                } else {
                    // empty byte range, just initialize with length.
                    let local = LocalState::new(&local_path, file_len, options)?;
                    State::Ready { remote, local }
                }
            }
        };

        let cache = DiskCache::new(
            self.remote_fs.clone(),
            remote_extra,
            path.as_ref(),
            local_path,
            options,
            state,
        );

        if matches!(populate, Populate::Blocking) {
            // Force the prefill to resolve before returning.
            cache.state()?;
        }

        Ok(cache)
    }
}
