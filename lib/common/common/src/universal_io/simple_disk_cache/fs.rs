use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use super::DiskCacheRemote;
use super::config::DiskCacheConfig;
use super::file::{DiskCache, State};
use crate::mmap::AdviceSetting;
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::remote_manifest::{FileInfo, RemoteManifest};
use crate::universal_io::{
    ListedFile, OpenExtra, OpenOptions, OwnedPipeline, Populate, Result, UniversalIoError,
    UniversalRead, UniversalReadFileOps, UniversalReadFs,
};

/// Construction context for [`DiskCacheFs`]: carries the
/// remote/local-mirror layout config alongside the remote filesystem's own
/// construction context. Always constructed explicitly — there is no
/// global / default lookup.
pub struct DiskCacheFsContext<C> {
    pub config: Arc<DiskCacheConfig>,
    pub remote_ctx: C,
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
#[derive(Debug)]
pub struct DiskCacheFs<R>
where
    R: UniversalRead,
{
    config: Arc<DiskCacheConfig>,
    manifest: Arc<RemoteManifest>,
    remote_fs: R::Fs,
}

impl<R> DiskCacheFs<R>
where
    R: UniversalRead,
{
    fn open_remote(
        &self,
        path: impl AsRef<Path>,
        extra: <<R as UniversalRead>::Fs as UniversalReadFs>::OpenExtra,
    ) -> Result<R> {
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

impl<R> Clone for DiskCacheFs<R>
where
    R: UniversalRead,
    R::Fs: Clone,
{
    fn clone(&self) -> Self {
        let Self {
            config,
            manifest,
            remote_fs,
        } = self;
        Self {
            config: config.clone(),
            manifest: manifest.clone(),
            remote_fs: remote_fs.clone(),
        }
    }
}

impl<R> UniversalReadFileOps for DiskCacheFs<R>
where
    R: UniversalRead,
{
    type ContextConfig = DiskCacheFsContext<<R::Fs as UniversalReadFileOps>::ContextConfig>;

    fn from_context(ctx: Self::ContextConfig) -> Result<Self> {
        let DiskCacheFsContext { config, remote_ctx } = ctx;
        let remote_fs = R::Fs::from_context(remote_ctx)?;
        let manifest = Arc::new(RemoteManifest::new(&remote_fs, config.remote_dir())?);
        Ok(Self {
            config,
            manifest,
            remote_fs,
        })
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<ListedFile>> {
        Ok(self.manifest.list_files(prefix_path))
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        Ok(self.manifest.exists(path))
    }

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
    type OpenExtra = <R::Fs as UniversalReadFs>::OpenExtra;

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

        let extra = extra.with_prevent_caching(true);
        let local_path = self.config.local_path_for(path.as_ref())?;

        let populate = if crate::low_memory::low_memory_mode().skip_populate() {
            Populate::No
        } else {
            options.populate
        };

        let file_info = self.manifest.get(path.as_ref());

        let state = match (file_info, populate) {
            // Initialize with known info
            (Some(file_info), Populate::Auto | Populate::No) => {
                let FileInfo { size } = file_info;
                let local = LocalState::new(&local_path, *size, options)?;

                let remote = self.open_remote(path.as_ref(), extra.clone())?;

                State::Ready { local, remote }
            }
            // Initialize with known info, but finish populating.
            (Some(file_info), Populate::Blocking | Populate::PreferBackground) => {
                let FileInfo { size } = file_info;
                let local = LocalState::new(&local_path, *size, options)?;

                let remote = self.open_remote(path.as_ref(), extra.clone())?;

                let mut pipeline = OwnedPipeline::new(remote)?;

                // FIXME: check `can_schedule` in a loop first
                pipeline.schedule_whole(0, 0)?;

                State::ReopenPrefill { pipeline, local }
            }
            // No prefetched info, keep uninitialized.
            (None, Populate::Auto | Populate::No) => State::Uninit,
            // No prefetched info, schedule population.
            (None, Populate::Blocking | Populate::PreferBackground) => {
                let remote = self.remote_fs.open(
                    path.as_ref(),
                    OpenOptions {
                        writeable: false,
                        need_sequential: true,
                        populate: Populate::No,
                        advice: AdviceSetting::Global,
                    },
                    extra.clone(),
                )?;

                let mut pipeline = OwnedPipeline::new(remote)?;

                // FIXME: check `can_schedule` in a loop first
                pipeline.schedule_whole((), 0)?;

                State::OpenPrefill { pipeline }
            }
        };

        let cache = DiskCache::new(
            self.remote_fs.clone(),
            extra,
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
