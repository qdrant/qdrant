use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;

use crate::mmap::AdviceSetting;
use crate::universal_io::traits::CachedReadFs;
use crate::universal_io::{
    ListedFile, OpenExtra, OpenOptions, Populate, UioResult, UniversalIoError,
    UniversalReadFileOps, UniversalReadFs,
};

#[derive(Clone, Debug)]
pub struct FileInfo {
    /// Length in bytes of the entire file
    pub size: u64,
    /// Last modification time, when the listing backend exposes one
    pub last_modified: Option<std::time::SystemTime>,
    /// Entity tag, when the listing backend exposes one (object stores)
    pub etag: Option<String>,
}

impl FileInfo {
    /// Return true if both `FileInfo` have all data and it's equal.
    pub fn full_eq(&self, other: &FileInfo) -> bool {
        let Self {
            size,
            last_modified: Some(last_modified),
            etag: Some(etag),
        } = self
        else {
            return false;
        };

        let Self {
            size: other_size,
            last_modified: Some(other_last_modified),
            etag: Some(other_etag),
        } = other
        else {
            return false;
        };

        size == other_size && last_modified == other_last_modified && etag == other_etag
    }
}

/// Read-only filesystem wrapper that snapshots the file listing and serves
/// opens from explicitly prefetched handles. The only [`CachedReadFs`]
/// implementation.
///
/// Opens produce the *wrapped* backend's file type (`Fs::File`), so
/// components generic over `impl UniversalReadFs<File = S>` accept a raw
/// backend and this wrapper interchangeably, and stored handle types never
/// mention the wrapper.
///
/// Until [`CachedFs::cache_file_info`] takes the listing snapshot, the
/// wrapper is a passthrough: listing, existence checks and opens forward to
/// the inner filesystem unchanged (prefetched handles are still consumed
/// first).
///
/// Once the snapshot is taken, listing and existence checks are answered
/// from it without touching the inner filesystem, and opens of paths absent
/// from the snapshot fail with `NotFound` locally — probing for optional
/// files is free. Opens first consume any handle registered via
/// [`CachedFs::schedule_prefetch`]; opens of listed files that were not
/// scheduled fall back to a plain inner open.
///
/// Prefetched handles are take-once: [`UniversalReadFs::open`] removes the
/// handle from the pool and returns it owned. The pool is shared across
/// clones.
pub struct CachedFs<Fs>
where
    Fs: UniversalReadFs,
    Fs::File: 'static,
{
    fs: Fs,
    prefix_path: PathBuf,
    /// `None` until [`CachedFs::cache_file_info`] takes the listing
    /// snapshot; the wrapper forwards to `fs` until then.
    files_info: Option<HashMap<PathBuf, FileInfo>>,
    /// Previous listing snapshot.
    previous_files_info: Option<HashMap<PathBuf, FileInfo>>,
    files_prefetched: Arc<Mutex<HashMap<PathBuf, ScheduledFile<Fs::File>>>>,
    runtime: tokio::runtime::Handle,
}

enum ScheduledFile<S: 'static> {
    Future(Pin<Box<dyn Future<Output = UioResult<S>> + Send + 'static>>),
    Ready(UioResult<S>),
    Unchanged,
}

impl<S> Debug for ScheduledFile<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScheduledFile::Future(_) => write!(f, "Future"),
            ScheduledFile::Ready(_) => write!(f, "Ready"),
            ScheduledFile::Unchanged => write!(f, "Unchanged"),
        }
    }
}

/// Manual impl: `derive(Clone)` would add a spurious `Fs::File: Clone`
/// bound for the projection in `files_prefetched`, even though the
/// `Arc` field is unconditionally cloneable (rust-lang/rust#26925).
impl<Fs: UniversalReadFs> Clone for CachedFs<Fs> {
    fn clone(&self) -> Self {
        let Self {
            fs,
            prefix_path,
            files_info,
            previous_files_info,
            files_prefetched,
            runtime,
        } = self;
        Self {
            fs: fs.clone(),
            prefix_path: prefix_path.clone(),
            files_info: files_info.clone(),
            previous_files_info: previous_files_info.clone(),
            files_prefetched: files_prefetched.clone(),
            runtime: runtime.clone(),
        }
    }
}

impl<Fs: UniversalReadFs> CachedFs<Fs> {
    pub fn new(fs: Fs, prefix_path: &Path, runtime: tokio::runtime::Handle) -> UioResult<Self> {
        Ok(Self {
            fs,
            prefix_path: prefix_path.to_path_buf(),
            files_info: None,
            previous_files_info: None,
            files_prefetched: Arc::new(Mutex::new(HashMap::new())),
            runtime,
        })
    }

    /// The wrapped inner filesystem.
    ///
    /// Components that keep a filesystem handle for *later* opens (e.g. live
    /// reload attaching files that appear after the snapshot) must retain
    /// this raw handle, not the `CachedReadFs`: the snapshot goes stale the
    /// moment the underlying directory changes.
    pub fn inner(&self) -> &Fs {
        &self.fs
    }

    /// File info from the snapshot; `None` before [`CachedFs::cache_file_info`].
    pub fn file_info(&self, path: &Path) -> Option<&FileInfo> {
        self.files_info.as_ref()?.get(path)
    }

    /// Previous file info from the snapshot; `None` before [`CachedFs::cache_file_info`] is called twice.
    pub fn previous_file_info(&self, path: &Path) -> Option<&FileInfo> {
        self.previous_files_info.as_ref()?.get(path)
    }

    /// Files matching `prefix_path` in the snapshot; empty before
    /// [`CachedFs::cache_file_info`].
    ///
    /// A path matches when its component at the prefix's final position
    /// starts with the prefix's final component (`dir/chunk_` matches
    /// `dir/chunk_1.dat` and everything under `dir/chunk_extra/`) — the
    /// same name-based matching as the local backends, immune to mixed
    /// `/` and `\` separators on Windows.
    fn cached_list_files(&self, prefix_path: &Path) -> Vec<ListedFile> {
        let dir = prefix_path.parent().unwrap_or(Path::new(""));
        let name_prefix = prefix_path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();

        self.files_info
            .iter()
            .flatten()
            .filter(|(path, _)| {
                path.strip_prefix(dir)
                    .ok()
                    .and_then(|rel| rel.components().next())
                    .is_some_and(|first| {
                        first
                            .as_os_str()
                            .to_string_lossy()
                            .starts_with(&name_prefix)
                    })
            })
            .map(|(path, info)| ListedFile {
                path: path.clone(),
                size: info.size,
                last_modified: info.last_modified,
                etag: info.etag.clone(),
            })
            .collect()
    }
}

impl<Fs: UniversalReadFs> CachedReadFs for CachedFs<Fs> {
    /// Take a LIST snapshot of the filesystem and drop prefetched files.
    fn cache_file_info(&mut self) -> UioResult<()> {
        // List all files
        let list = self.fs.list_files(&self.prefix_path)?;

        let files_info: HashMap<_, _> = list
            .into_iter()
            .map(
                |ListedFile {
                     path,
                     size,
                     last_modified,
                     etag,
                 }| {
                    let info = FileInfo {
                        size,
                        last_modified,
                        etag,
                    };
                    (path, info)
                },
            )
            .collect();

        self.files_info = Some(files_info);
        self.files_prefetched.lock().clear();

        Ok(())
    }

    fn rotate_cache_file_info(&mut self) {
        self.previous_files_info = self.files_info.take();
        self.files_prefetched.lock().clear();
    }

    fn schedule_open(
        &self,
        path: &Path,
        open_arguments: Option<OpenOptions>,
        open_extra: Option<Fs::OpenExtra>,
    ) {
        let mut files_prefetched = self.files_prefetched.lock();

        if files_prefetched.contains_key(path) {
            return;
        }

        let open_options = open_arguments.unwrap_or(OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Global,
        });

        let mut open_extra = open_extra.unwrap_or_default();
        if let Some(info) = self.file_info(path) {
            open_extra = open_extra.with_known_len(info.size);
        }

        // Clone the fs handle so that the future can own it.
        let fs = self.fs.clone();
        let path_owned = path.to_path_buf();
        let file = async move { fs.open_async(path_owned, open_options, open_extra).await };
        files_prefetched.insert(path.to_path_buf(), ScheduledFile::Future(Box::pin(file)));
    }

    // TODO(uio): merge into `schedule_open`? might make it simpler to use
    fn reschedule_open(
        &self,
        path: &Path,
        open_arguments: Option<OpenOptions>,
        open_extra: Option<Fs::OpenExtra>,
    ) {
        // Check if their file info is complete and didn't change.
        if self
            .previous_file_info(path)
            .zip(self.file_info(path))
            .is_some_and(|(previous, current)| previous.full_eq(current))
        {
            self.files_prefetched
                .lock()
                .entry(path.to_path_buf())
                .or_insert(ScheduledFile::Unchanged);
            return;
        }

        // Otherwise schedule normally
        self.schedule_open(path, open_arguments, open_extra)
    }

    fn schedule(
        &self,
        path: PathBuf,
        fut: Pin<Box<dyn Future<Output = UioResult<Fs::File>> + Send + 'static>>,
    ) {
        self.files_prefetched
            .lock()
            .insert(path, ScheduledFile::Future(fut));
    }

    fn wait_all(&self) -> UioResult<()> {
        let mut lock = self.files_prefetched.lock();
        let futs = lock
            .extract_if(|_path, scheduled| matches!(scheduled, ScheduledFile::Future(_)))
            .map(|(path, scheduled)| {
                if let ScheduledFile::Future(fut) = scheduled {
                    async move { (path, fut.await) }
                } else {
                    unreachable!()
                }
            })
            .collect::<FuturesUnordered<_>>();

        let results = self.runtime.block_on(futs.collect::<Vec<_>>());
        for (path, result) in results {
            lock.insert(path, ScheduledFile::Ready(result));
        }
        Ok(())
    }

    fn cached_file_info(&self, path: &Path) -> Option<FileInfo> {
        self.files_info.as_ref()?.get(path).cloned()
    }
}

/// Construction context for [`CachedReadFs`]: the inner filesystem's own
/// construction context plus the prefix path under which the file listing
/// snapshot is taken. Always constructed explicitly.
pub struct CachedReadFsContext<C> {
    pub inner: C,
    pub prefix_path: PathBuf,
    pub runtime: tokio::runtime::Handle,
}

impl<Fs: UniversalReadFs> UniversalReadFileOps for CachedFs<Fs> {
    type ContextConfig = CachedReadFsContext<Fs::ContextConfig>;

    fn from_context(context: Self::ContextConfig) -> UioResult<Self> {
        let CachedReadFsContext {
            inner,
            prefix_path,
            runtime,
        } = context;
        Self::new(Fs::from_context(inner)?, &prefix_path, runtime)
    }

    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        match &self.files_info {
            Some(_) => Ok(self.cached_list_files(prefix_path)),
            None => self.fs.list_files(prefix_path),
        }
    }

    fn exists(&self, path: &Path) -> UioResult<bool> {
        match &self.files_info {
            Some(files_info) => Ok(files_info.contains_key(path)),
            None => self.fs.exists(path),
        }
    }
}

impl<Fs: UniversalReadFs> Debug for CachedFs<Fs> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self {
            fs,
            prefix_path,
            files_info,
            previous_files_info,
            files_prefetched,
            runtime,
        } = self;
        f.debug_struct("CachedReadFs")
            .field("fs", fs)
            .field("prefix_path", prefix_path)
            .field("files_info", files_info)
            .field("previous_files_info", previous_files_info)
            .field("files_prefetched", &*files_prefetched.lock())
            .field("runtime", runtime)
            .finish()
    }
}

impl<Fs: UniversalReadFs> UniversalReadFs for CachedFs<Fs> {
    /// The *wrapped* backend's file type: opening through the cache hands
    /// out the very handles the inner filesystem produced (prefetched or
    /// fallback-opened), so the wrapper never appears in stored types.
    type File = Fs::File;
    type OpenExtra = Fs::OpenExtra;

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> UioResult<Fs::File> {
        let path = path.as_ref();

        if options.writeable {
            return Err(UniversalIoError::Uninitialized {
                description:
                    "CachedReadFs only supports read-only files, writeable option is not allowed"
                        .to_string(),
            });
        }

        if let Some(file) = self.files_prefetched.lock().remove(path) {
            return match file {
                ScheduledFile::Future(future) => self.runtime.block_on(future),
                ScheduledFile::Ready(result) => result,
                ScheduledFile::Unchanged => Err(UniversalIoError::UnchangedOpen {
                    path: path.to_owned(),
                    since: self.file_info(path).and_then(|info| info.last_modified),
                }),
            };
        }

        // With a snapshot, unlisted paths fail locally — probing for
        // optional files never reaches the inner filesystem.
        if let Some(files_info) = &self.files_info
            && !files_info.contains_key(path)
        {
            return Err(UniversalIoError::NotFound {
                path: path.to_path_buf(),
            });
        }

        // The path was never scheduled for prefetch. If a snapshot was taken it
        // still carries the file's size, so thread it into the open as a known
        // length — this lets the backend skip a remote `len`/HEAD round-trip
        // (e.g. `DiskCacheFs` opens straight into `State::Ready`). Without a
        // snapshot this is a plain cache-bypass open.
        let extra = match self.file_info(path) {
            Some(info) => extra.with_known_len(info.size),
            None => extra,
        };
        self.fs.open(path, options, extra)
    }

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
