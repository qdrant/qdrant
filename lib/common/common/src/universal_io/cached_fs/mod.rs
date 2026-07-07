use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytemuck::TransparentWrapper;
use parking_lot::Mutex;

use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::mmap::AdviceSetting;
use crate::universal_io::wrappers::WrappedReadPipeline;
use crate::universal_io::{
    Item, ListedFile, OpenOptions, Populate, ReadBytesItem, ReadRange, Result, UniversalIoError,
    UniversalKind, UniversalRead, UniversalReadFileOps, UniversalReadFs, UserData,
};

#[derive(Clone, Debug)]
pub struct FileInfo {
    /// Length in bytes of the entire file
    pub size: u64,
}

/// Read-only filesystem wrapper that snapshots the file listing and serves
/// opens from explicitly prefetched handles.
///
/// Until [`Self::cache_file_info`] takes the listing snapshot, the wrapper is
/// a passthrough: listing, existence checks and opens forward to the inner
/// filesystem unchanged (prefetched handles are still consumed first).
///
/// Once the snapshot is taken, listing and existence checks are answered
/// from it without touching the inner filesystem, and opens of paths absent
/// from the snapshot fail with `NotFound` locally — probing for optional
/// files is free. Opens are expected to hit a handle registered via
/// [`Self::schedule_prefetch`]; a miss on a path the snapshot does contain
/// is currently `todo!` (unreachable: the open path prefetches every listed
/// file, and each file is opened at most once).
///
/// Prefetched handles are take-once: [`UniversalReadFs::open`] removes the
/// handle from the pool and returns it owned. The pool is shared across
/// clones.
pub struct CachedReadFs<Fs: UniversalReadFs> {
    fs: Fs,
    prefix_path: PathBuf,
    /// `None` until [`Self::cache_file_info`] takes the listing snapshot;
    /// the wrapper forwards to `fs` until then.
    files_info: Option<HashMap<PathBuf, FileInfo>>,
    files_prefetched: Arc<Mutex<HashMap<PathBuf, Fs::File>>>,
}

/// Manual impl: `derive(Clone)` would add a spurious `Fs::File: Clone`
/// bound for the projection in `files_prefetched`, even though the
/// `Arc` field is unconditionally cloneable (rust-lang/rust#26925).
impl<Fs: UniversalReadFs> Clone for CachedReadFs<Fs> {
    fn clone(&self) -> Self {
        let Self {
            fs,
            prefix_path,
            files_info,
            files_prefetched,
        } = self;
        Self {
            fs: fs.clone(),
            prefix_path: prefix_path.clone(),
            files_info: files_info.clone(),
            files_prefetched: files_prefetched.clone(),
        }
    }
}

impl<Fs: UniversalReadFs> CachedReadFs<Fs> {
    pub fn new(fs: Fs, prefix_path: &Path) -> Result<Self> {
        Ok(Self {
            fs,
            prefix_path: prefix_path.to_path_buf(),
            files_info: None,
            files_prefetched: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn cache_file_info(&mut self) -> Result<()> {
        // List all files
        let list = self.fs.list_files(&self.prefix_path)?;

        let files_info: HashMap<_, _> = list
            .into_iter()
            .map(|ListedFile { path, size }| {
                let info = FileInfo { size };
                (path, info)
            })
            .collect();

        self.files_info = Some(files_info);

        Ok(())
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

    /// File info from the snapshot; `None` before [`Self::cache_file_info`].
    pub fn file_info(&self, path: &Path) -> Option<&FileInfo> {
        self.files_info.as_ref()?.get(path)
    }

    /// Files from the snapshot; empty before [`Self::cache_file_info`].
    ///
    /// A path matches when its component at the prefix's final position
    /// starts with the prefix's final component (`dir/chunk_` matches
    /// `dir/chunk_1.dat` and everything under `dir/chunk_extra/`) — the
    /// same name-based matching as the local backends, immune to mixed
    /// `/` and `\` separators on Windows.
    pub fn list_files(&self, prefix_path: &Path) -> Vec<ListedFile> {
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
            })
            .collect()
    }

    /// Existence per the snapshot; `false` before [`Self::cache_file_info`].
    pub fn exists(&self, path: &Path) -> bool {
        self.files_info
            .as_ref()
            .is_some_and(|files_info| files_info.contains_key(path))
    }

    /// Take a raw inner file handle, bypassing the [`UniversalReadFs`] trait.
    ///
    /// Cache-aware callers that *store* handles long-term use this so their
    /// stored type stays `Fs::File` — the [`CachedFile`] wrapper the trait
    /// impl returns is only meant for transient open-read-discard access
    /// (e.g. [`read_json_via`](crate::universal_io::read_json_via)).
    ///
    /// Semantics match the trait open: prefetched handles are taken from the
    /// pool first; without a snapshot the call passes through to the inner
    /// filesystem; with one, unlisted paths fail `NotFound` locally and a
    /// listed-but-not-prefetched path is currently `todo!` (unreachable; see
    /// the type-level docs).
    pub fn take_file(
        &self,
        path: &Path,
        options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> Result<Fs::File> {
        if options.writeable {
            return Err(UniversalIoError::Uninitialized {
                description:
                    "CachedReadFs only supports read-only files, writeable option is not allowed"
                        .to_string(),
            });
        }

        if let Some(file) = self.files_prefetched.lock().remove(path) {
            return Ok(file);
        }

        let Some(files_info) = &self.files_info else {
            debug_assert!(
                false,
                "CachedReadFs::take_file called before cache_file_info"
            );
            return self.fs.open(path, options, extra);
        };

        match files_info.get(path) {
            None => Err(UniversalIoError::NotFound {
                path: path.to_path_buf(),
            }),
            Some(_file_info) => {
                // Fallback to cache bypass.
                // If we are here, that means open path is not optimized enough.
                // After read-only read path is refactored, this should be protected
                // by debug assertion
                self.fs.open(path, options, extra)
            }
        }
    }

    pub fn schedule_prefetch(
        &self,
        path: &Path,
        open_arguments: Option<OpenOptions>,
        open_extra: Option<Fs::OpenExtra>,
    ) -> Result<()> {
        let mut files_prefetched = self.files_prefetched.lock();

        if files_prefetched.contains_key(path) {
            return Ok(());
        }

        let mut open_options = open_arguments.unwrap_or(OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Global,
        });

        open_options.populate = Populate::PreferBackground;

        let open_extra = open_extra.unwrap_or_default();

        let file = self.fs.open(path, open_options, open_extra)?;
        files_prefetched.insert(path.to_path_buf(), file);

        Ok(())
    }
}

/// Construction context for [`CachedReadFs`]: the inner filesystem's own
/// construction context plus the prefix path under which the file listing
/// snapshot is taken. Always constructed explicitly.
pub struct CachedReadFsContext<C> {
    pub inner: C,
    pub prefix_path: PathBuf,
}

impl<Fs: UniversalReadFs> UniversalReadFileOps for CachedReadFs<Fs> {
    type ContextConfig = CachedReadFsContext<Fs::ContextConfig>;

    fn from_context(context: Self::ContextConfig) -> Result<Self> {
        let CachedReadFsContext { inner, prefix_path } = context;
        Self::new(Fs::from_context(inner)?, &prefix_path)
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<ListedFile>> {
        match &self.files_info {
            Some(_) => Ok(self.list_files(prefix_path)),
            None => self.fs.list_files(prefix_path),
        }
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        match &self.files_info {
            Some(files_info) => Ok(files_info.contains_key(path)),
            None => self.fs.exists(path),
        }
    }
}

impl<Fs: UniversalReadFs> Debug for CachedReadFs<Fs> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self {
            fs,
            prefix_path,
            files_info,
            files_prefetched,
        } = self;
        f.debug_struct("CachedReadFs")
            .field("fs", fs)
            .field("prefix_path", prefix_path)
            .field("files_info", files_info)
            .field("files_prefetched", &*files_prefetched.lock())
            .finish()
    }
}

impl<Fs: UniversalReadFs> UniversalReadFs for CachedReadFs<Fs> {
    type File = CachedFile<Fs::File>;
    type OpenExtra = Fs::OpenExtra;

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> Result<Self::File> {
        Ok(CachedFile(self.take_file(path.as_ref(), options, extra)?))
    }
}

/// File handle produced by the [`UniversalReadFs`] impl of
/// [`CachedReadFs`]: the prefetched inner handle taken out of the pool (or
/// a direct fallback open).
///
/// Exists purely to satisfy the bidirectional
/// `UniversalReadFs<File = Self>` constraint on [`UniversalRead::Fs`]; all
/// operations delegate to the wrapped inner file. Meant for transient
/// open-read-discard helpers (e.g.
/// [`read_json_via`](crate::universal_io::read_json_via)) that never leak
/// the handle; code that stores handles long-term takes the raw inner file
/// via [`CachedReadFs::take_file`] instead, so `CachedFile` never spreads
/// into stored types.
#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct CachedFile<S>(S);

impl<S: UniversalRead> UniversalRead for CachedFile<S> {
    type Fs = CachedReadFs<S::Fs>;

    type ReadPipeline<'file, U>
        = WrappedReadPipeline<Self, S::ReadPipeline<'file, U>>
    where
        Self: 'file,
        U: UserData;

    #[inline]
    fn reopen(&mut self) -> Result<()> {
        self.0.reopen()
    }

    #[inline]
    fn read<P: AccessPattern, T: Item>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        self.0.read::<P, T>(range)
    }

    #[inline]
    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>> {
        self.0.read_bytes::<P>(range, align)
    }

    #[inline]
    fn read_whole<T: Item>(&self) -> Result<Cow<'_, [T]>> {
        self.0.read_whole()
    }

    #[inline]
    fn read_batch<P, T, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        callback: impl FnMut(U, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
    {
        self.0.read_batch::<P, T, U>(ranges, callback)
    }

    #[inline]
    fn read_iter<P, T, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'_, [T]>)>>>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
    {
        self.0.read_iter::<P, T, U>(ranges)
    }

    #[inline]
    fn read_bytes_iter<P, U>(
        &self,
        ranges: impl IntoIterator<Item = ReadBytesItem<U>>,
    ) -> Result<impl Iterator<Item = Result<(U, ACow<'_>)>>>
    where
        P: AccessPattern,
        U: UserData,
    {
        self.0.read_bytes_iter::<P, U>(ranges)
    }

    #[inline]
    fn len<T>(&self) -> Result<u64> {
        self.0.len::<T>()
    }

    #[inline]
    fn populate(&self) -> Result<()> {
        self.0.populate()
    }

    #[inline]
    fn populate_auto() -> bool {
        S::populate_auto()
    }

    #[inline]
    fn clear_ram_cache(&self) -> Result<()> {
        self.0.clear_ram_cache()
    }

    fn kind() -> UniversalKind {
        S::kind()
    }
}
