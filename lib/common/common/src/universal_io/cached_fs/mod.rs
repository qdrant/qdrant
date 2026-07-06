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

/// Read-only filesystem wrapper that snapshots the file listing at
/// construction time and serves opens from explicitly prefetched handles.
///
/// Listing and existence checks are answered from the snapshot taken in
/// [`Self::new`] without touching the inner filesystem. Opens are expected
/// to hit a handle registered via [`Self::schedule_prefetch`]; a miss falls
/// back to a direct open on the inner filesystem, but is treated as a logic
/// error (panics in debug builds, logs a warning in release builds).
///
/// Prefetched handles are take-once: [`UniversalReadFs::open`] removes the
/// handle from the pool and returns it owned, so a second open of the same
/// path goes through the fallback. The pool is shared across clones.
pub struct CachedReadFs<Fs: UniversalReadFs> {
    fs: Fs,
    prefix_path: PathBuf,
    files_info: HashMap<PathBuf, FileInfo>,
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
            files_info: HashMap::new(),
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

        self.files_info = files_info;

        Ok(())
    }

    pub fn file_info(&self, path: &Path) -> Option<&FileInfo> {
        self.files_info.get(path)
    }

    pub fn list_files(&self, prefix_path: &Path) -> Vec<ListedFile> {
        let prefix_string = prefix_path.to_string_lossy();

        self.files_info
            .iter()
            .filter(|(path, _)| path.to_string_lossy().starts_with(prefix_string.as_ref()))
            .map(|(path, info)| ListedFile {
                path: path.clone(),
                size: info.size,
            })
            .collect()
    }

    pub fn exists(&self, path: &Path) -> bool {
        self.files_info.contains_key(path)
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

        let open_options = open_arguments.unwrap_or(OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::PreferBackground,
            advice: AdviceSetting::Global,
        });

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
        Ok(self.list_files(prefix_path))
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        Ok(self.exists(path))
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
        let path = path.as_ref();

        if options.writeable {
            return Err(UniversalIoError::Uninitialized {
                description:
                    "CachedReadFs only supports read-only files, writeable option is not allowed"
                        .to_string(),
            });
        }

        if let Some(file) = self.files_prefetched.lock().remove(path) {
            return Ok(CachedFile(file));
        }

        debug_assert!(
            false,
            "CachedReadFs: file {} was not prefetched",
            path.display(),
        );
        log::warn!(
            "CachedReadFs: file {} was not prefetched, falling back to a direct open",
            path.display(),
        );

        Ok(CachedFile(self.fs.open(path, options, extra)?))
    }
}

/// File handle produced by [`CachedReadFs::open`]: the prefetched inner
/// handle taken out of the pool (or a direct fallback open).
///
/// Exists purely to satisfy the bidirectional
/// `UniversalReadFs<File = Self>` constraint on [`UniversalRead::Fs`];
/// all operations delegate to the wrapped inner file.
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
