use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::mmap::AdviceSetting;
use crate::universal_io::traits::CachedReadFs;
use crate::universal_io::{
    ListedFile, OpenExtra, OpenOptions, Populate, Result, UniversalIoError, UniversalReadFileOps,
    UniversalReadFs,
};

#[derive(Clone, Debug)]
pub struct FileInfo {
    /// Length in bytes of the entire file
    pub size: u64,
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
pub struct CachedFs<Fs: UniversalReadFs> {
    fs: Fs,
    prefix_path: PathBuf,
    /// `None` until [`CachedFs::cache_file_info`] takes the listing
    /// snapshot; the wrapper forwards to `fs` until then.
    files_info: Option<HashMap<PathBuf, FileInfo>>,
    files_prefetched: Arc<Mutex<HashMap<PathBuf, Fs::File>>>,
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

impl<Fs: UniversalReadFs> CachedFs<Fs> {
    pub fn new(fs: Fs, prefix_path: &Path) -> Result<Self> {
        Ok(Self {
            fs,
            prefix_path: prefix_path.to_path_buf(),
            files_info: None,
            files_prefetched: Arc::new(Mutex::new(HashMap::new())),
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
            })
            .collect()
    }
}

impl<Fs: UniversalReadFs> CachedReadFs for CachedFs<Fs> {
    fn cache_file_info(&mut self) -> Result<()> {
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

    fn schedule_prefetch(
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

        let mut open_extra = open_extra.unwrap_or_default();
        if let Some(info) = self.file_info(path) {
            open_extra = open_extra.with_known_len(info.size);
        }

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

impl<Fs: UniversalReadFs> UniversalReadFileOps for CachedFs<Fs> {
    type ContextConfig = CachedReadFsContext<Fs::ContextConfig>;

    fn from_context(context: Self::ContextConfig) -> Result<Self> {
        let CachedReadFsContext { inner, prefix_path } = context;
        Self::new(Fs::from_context(inner)?, &prefix_path)
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<ListedFile>> {
        match &self.files_info {
            Some(_) => Ok(self.cached_list_files(prefix_path)),
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

impl<Fs: UniversalReadFs> Debug for CachedFs<Fs> {
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
    ) -> Result<Fs::File> {
        let path = path.as_ref();

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

        // With a snapshot, unlisted paths fail locally — probing for
        // optional files never reaches the inner filesystem.
        if let Some(files_info) = &self.files_info
            && !files_info.contains_key(path)
        {
            return Err(UniversalIoError::NotFound {
                path: path.to_path_buf(),
            });
        }

        // Cache bypass: the path was never scheduled, and no snapshot was taken
        self.fs.open(path, options, extra)
    }
}
