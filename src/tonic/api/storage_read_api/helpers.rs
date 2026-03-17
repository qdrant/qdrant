use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use collection::operations::verification::new_unchecked_verification_pass;
use common::universal_io::{ElementsRange, FileIndex, UniversalIoError, UniversalRead};
use storage::content_manager::toc::COLLECTIONS_DIR;
use storage::dispatcher::Dispatcher;
use tonic::Status;

use crate::tonic::api::storage_read_api::StorageReadService;

impl<S: UniversalRead<u8> + Send + Sync + 'static> StorageReadService<S> {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            dispatcher,
            _marker: PhantomData,
        }
    }

    /// Return the root directory that contains all collections.
    fn collections_base_path(&self, auth: &storage::rbac::Auth) -> PathBuf {
        let pass = new_unchecked_verification_pass();
        let toc = self.dispatcher.toc(auth, &pass);
        toc.storage_path().join(COLLECTIONS_DIR)
    }

    /// Return the base directory for a collection.
    pub fn collection_base_path(
        &self,
        auth: &storage::rbac::Auth,
        collection_name: &str,
    ) -> PathBuf {
        self.collections_base_path(auth).join(collection_name)
    }

    /// Resolve a collection-scoped relative path to an absolute path,
    /// rejecting any traversal attempts.
    pub fn resolve_path(
        &self,
        auth: &storage::rbac::Auth,
        collection_name: &str,
        relative_path: &str,
    ) -> Result<PathBuf, Status> {
        let collections_root = self.collections_base_path(auth);
        let base = self.collection_base_path(auth, collection_name);
        let canonical_collections_root = fs_err::canonicalize(&collections_root).map_err(|e| {
            Status::internal(format!("Failed to canonicalize collections root: {e}"))
        })?;
        let expected_canonical_base = canonical_collections_root.join(collection_name);
        let canonical_base = match fs_err::canonicalize(&base) {
            Ok(canonical) => {
                if canonical != expected_canonical_base {
                    return Err(Status::permission_denied(format!(
                        "Collection '{collection_name}' resolves outside its directory",
                    )));
                }
                Some(canonical)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(Status::internal(format!(
                    "Failed to canonicalize collection base: {e}"
                )));
            }
        };

        let rel = Path::new(relative_path);
        for c in rel.components() {
            match c {
                Component::Normal(_) => {}
                _ => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid path component in '{relative_path}'"
                    )));
                }
            }
        }

        let full = base.join(rel);

        // Try to canonicalize and verify the path is under the collection base.
        // If the file doesn't exist yet (e.g. for an exists check), the component
        // check above is sufficient since we rejected all non-Normal components.
        match fs_err::canonicalize(&full) {
            Ok(canonical) => {
                if let Some(canonical_base) = &canonical_base
                    && !canonical.starts_with(canonical_base)
                {
                    return Err(Status::permission_denied(format!(
                        "Path '{}' is outside the collection directory",
                        full.display()
                    )));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(canonical_ancestor) = canonicalize_existing_ancestor(&full)
                    .map_err(|e| Status::internal(format!("Failed to canonicalize path: {e}")))?
                    && let Some(canonical_base) = &canonical_base
                    && !canonical_ancestor.starts_with(canonical_base)
                {
                    return Err(Status::permission_denied(format!(
                        "Path '{}' is outside the collection directory",
                        full.display()
                    )));
                }
            }
            Err(e) => {
                return Err(Status::internal(format!(
                    "Failed to canonicalize path: {e}"
                )));
            }
        }

        Ok(full)
    }
}

fn canonicalize_existing_ancestor(path: &Path) -> std::io::Result<Option<PathBuf>> {
    let mut current = Some(path);
    while let Some(candidate) = current {
        match fs_err::canonicalize(candidate) {
            Ok(canonical) => return Ok(Some(canonical)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                current = candidate.parent();
            }
            Err(e) => return Err(e),
        }
    }

    Ok(None)
}

/// Validate a requested range against the file size using the same
/// out-of-bounds semantics as `UniversalRead::read()`.
pub fn validate_range(range: ElementsRange, data_length: u64) -> common::universal_io::Result<()> {
    let end = range
        .start
        .checked_add(range.length)
        .ok_or(UniversalIoError::OutOfBounds {
            start: range.start,
            end: u64::MAX,
            elements: usize::try_from(data_length).unwrap_or(usize::MAX),
        })?;

    if end > data_length {
        return Err(UniversalIoError::OutOfBounds {
            start: range.start,
            end,
            elements: usize::try_from(data_length).unwrap_or(usize::MAX),
        });
    }

    Ok(())
}

/// Convert UniversalIoError to tonic Status.
pub fn io_error_to_status(e: UniversalIoError) -> Status {
    match e {
        UniversalIoError::Io(e) => match e.kind() {
            std::io::ErrorKind::NotFound => Status::not_found(format!("File not found: {e}")),
            std::io::ErrorKind::PermissionDenied => {
                Status::permission_denied(format!("Permission denied: {e}"))
            }
            _ => Status::internal(format!("I/O error: {e}")),
        },
        UniversalIoError::Mmap(e) => Status::internal(format!("Mmap error: {e}")),
        UniversalIoError::OutOfBounds {
            start,
            end,
            elements,
        } => Status::out_of_range(format!(
            "Range {start}..{end} out of bounds (size: {elements})"
        )),
        UniversalIoError::NotFound { path } => {
            Status::not_found(format!("File not found: {}", path.display()))
        }
        UniversalIoError::InvalidFileIndex { file_index, files } => Status::internal(format!(
            "Invalid file index: {file_index} (num_files: {files})"
        )),
        UniversalIoError::IoUringNotSupported(e) => {
            Status::internal(format!("IoUring not supported: {e}"))
        }
    }
}

/// Dispatch a read call on `MmapU8` based on a runtime `sequential` flag.
/// Needed because `UniversalRead::read` uses a const generic parameter.
pub fn dispatch_read<S: UniversalRead<u8>>(
    storage: &S,
    range: ElementsRange,
    sequential: bool,
) -> common::universal_io::Result<std::borrow::Cow<'_, [u8]>> {
    if sequential {
        storage.read::<true>(range)
    } else {
        storage.read::<false>(range)
    }
}

/// Dispatch a read_batch call based on a runtime `sequential` flag.
pub fn dispatch_read_batch<S: UniversalRead<u8>>(
    storage: &S,
    ranges: impl IntoIterator<Item = ElementsRange>,
    sequential: bool,
    callback: impl FnMut(usize, &[u8]) -> common::universal_io::Result<()>,
) -> common::universal_io::Result<()> {
    if sequential {
        storage.read_batch::<true>(ranges, callback)
    } else {
        storage.read_batch::<false>(ranges, callback)
    }
}

/// Dispatch a read_multi call based on a runtime `sequential` flag.
pub fn dispatch_read_multi<S: UniversalRead<u8>>(
    files: &[S],
    reads: impl IntoIterator<Item = (FileIndex, ElementsRange)>,
    sequential: bool,
    callback: impl FnMut(usize, FileIndex, &[u8]) -> common::universal_io::Result<()>,
) -> common::universal_io::Result<()> {
    if sequential {
        S::read_multi::<true>(files, reads, callback)
    } else {
        S::read_multi::<false>(files, reads, callback)
    }
}
