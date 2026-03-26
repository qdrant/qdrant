use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use collection::operations::verification::new_unchecked_verification_pass;
use common::universal_io::{ReadRange, UniversalIoError, UniversalRead};
use storage::content_manager::toc::COLLECTIONS_DIR;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use tonic::Status;

use crate::tonic::api::storage_read_api::StorageReadService;

impl<S: UniversalRead<u8> + Send + Sync + 'static> StorageReadService<S> {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            dispatcher,
            _marker: PhantomData,
        }
    }

    /// Verify at least read access to the collection and resolve a potential
    /// alias to the real collection name.
    ///
    /// Returns the resolved collection name and its base directory path.
    pub async fn check_and_resolve_collection(
        &self,
        auth: &storage::rbac::Auth,
        collection_name: &str,
        method: &str,
    ) -> Result<(String, PathBuf), Status> {
        let pass = auth
            .check_collection_access(collection_name, AccessRequirements::new(), method)
            .map_err(Status::from)?;

        let verification_pass = new_unchecked_verification_pass();
        let toc = self.dispatcher.toc(auth, &verification_pass);
        let resolved_name = match toc.get_collection(&pass).await {
            Ok(collection) => collection.name().to_string(),
            Err(_) => collection_name.to_string(),
        };

        let base = toc
            .storage_path()
            .join(COLLECTIONS_DIR)
            .join(&resolved_name);
        Ok((resolved_name, base))
    }

    /// Resolve a collection-scoped relative path to an absolute path,
    /// rejecting any traversal attempts.
    ///
    /// `collection_base` is the absolute path to the collection directory
    /// (as returned by [`check_and_resolve_collection`]).
    ///
    /// The `collections_root` (derived via `collection_base.parent()`) is needed
    /// to detect symlink escapes at the collection directory level. We
    /// canonicalize both the root and the collection dir independently, then
    /// verify that `canonicalize(collection_base) == canonicalize(root) / name`.
    /// If the collection dir is a symlink pointing elsewhere the two will
    /// diverge and the request is rejected.
    ///
    /// Examples (given `root = /data/collections`):
    ///   /data/collections/my_col          → canonical matches, OK
    ///   /data/collections/evil -> /tmp     → canonical is /tmp ≠ /data/collections/evil, DENIED
    ///   /data/collections/col/../../etc    → rejected earlier by component check
    pub fn resolve_path(
        &self,
        collection_base: &Path,
        relative_path: &str,
    ) -> Result<PathBuf, Status> {
        let collection_name = collection_base
            .file_name()
            .expect("collection base path always has a file name");
        let collections_root = collection_base
            .parent()
            .expect("collection base path always has a parent");
        let canonical_collections_root = fs_err::canonicalize(collections_root).map_err(|e| {
            Status::internal(format!("Failed to canonicalize collections root: {e}"))
        })?;
        let expected_canonical_base = canonical_collections_root.join(collection_name);
        let canonical_base = match fs_err::canonicalize(collection_base) {
            Ok(canonical) => {
                if canonical != expected_canonical_base {
                    return Err(Status::permission_denied(format!(
                        "Collection '{}' resolves outside its directory",
                        collection_name.to_string_lossy(),
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

        let full = collection_base.join(rel);

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
pub fn validate_range(range: ReadRange, data_length: u64) -> common::universal_io::Result<()> {
    let end = range
        .byte_offset
        .checked_add(range.length)
        .ok_or(UniversalIoError::OutOfBounds {
            start: range.byte_offset,
            end: u64::MAX,
            elements: usize::try_from(data_length).unwrap_or(usize::MAX),
        })?;

    if end > data_length {
        return Err(UniversalIoError::OutOfBounds {
            start: range.byte_offset,
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
