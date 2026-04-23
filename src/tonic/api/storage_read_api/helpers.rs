use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::shard::ShardId;
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

    /// Verify read access to the collection, confirm the requested shard has
    /// a local replica on this peer that is currently readable, and resolve
    /// the on-disk shard directory.
    ///
    /// Returns `(shard_dir, collections_root)`. The collections root is used
    /// as the canonicalization anchor when resolving caller-supplied paths.
    ///
    /// Replicas of the same shard on different peers are not binary-compatible
    /// (segment IDs, WAL positions, optimizer state all diverge), so if the
    /// local replica has fallen behind or failed, the caller must target a
    /// different peer rather than silently read stale bytes.
    pub async fn check_and_resolve_shard(
        &self,
        auth: &storage::rbac::Auth,
        collection_name: &str,
        shard_id: ShardId,
        method: &str,
    ) -> Result<(PathBuf, PathBuf), Status> {
        let pass = auth
            .check_collection_access(collection_name, AccessRequirements::new(), method)
            .map_err(Status::from)?;

        let verification_pass = new_unchecked_verification_pass();
        let toc = self.dispatcher.toc(auth, &verification_pass);
        let collections_root = toc.storage_path().join(COLLECTIONS_DIR);

        let resolved_name = match toc.get_collection(&pass).await {
            Ok(collection) => {
                let shards_holder = collection.shards_holder();
                let holder = shards_holder.read().await;
                let Some(replica_set) = holder.get_shard(shard_id) else {
                    return Err(Status::not_found(format!(
                        "Shard {shard_id} not found in collection '{}' on this peer",
                        collection.name(),
                    )));
                };

                let local_peer_id = replica_set.this_peer_id();
                let is_readable = replica_set
                    .peer_state(local_peer_id)
                    .map(|state| state.is_readable())
                    .unwrap_or(false);

                if !is_readable {
                    return Err(Status::failed_precondition(format!(
                        "Shard {shard_id} of collection '{}' is not readable on this peer",
                        collection.name(),
                    )));
                }

                collection.name().to_string()
            }
            // Callers that bypass the TOC (e.g. tests) land here; defer
            // missing-shard detection to the on-disk path resolution below.
            Err(_) => collection_name.to_string(),
        };

        let shard_base = collections_root
            .join(&resolved_name)
            .join(shard_id.to_string());

        Ok((shard_base, collections_root))
    }

    /// Resolve a shard-scoped relative path to an absolute path, rejecting
    /// any traversal attempts.
    ///
    /// `shard_base` is the absolute path to the shard directory (as returned
    /// by [`check_and_resolve_shard`]). `collections_root` is needed to detect
    /// symlink escapes at the collection directory level. We canonicalize the
    /// collections root and verify that `canonicalize(shard_base)` matches
    /// `canonicalize(collections_root)` joined with the `<collection>/<shard>`
    /// segments; if any intermediate directory is a symlink pointing
    /// elsewhere the two diverge and the request is rejected.
    ///
    /// Examples (given `root = /data/collections`):
    ///   /data/collections/my_col/0          → canonical matches, OK
    ///   /data/collections/evil -> /tmp      → canonical is /tmp ≠ /data/collections/evil, DENIED
    ///   /data/collections/col/0/../../etc   → rejected earlier by component check
    pub fn resolve_path(
        shard_base: &Path,
        collections_root: &Path,
        relative_path: &str,
    ) -> Result<PathBuf, Status> {
        let relative_to_root = shard_base
            .strip_prefix(collections_root)
            .map_err(|_| Status::internal("Shard base path is not under collections root"))?;
        let canonical_collections_root = fs_err::canonicalize(collections_root).map_err(|e| {
            Status::internal(format!("Failed to canonicalize collections root: {e}"))
        })?;
        let expected_canonical_base = canonical_collections_root.join(relative_to_root);
        let canonical_base = match fs_err::canonicalize(shard_base) {
            Ok(canonical) => {
                if canonical != expected_canonical_base {
                    return Err(Status::permission_denied(format!(
                        "Shard path '{}' resolves outside its directory",
                        relative_to_root.display(),
                    )));
                }
                Some(canonical)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(Status::internal(format!(
                    "Failed to canonicalize shard base: {e}"
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

        let full = shard_base.join(rel);

        // Try to canonicalize and verify the path is under the shard base.
        // If the file doesn't exist yet (e.g. for an exists check), the component
        // check above is sufficient since we rejected all non-Normal components.
        match fs_err::canonicalize(&full) {
            Ok(canonical) => {
                if let Some(canonical_base) = &canonical_base
                    && !canonical.starts_with(canonical_base)
                {
                    return Err(Status::permission_denied(format!(
                        "Path '{}' is outside the shard directory",
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
                        "Path '{}' is outside the shard directory",
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
        UniversalIoError::Uninitialized { description } => {
            Status::internal(format!("Uninitialized: {description}"))
        }
        UniversalIoError::BytemuckCast(e) => Status::internal(format!("Bytemuck cast error: {e}")),
        UniversalIoError::ZerocopySize(e) => Status::internal(e),
    }
}
