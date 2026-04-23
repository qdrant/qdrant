//! Shared I/O primitives for the public `StorageRead` (collection-scoped) and
//! the internal `ShardStorageRead` (shard-scoped) services.
//!
//! Functions here are scope-agnostic: the caller picks a `base` directory
//! (either a collection root or a shard directory) and a `safe_root` anchor
//! that `base` must canonicalize under. Path resolution rejects traversal,
//! symlink escapes at the base level, and symlinks inside `base` that resolve
//! outside it.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use common::generic_consts::Random;
use common::universal_io::{FileIndex, OpenOptions, ReadRange, UniversalIoError, UniversalRead};
use futures::Stream;
use tonic::Status;

/// Chunk size for streaming reads (~1 MB).
pub const STREAM_CHUNK_SIZE: u64 = 1024 * 1024;

/// Resolve a `relative` path inside `base`, rejecting any traversal or
/// symlink escapes.
///
/// `safe_root` must be an ancestor of `base` whose canonical form is trusted
/// (e.g. the storage-wide collections directory). `base` itself is allowed to
/// be missing on disk (for tests or pre-creation requests), but if it exists
/// its canonical form must equal `canonical(safe_root)` joined with the
/// path segments between `safe_root` and `base`. That catches the case where
/// any intermediate directory is a symlink pointing elsewhere.
pub fn resolve_path(base: &Path, safe_root: &Path, relative: &str) -> Result<PathBuf, Status> {
    let relative_to_safe = base
        .strip_prefix(safe_root)
        .map_err(|_| Status::internal("Base path is not under safe root"))?;
    let canonical_safe_root = fs_err::canonicalize(safe_root)
        .map_err(|e| Status::internal(format!("Failed to canonicalize safe root: {e}")))?;
    let expected_canonical_base = canonical_safe_root.join(relative_to_safe);
    let canonical_base = match fs_err::canonicalize(base) {
        Ok(canonical) => {
            if canonical != expected_canonical_base {
                return Err(Status::permission_denied(format!(
                    "Path '{}' resolves outside its directory",
                    relative_to_safe.display(),
                )));
            }
            Some(canonical)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            return Err(Status::internal(format!(
                "Failed to canonicalize base: {e}"
            )));
        }
    };

    let rel = Path::new(relative);
    for c in rel.components() {
        match c {
            Component::Normal(_) => {}
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Invalid path component in '{relative}'"
                )));
            }
        }
    }

    let full = base.join(rel);

    match fs_err::canonicalize(&full) {
        Ok(canonical) => {
            if let Some(canonical_base) = &canonical_base
                && !canonical.starts_with(canonical_base)
            {
                return Err(Status::permission_denied(format!(
                    "Path '{}' is outside the base directory",
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
                    "Path '{}' is outside the base directory",
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

/// `UniversalRead::exists()` with NotFound coerced to `false`.
pub async fn file_exists<S>(path: PathBuf) -> Result<bool, Status>
where
    S: UniversalRead<u8> + Send + Sync + 'static,
{
    tokio::task::spawn_blocking(move || match S::exists(&path) {
        Ok(exists) => Ok(exists),
        Err(UniversalIoError::NotFound { .. }) => Ok(false),
        Err(UniversalIoError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(io_error_to_status(e)),
    })
    .await
    .map_err(|e| Status::internal(format!("Task join error: {e}")))?
}

/// List files whose full canonical path begins with `prefix_path`. Returns
/// paths relative to `base`, always joined with forward slashes.
pub async fn list_files<S>(base: PathBuf, prefix_path: PathBuf) -> Result<Vec<String>, Status>
where
    S: UniversalRead<u8> + Send + Sync + 'static,
{
    let paths = tokio::task::spawn_blocking(move || S::list_files(&prefix_path))
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))?
        .map_err(io_error_to_status)?;

    let relative_paths = paths
        .into_iter()
        .filter_map(|p| {
            p.strip_prefix(&base).ok().map(|rel| {
                let components = rel
                    .components()
                    .filter_map(|c| c.as_os_str().to_str())
                    .collect::<Vec<_>>();
                components.join("/")
            })
        })
        .collect::<Vec<_>>();

    Ok(relative_paths)
}

pub async fn file_length<S>(path: PathBuf) -> Result<u64, Status>
where
    S: UniversalRead<u8> + Send + Sync + 'static,
{
    let open_options = OpenOptions::default();
    tokio::task::spawn_blocking(move || {
        let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
        storage.len().map_err(io_error_to_status)
    })
    .await
    .map_err(|e| Status::internal(format!("Task join error: {e}")))?
}

pub async fn read_bytes<S>(path: PathBuf, range: ReadRange) -> Result<Vec<u8>, Status>
where
    S: UniversalRead<u8> + Send + Sync + 'static,
{
    let open_options = OpenOptions::default();
    tokio::task::spawn_blocking(move || {
        let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
        let cow = storage.read::<Random>(range).map_err(io_error_to_status)?;
        Ok::<_, Status>(cow.into_owned())
    })
    .await
    .map_err(|e| Status::internal(format!("Task join error: {e}")))?
}

pub async fn read_whole<S>(path: PathBuf) -> Result<Vec<u8>, Status>
where
    S: UniversalRead<u8> + Send + Sync + 'static,
{
    let open_options = OpenOptions::default();
    tokio::task::spawn_blocking(move || {
        let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
        let cow = storage.read_whole().map_err(io_error_to_status)?;
        Ok::<_, Status>(cow.into_owned())
    })
    .await
    .map_err(|e| Status::internal(format!("read_whole error: {e}")))?
}

pub async fn read_batch<S>(path: PathBuf, ranges: Vec<ReadRange>) -> Result<Vec<Vec<u8>>, Status>
where
    S: UniversalRead<u8> + Send + Sync + 'static,
{
    let open_options = OpenOptions::default();
    tokio::task::spawn_blocking(move || {
        let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
        let mut results = ranges.iter().map(|_| Vec::new()).collect::<Vec<_>>();
        storage
            .read_batch::<Random, _>(ranges.into_iter().enumerate(), |idx, chunk| {
                results[idx].extend_from_slice(chunk);
                Ok(())
            })
            .map_err(io_error_to_status)?;

        Ok::<_, Status>(results)
    })
    .await
    .map_err(|e| Status::internal(format!("Task join error: {e}")))?
}

/// Read ranges across multiple files. Each input is already resolved to an
/// absolute path + range; paths are deduplicated internally.
pub async fn read_multi<S>(
    resolved_reads: Vec<(PathBuf, ReadRange)>,
) -> Result<Vec<Vec<u8>>, Status>
where
    S: UniversalRead<u8> + Send + Sync + 'static,
{
    let open_options = OpenOptions::default();

    let mut path_to_index = HashMap::<PathBuf, FileIndex>::new();
    let mut unique_paths = Vec::<PathBuf>::new();
    let mut reads_ = Vec::<(FileIndex, ReadRange)>::with_capacity(resolved_reads.len());

    for (resolved, range) in resolved_reads {
        let file_index = *path_to_index.entry(resolved.clone()).or_insert_with(|| {
            let idx = unique_paths.len();
            unique_paths.push(resolved);
            idx
        });
        reads_.push((file_index, range));
    }

    tokio::task::spawn_blocking(move || {
        let files = unique_paths
            .iter()
            .map(|p| S::open(p, open_options))
            .collect::<common::universal_io::Result<Vec<_>>>()
            .map_err(io_error_to_status)?;

        let mut results = vec![Vec::new(); reads_.len()];

        let reads = reads_
            .into_iter()
            .enumerate()
            .map(|(op_idx, (file_idx, range))| (op_idx, &files[file_idx], range));

        S::read_multi::<Random, _>(reads, |op_idx, chunk| {
            results[op_idx].extend_from_slice(chunk);
            Ok(())
        })
        .map_err(io_error_to_status)?;

        Ok::<Vec<Vec<u8>>, Status>(results)
    })
    .await
    .map_err(|e| Status::internal(format!("Task join error: {e}")))?
}

/// Streaming variant of `read_bytes`. Opens the file once, validates the
/// range against the file length, then yields `STREAM_CHUNK_SIZE` chunks.
/// Only individual chunk reads run on the blocking pool.
pub async fn read_bytes_stream<S>(
    path: PathBuf,
    range: ReadRange,
) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send>>, Status>
where
    S: UniversalRead<u8> + Send + Sync + 'static,
{
    let open_options = OpenOptions::default();
    let (storage, range) = tokio::task::spawn_blocking(move || {
        let s = S::open(&path, open_options).map_err(io_error_to_status)?;
        let file_len = s.len().map_err(io_error_to_status)?;
        validate_range(range, file_len).map_err(io_error_to_status)?;
        Ok::<_, Status>((s, range))
    })
    .await
    .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

    let storage = Arc::new(storage);

    let stream = futures::stream::try_unfold(
        (storage, range.byte_offset, range.length),
        move |(storage, current_offset, remaining)| async move {
            if remaining == 0 {
                return Ok(None);
            }

            let chunk_size = remaining.min(STREAM_CHUNK_SIZE);
            let storage_for_read = Arc::clone(&storage);

            let data = tokio::task::spawn_blocking(move || {
                storage_for_read
                    .read::<Random>(ReadRange {
                        byte_offset: current_offset,
                        length: chunk_size,
                    })
                    .map(|cow| cow.into_owned())
                    .map_err(io_error_to_status)
            })
            .await
            .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

            Ok(Some((
                data,
                (storage, current_offset + chunk_size, remaining - chunk_size),
            )))
        },
    );

    Ok(Box::pin(stream))
}
