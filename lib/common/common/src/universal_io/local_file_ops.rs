use std::io::{self, Write as _};
use std::path::Path;

use crate::fs::atomic_save;
use crate::mmap::create_and_ensure_length;
use crate::universal_io::{ListedFile, UioResult, UniversalIoError};

/// `writev(2)`-family syscalls accept at most this many iovecs per call
/// (`IOV_MAX`, 1024 on Linux).
const IOV_MAX: usize = 1024;

/// Collect `items` into non-empty [`io::IoSlice`]s for a vectored append,
/// alongside their total byte length. Empty slices are dropped — they would
/// trip [`write_all_vectored`]'s `WriteZero` check.
pub(super) fn collect_append_slices<'a, T: bytemuck::Pod>(
    items: impl IntoIterator<Item = &'a [T]>,
) -> (Vec<io::IoSlice<'a>>, usize) {
    let slices: Vec<io::IoSlice<'_>> = items
        .into_iter()
        .map(|item| bytemuck::cast_slice(item))
        .filter(|bytes: &&[u8]| !bytes.is_empty())
        .map(io::IoSlice::new)
        .collect();
    let total = slices.iter().map(|slice| slice.len()).sum();

    (slices, total)
}

/// Write all `slices` with vectored writes, handling short writes and the
/// [`IOV_MAX`] limit.
///
/// On an `O_APPEND` fd every underlying `write`/`writev` syscall is an atomic
/// grow+write at the current end-of-file, and under a single writer
/// consecutive syscalls stay contiguous — so splitting is safe there.
///
/// `slices` must not contain empty slices: an all-empty head would report a
/// spurious `WriteZero` error.
pub(super) fn write_all_vectored(
    mut writer: impl io::Write,
    mut slices: &mut [io::IoSlice<'_>],
) -> io::Result<()> {
    while !slices.is_empty() {
        let chunk = slices.len().min(IOV_MAX);
        let written = match writer.write_vectored(&slices[..chunk]) {
            Ok(written) => written,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        };
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        io::IoSlice::advance_slices(&mut slices, written);
    }

    Ok(())
}

pub fn local_create(path: &Path, expected_length: usize) -> UioResult<()> {
    create_and_ensure_length(path, expected_length)
        .map(drop)
        .map_err(|err| UniversalIoError::extract_not_found(err, path))
}

pub fn local_create_dir(path: &Path) -> UioResult<()> {
    fs_err::create_dir_all(path).map_err(|err| UniversalIoError::extract_not_found(err, path))
}

pub fn local_remove(path: &Path) -> UioResult<()> {
    fs_err::remove_file(path).map_err(|err| UniversalIoError::extract_not_found(err, path))
}

pub fn local_remove_dir(path: &Path) -> UioResult<()> {
    fs_err::remove_dir_all(path).map_err(|err| UniversalIoError::extract_not_found(err, path))
}

pub fn local_atomic_save(path: &Path, bytes: &[u8]) -> UioResult<()> {
    atomic_save(path, |writer| {
        writer.write_all(bytes).map_err(UniversalIoError::from)
    })
}

/// List files matching `prefix_path`, recursing into subdirectories —
/// mirroring the flat key-prefix semantics of object-store backends, so a
/// directory prefix lists the whole tree beneath it.
///
/// An entry matches when its name at the prefix's final position starts with
/// the prefix's final component: `dir/chunk_` matches `dir/chunk_1.dat` and
/// everything under `dir/chunk_extra/`. Matching is name-based, never a
/// whole-path string comparison — a joined prefix may mix `/` and `\` on
/// Windows, where entry paths use `\` throughout.
pub fn local_list_files(prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
    let dir = prefix_path.parent().unwrap_or(Path::new("."));
    let name_prefix = prefix_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();

    let mut results = Vec::new();
    // Directories whose name matched the prefix: the whole tree beneath each
    // of them matches.
    let mut matched_dirs = Vec::new();

    let entries =
        fs_err::read_dir(dir).map_err(|err| UniversalIoError::extract_not_found(err, dir))?;
    for entry in entries {
        let entry = entry?;
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(&name_prefix)
        {
            continue;
        }
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            matched_dirs.push(entry.path());
        } else if file_type.is_file() {
            let metadata = entry.metadata()?;
            results.push(ListedFile {
                path: entry.path(),
                size: metadata.len(),
                last_modified: metadata.modified().ok(),
            });
        }
    }

    while let Some(dir) = matched_dirs.pop() {
        let entries =
            fs_err::read_dir(&dir).map_err(|err| UniversalIoError::extract_not_found(err, &dir))?;
        for entry in entries {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                matched_dirs.push(entry.path());
            } else if file_type.is_file() {
                let metadata = entry.metadata()?;
                results.push(ListedFile {
                    path: entry.path(),
                    size: metadata.len(),
                    last_modified: metadata.modified().ok(),
                });
            }
        }
    }

    Ok(results)
}
