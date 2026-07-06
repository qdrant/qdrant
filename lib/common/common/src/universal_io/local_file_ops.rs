use std::io::Write as _;
use std::path::Path;

use crate::fs::atomic_save;
use crate::mmap::create_and_ensure_length;
use crate::universal_io::{ListedFile, UniversalIoError};

pub fn local_create(path: &Path, expected_length: usize) -> crate::universal_io::Result<()> {
    create_and_ensure_length(path, expected_length)
        .map(drop)
        .map_err(|err| UniversalIoError::extract_not_found(err, path))
}

pub fn local_create_dir(path: &Path) -> crate::universal_io::Result<()> {
    fs_err::create_dir_all(path).map_err(|err| UniversalIoError::extract_not_found(err, path))
}

pub fn local_remove(path: &Path) -> crate::universal_io::Result<()> {
    fs_err::remove_file(path).map_err(|err| UniversalIoError::extract_not_found(err, path))
}

pub fn local_remove_dir(path: &Path) -> crate::universal_io::Result<()> {
    fs_err::remove_dir_all(path).map_err(|err| UniversalIoError::extract_not_found(err, path))
}

pub fn local_atomic_save(path: &Path, bytes: &[u8]) -> crate::universal_io::Result<()> {
    atomic_save(path, |writer| {
        writer.write_all(bytes).map_err(UniversalIoError::from)
    })
}

/// List files whose full path starts with `prefix_path`, recursing into
/// subdirectories — matching the flat key-prefix semantics of object-store
/// backends, so a directory prefix lists the whole tree beneath it.
pub fn local_list_files(prefix_path: &Path) -> crate::universal_io::Result<Vec<ListedFile>> {
    let start_dir = prefix_path.parent().unwrap_or(Path::new("."));
    let prefix = prefix_path.to_string_lossy().into_owned();

    let mut results = Vec::new();
    let mut dirs = vec![start_dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let entries =
            fs_err::read_dir(&dir).map_err(|err| UniversalIoError::extract_not_found(err, &dir))?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let path_string = path.to_string_lossy().into_owned();
            let file_type = entry.file_type()?;

            if file_type.is_dir() {
                // Descend when the directory can contain matching paths: it
                // matches the prefix itself, or it is an ancestor of it (the
                // walk starts at the prefix's parent, e.g. prefix `dir/file_`
                // must descend into `dir`).
                if path_string.starts_with(&prefix) || prefix.starts_with(&path_string) {
                    dirs.push(path);
                }
            } else if file_type.is_file() && path_string.starts_with(&prefix) {
                let size = entry.metadata()?.len();
                results.push(ListedFile { path, size });
            }
        }
    }

    Ok(results)
}
