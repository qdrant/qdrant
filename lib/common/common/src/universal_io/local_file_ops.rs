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

pub fn local_list_files(prefix_path: &Path) -> crate::universal_io::Result<Vec<ListedFile>> {
    let dir = prefix_path.parent().unwrap_or(Path::new("."));
    let file_prefix = prefix_path
        .file_name()
        .map(|str| str.to_string_lossy().into_owned())
        .unwrap_or_default();

    let mut results = Vec::new();
    let entries =
        fs_err::read_dir(dir).map_err(|err| UniversalIoError::extract_not_found(err, dir))?;

    for entry in entries {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str()
            && name.starts_with(&file_prefix)
            && entry.file_type()?.is_file()
        {
            let path = dir.join(name);
            let size = entry.metadata()?.len();

            results.push(ListedFile { path, size });
        }
    }

    Ok(results)
}
