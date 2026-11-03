use std::path::{Path, PathBuf};

use fs_err as fs;
use walkdir::WalkDir;

/// Total apparent (logical) size of the files in a directory, as reading them would return.
///
/// Unlike [`dir_disk_size`], sparse and compressed files count at their full length, and
/// symlinks are followed.
pub fn dir_apparent_size(path: impl AsRef<Path>) -> std::io::Result<u64> {
    let is_dangling_link = |err: &walkdir::Error| {
        err.io_error()
            .is_some_and(|err| err.kind() == std::io::ErrorKind::NotFound)
            && err.path().is_some_and(Path::is_symlink)
    };
    let mut size = 0;
    for entry in WalkDir::new(path).follow_links(true) {
        let metadata = match entry.and_then(|entry| entry.metadata()) {
            Ok(metadata) => metadata,
            // Link loops and dangling links add no data; other errors, like files vanishing mid-scan, fail
            Err(err) if err.loop_ancestor().is_some() || is_dangling_link(&err) => continue,
            Err(err) => return Err(err.into()),
        };
        if metadata.is_file() {
            size += metadata.len();
        }
    }
    Ok(size)
}

/// How many bytes a directory takes on disk.
///
/// Note: on non-unix systems, this function returns the apparent/logical
/// directory size rather than actual disk usage.
pub fn dir_disk_size(path: impl Into<PathBuf>) -> std::io::Result<u64> {
    fn dir_disk_size(mut dir: fs::ReadDir) -> std::io::Result<u64> {
        dir.try_fold(0, |acc, file| {
            let file = file?;
            let metadata = file.metadata()?;
            let size = if metadata.is_dir() {
                dir_disk_size(fs::read_dir(file.path())?)?
            } else {
                #[cfg(unix)]
                {
                    const BLOCK_SIZE: u64 = 512; // aka DEV_BSIZE
                    use std::os::unix::fs::MetadataExt;
                    metadata.blocks() * BLOCK_SIZE
                }
                #[cfg(not(unix))]
                {
                    metadata.len()
                }
            };
            Ok(acc + size)
        })
    }

    dir_disk_size(fs::read_dir(path.into())?)
}

/// List all files in the given directory recursively.
///
/// Notes:
/// - a directory must be given
/// - symlinks are considered to be files
pub fn list_files(dir: impl AsRef<Path>) -> std::io::Result<Vec<PathBuf>> {
    let dir = dir.as_ref();
    if !dir.is_dir() {
        return Ok(vec![]);
    }

    let mut files = Vec::new();
    for entry in WalkDir::new(dir).min_depth(1).follow_links(true) {
        let entry = entry?;
        let file_type = entry.file_type();
        if file_type.is_file() || file_type.is_symlink() {
            files.push(entry.into_path());
        }
    }

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Counts what a snapshot would read: sparse files at full length and data behind
    /// symlinks. Dangling links hold no data and must not fail the scan.
    #[cfg(unix)]
    #[test]
    fn dir_apparent_size_counts_readable_bytes() {
        let linked = tempfile::tempdir().unwrap();
        fs::write(linked.path().join("data"), [0u8; 100]).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let sparse = fs::File::create(dir.path().join("sparse")).unwrap();
        sparse.set_len(64 * 1024 * 1024).unwrap();
        std::os::unix::fs::symlink(linked.path(), dir.path().join("linked")).unwrap();
        std::os::unix::fs::symlink("/nonexistent", dir.path().join("dangling")).unwrap();

        assert_eq!(
            dir_apparent_size(dir.path()).unwrap(),
            64 * 1024 * 1024 + 100
        );
    }
}
