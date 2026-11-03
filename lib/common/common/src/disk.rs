use std::path::{Path, PathBuf};

use fs_err as fs;
use walkdir::WalkDir;

/// Total apparent (logical) size of the files in a directory, as reading them would return.
///
/// Unlike [`dir_disk_size`], sparse and compressed files count at their full length.
pub fn dir_apparent_size(path: impl AsRef<Path>) -> std::io::Result<u64> {
    WalkDir::new(path)
        .follow_links(true)
        .into_iter()
        .try_fold(0, |acc, entry| {
            let metadata = entry?.metadata()?;
            Ok(if metadata.is_file() {
                acc + metadata.len()
            } else {
                acc
            })
        })
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

    /// Tar reads sparse files at full length, so part sizing must not count only allocated blocks.
    #[test]
    fn dir_apparent_size_counts_sparse_files_at_full_length() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("nested")).unwrap();
        let sparse = fs::File::create(dir.path().join("nested/sparse")).unwrap();
        sparse.set_len(64 * 1024 * 1024).unwrap();
        fs::write(dir.path().join("small"), b"abc").unwrap();

        assert_eq!(dir_apparent_size(dir.path()).unwrap(), 64 * 1024 * 1024 + 3);
    }

    /// Snapshots read collection data through symlinks, so sizing must count it too.
    #[cfg(unix)]
    #[test]
    fn dir_apparent_size_follows_symlinked_dirs() {
        let target = tempfile::tempdir().unwrap();
        fs::write(target.path().join("data"), [0u8; 100]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(target.path(), dir.path().join("linked")).unwrap();

        assert_eq!(dir_apparent_size(dir.path()).unwrap(), 100);
    }
}
