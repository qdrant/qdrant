use std::path::{Path, PathBuf};

use fs_err as fs;
use walkdir::WalkDir;

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
