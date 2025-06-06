use std::path::{Path, PathBuf};

use walkdir::WalkDir;

/// How many bytes a directory takes.
pub fn dir_size(path: impl Into<PathBuf>) -> std::io::Result<u64> {
    fn dir_size(mut dir: std::fs::ReadDir) -> std::io::Result<u64> {
        dir.try_fold(0, |acc, file| {
            let file = file?;
            let size = match file.metadata()? {
                data if data.is_dir() => dir_size(std::fs::read_dir(file.path())?)?,
                data => data.len(),
            };
            Ok(acc + size)
        })
    }

    dir_size(std::fs::read_dir(path.into())?)
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
