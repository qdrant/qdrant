use std::io;
use std::path::{Path, PathBuf};

use fs_err as fs;
use fs_extra::dir::CopyOptions;

/// Move directory from one location to another
///
/// Handles the case when the source and destination are on different filesystems.
/// If destination directory exists, the contents of the source directory are moved
/// into the destination directory, preserving existing files in the destination.
pub fn move_dir(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> io::Result<()> {
    let from = from.into();
    let to = to.into();

    let Err(_err) = fs::rename(&from, &to) else {
        return Ok(());
    };

    if !to.exists() {
        fs::create_dir(&to)?;
    }

    let opts = CopyOptions::new().content_only(true).overwrite(true);

    fs_extra::dir::move_dir(&from, &to, &opts).map_err(|err| {
        io::Error::other(format!(
            "failed to move directory {} to {}: {err}",
            from.display(),
            to.display(),
        ))
    })?;
    Ok(())
}

/// Move file from one location to another.
/// Handles the case when the source and destination are on different filesystems.
pub fn move_file(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let from = from.as_ref();
    let to = to.as_ref();

    // Try to rename first and fallback to copy to prevent TOCTOU.
    if let Ok(()) = fs::rename(from, to) {
        return Ok(());
    }

    // If rename failed, try to copy.
    // It is possible that the source and destination are on different filesystems.
    if let Err(err) = fs::copy(from, to) {
        let _res = cleanup_file(to);
        return Err(err);
    }

    if let Err(err) = fs::remove_file(from) {
        let _res = cleanup_file(to);
        return Err(err);
    }

    Ok(())
}

/// Remove the file if it exists. Print a warning if the file can't be removed.
fn cleanup_file(path: &Path) -> io::Result<()> {
    if let Err(err) = fs::remove_file(path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        return Err(err);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_move_dir_preserves_existing_destination_files() {
        let temp = tempdir().unwrap();

        // Create source directory with a file
        let source_dir = temp.path().join("source");
        fs::create_dir(&source_dir).unwrap();
        fs::write(source_dir.join("source_file.txt"), "source content").unwrap();

        // Create destination directory with a different file
        let dest_dir = temp.path().join("dest");
        fs::create_dir(&dest_dir).unwrap();
        fs::write(dest_dir.join("existing_file.txt"), "existing content").unwrap();

        // Move source to destination
        move_dir(&source_dir, &dest_dir).unwrap();

        // Verify source file was moved
        assert!(dest_dir.join("source_file.txt").exists());
        assert_eq!(
            fs::read_to_string(dest_dir.join("source_file.txt")).unwrap(),
            "source content"
        );

        // Verify existing destination file is preserved
        assert!(dest_dir.join("existing_file.txt").exists());
        assert_eq!(
            fs::read_to_string(dest_dir.join("existing_file.txt")).unwrap(),
            "existing content"
        );

        // Verify source directory is removed
        assert!(!source_dir.exists());
    }
}
