use std::{
    fs::{self, OpenOptions},
    io,
    path::Path,
};

pub fn fsync_dir_recursive(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        // Sync all files and subdirectories first
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();

            if entry_path.is_dir() {
                fsync_dir_recursive(&entry_path)?; // Recursively sync subdirectories
            } else {
                fsync_file(&entry_path)?; // Sync individual file
            }
        }
        // Finally, sync the directory itself
        fsync_dir(path)?;
    }
    Ok(())
}

pub fn fsync_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().read(true).open(path)?;
    file.sync_all() // Ensure file content is flushed
}

pub fn fsync_dir(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().read(true).open(path)?; // Open directory
    file.sync_all() // Flush directory metadata
}
