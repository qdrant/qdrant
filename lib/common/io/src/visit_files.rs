use std::path::{Path, PathBuf};

pub fn visit_files_recursively(dir: &Path, cb: &mut impl FnMut(PathBuf)) -> std::io::Result<()> {
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_files_recursively(&path, cb)?;
            } else {
                cb(entry.path());
            }
        }
    }
    Ok(())
}
