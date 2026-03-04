use std::path::{Path, PathBuf};

use crate::universal_io::UniversalIoError;

pub fn local_list_files(prefix_path: &Path) -> crate::universal_io::Result<Vec<PathBuf>> {
    let dir = prefix_path.parent().unwrap_or(Path::new("."));
    let file_prefix = prefix_path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();

    let mut results = Vec::new();
    let entries = fs_err::read_dir(dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            UniversalIoError::NotFound {
                path: dir.to_path_buf(),
            }
        } else {
            UniversalIoError::from(e)
        }
    })?;

    for entry in entries {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str()
            && name.starts_with(&file_prefix)
            && entry.file_type()?.is_file()
        {
            results.push(dir.join(name));
        }
    }

    Ok(results)
}
