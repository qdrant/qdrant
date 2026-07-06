use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::universal_io::{ListedFile, Result, UniversalReadFs};

#[derive(Debug)]
pub struct FileInfo {
    /// Length in bytes of the entire file
    pub size: u64,
}

#[derive(Debug)]
pub struct RemoteManifest {
    /// A preloaded view of the remote files, which include filepaths, sizes, and even partial reads.
    files: HashMap<PathBuf, FileInfo>,
}

impl RemoteManifest {
    pub fn new<Fs: UniversalReadFs>(fs: &Fs, prefix_path: &Path) -> Result<Self> {
        // List all files
        let list = fs.list_files(prefix_path)?;

        let files: HashMap<_, _> = list
            .into_iter()
            .map(|ListedFile { path, size }| {
                let info = FileInfo { size };
                (path, info)
            })
            .collect();

        Ok(Self { files })
    }

    pub fn get(&self, path: &Path) -> Option<&FileInfo> {
        self.files.get(path)
    }

    pub fn list_files(&self, prefix_path: &Path) -> Vec<ListedFile> {
        let prefix_string = prefix_path.to_string_lossy();

        self.files
            .iter()
            .filter(|(path, _)| path.to_string_lossy().starts_with(prefix_string.as_ref()))
            .map(|(path, info)| ListedFile {
                path: path.clone(),
                size: info.size,
            })
            .collect()
    }

    pub fn exists(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }
}
