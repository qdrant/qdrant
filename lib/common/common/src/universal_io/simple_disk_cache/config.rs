use std::path::{Path, PathBuf};

use fs_err as fs;

use crate::universal_io::{Result, UniversalIoError};

/// Suffix appended to every local cache file so local mirrors can't be
/// mistaken for the "real" (remote) copy.
const LOCAL_FILE_SUFFIX: &str = ".partial";

/// Configuration for [`DiskCache`](super::DiskCache).
///
/// Remote files under `remote_dir` are mirrored under `local_dir`, preserving the
/// relative path with [`LOCAL_FILE_SUFFIX`] appended to the file name.
#[derive(Debug)]
pub struct DiskCacheConfig {
    remote_dir: PathBuf,
    local_dir: PathBuf,
}

impl DiskCacheConfig {
    pub fn new(remote_dir: PathBuf, local_dir: PathBuf) -> Result<Self> {
        let remote_dir = fs::canonicalize(&remote_dir)
            .map_err(|err| UniversalIoError::extract_not_found(err, &remote_dir))?;
        let local_dir = fs::canonicalize(&local_dir)
            .map_err(|err| UniversalIoError::extract_not_found(err, &local_dir))?;
        Ok(Self {
            remote_dir,
            local_dir,
        })
    }

    pub fn local_dir(&self) -> &Path {
        &self.local_dir
    }

    /// Canonicalises `remote_path` (does filesystem I/O); returns
    /// [`UniversalIoError::NotFound`] if the result isn't under `remote_dir`.
    pub fn local_path_for(&self, remote_path: &Path) -> Result<PathBuf> {
        let canonical = fs::canonicalize(remote_path)
            .map_err(|err| UniversalIoError::extract_not_found(err, remote_path))?;
        let rel =
            canonical
                .strip_prefix(&self.remote_dir)
                .map_err(|_| UniversalIoError::NotFound {
                    path: remote_path.to_path_buf(),
                })?;

        let mut local = self.local_dir.join(rel);
        local.as_mut_os_string().push(LOCAL_FILE_SUFFIX);
        Ok(local)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use fs_err as fs;

    use super::DiskCacheConfig;

    #[test]
    fn strips_remote_dir_and_appends_suffix() {
        let tmp = tempfile::Builder::new()
            .prefix("simplediskcache-tests")
            .tempdir()
            .unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");
        fs::create_dir_all(remote_dir.join("collections/c/segment")).unwrap();
        fs::create_dir_all(&local_dir).unwrap();
        let input = remote_dir.join("collections/c/segment/data.bin");
        fs::write(&input, b"").unwrap();

        let cfg = DiskCacheConfig::new(remote_dir, local_dir).unwrap();
        let local = cfg.local_path_for(&input).unwrap();

        assert_eq!(
            local,
            cfg.local_dir()
                .join("collections/c/segment/data.bin.partial"),
        );
    }

    #[test]
    fn rejects_path_outside_remote_dir() {
        let tmp = tempfile::Builder::new()
            .prefix("simplediskcache-tests")
            .tempdir()
            .unwrap();
        let remote_dir = tmp.path().join("remote");
        let other_dir = tmp.path().join("other");
        let local_dir = tmp.path().join("local");
        fs::create_dir_all(&remote_dir).unwrap();
        fs::create_dir_all(&other_dir).unwrap();
        fs::create_dir_all(&local_dir).unwrap();
        let outside = other_dir.join("data.bin");
        fs::write(&outside, b"").unwrap();

        let cfg = DiskCacheConfig::new(remote_dir, local_dir).unwrap();
        let err = cfg.local_path_for(&outside).unwrap_err();

        assert_matches!(err, crate::universal_io::UniversalIoError::NotFound { .. });
    }
}
