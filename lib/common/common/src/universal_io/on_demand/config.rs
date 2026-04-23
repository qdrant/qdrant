use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;

/// Suffix appended to every local cache file so local mirrors can't be
/// mistaken for the "real" (remote) copy.
const LOCAL_FILE_SUFFIX: &str = ".ondemand";

/// Configuration for [`OnDemandFile`](super::OnDemandFile).
///
/// The remote directory structure is mirrored below [`local_dir`](Self::local_dir),
/// and each mirrored file is suffixed with [`LOCAL_FILE_SUFFIX`]. Root /
/// prefix / `.` / `..` components of the remote path are dropped when
/// computing the local path, so the mirror is always materialised under
/// `local_dir`.
#[derive(Debug)]
pub struct OnDemandConfig {
    local_dir: PathBuf,
}

static GLOBAL: OnceLock<OnDemandConfig> = OnceLock::new();

impl OnDemandConfig {
    pub fn new(local_dir: PathBuf) -> Self {
        Self { local_dir }
    }

    /// Install the process-wide instance. Panics if called more than once.
    pub fn initialize_global(local_dir: PathBuf) {
        GLOBAL
            .set(Self::new(local_dir))
            .expect("OnDemandConfig is already initialized");
    }

    /// Returns the globally-installed instance, if any.
    pub fn global() -> Option<&'static OnDemandConfig> {
        GLOBAL.get()
    }

    pub fn local_dir(&self) -> &Path {
        &self.local_dir
    }

    /// Maps a remote path to its corresponding local cache path.
    pub fn local_path_for(&self, remote_path: &Path) -> PathBuf {
        let rel: PathBuf = remote_path
            .components()
            .filter(|c| matches!(c, Component::Normal(_)))
            .collect();

        let mut local = self.local_dir.join(&rel);
        let mut filename = local
            .file_name()
            .map(|f| f.to_os_string())
            .unwrap_or_default();
        filename.push(LOCAL_FILE_SUFFIX);
        local.set_file_name(filename);
        local
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::OnDemandConfig;

    #[test]
    fn absolute_remote_path_is_mirrored_under_local_dir() {
        let cfg = OnDemandConfig::new(PathBuf::from("/var/cache/ondemand"));
        let local = cfg.local_path_for(Path::new("/storage/collections/c/segment/data.bin"));
        assert_eq!(
            local,
            Path::new("/var/cache/ondemand/storage/collections/c/segment/data.bin.ondemand"),
        );
    }

    #[test]
    fn relative_remote_path_is_mirrored_as_is() {
        let cfg = OnDemandConfig::new(PathBuf::from("/cache"));
        let local = cfg.local_path_for(Path::new("foo/bar.bin"));
        assert_eq!(local, Path::new("/cache/foo/bar.bin.ondemand"));
    }

    #[test]
    fn parent_components_are_stripped() {
        let cfg = OnDemandConfig::new(PathBuf::from("/cache"));
        let local = cfg.local_path_for(Path::new("../../etc/passwd"));
        assert_eq!(local, Path::new("/cache/etc/passwd.ondemand"));
    }
}
