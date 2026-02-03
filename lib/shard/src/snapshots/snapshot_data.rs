use common::tempfile_ext::MaybeTempPath;
use tempfile::TempDir;

pub enum SnapshotData {
    /// Tar file containing the snapshot, needs to be unpacked
    Packed(MaybeTempPath),
    /// Directory containing the unpacked snapshot
    Unpacked(TempDir),
}

impl SnapshotData {
    pub fn new_packed_persistent<P: AsRef<std::path::Path>>(path: P) -> Self {
        SnapshotData::Packed(MaybeTempPath::Persistent(path.as_ref().to_path_buf()))
    }

    /// Get path to the downloaded data
    pub fn path(&self) -> &std::path::Path {
        match self {
            SnapshotData::Packed(maybe_path) => maybe_path,
            SnapshotData::Unpacked(temp_dir) => temp_dir.path(),
        }
    }
}
