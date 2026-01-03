use std::path::{Path, PathBuf};

pub const WAL_PATH: &str = "wal";
pub const SEGMENTS_PATH: &str = "segments";
pub const NEWEST_CLOCKS_PATH: &str = "newest_clocks.json";
pub const OLDEST_CLOCKS_PATH: &str = "oldest_clocks.json";

#[inline]
pub fn segments_path(shard_path: &Path) -> PathBuf {
    shard_path.join(SEGMENTS_PATH)
}
