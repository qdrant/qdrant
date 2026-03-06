use std::path::{Path, PathBuf};

use fs_err as fs;

pub const WAL_PATH: &str = "wal";
pub const SEGMENTS_PATH: &str = "segments";
pub const NEWEST_CLOCKS_PATH: &str = "newest_clocks.json";
pub const OLDEST_CLOCKS_PATH: &str = "oldest_clocks.json";
pub const APPLIED_SEQ_FILE: &str = "applied_seq.json";

/// Shard represents all files, associated with a shard data (excluding configs)
/// Useful to not forget some files, while making operations on shard data
#[derive(Debug, Clone)]
pub struct ShardDataFiles {
    pub wal_path: PathBuf,
    pub segments_path: PathBuf,
    pub newest_clocks_path: PathBuf,
    pub oldest_clocks_path: PathBuf,
    pub applied_seq_path: PathBuf,
}

#[inline]
pub fn segments_path(shard_path: &Path) -> PathBuf {
    shard_path.join(SEGMENTS_PATH)
}

#[inline]
pub fn wal_path(shard_path: &Path) -> PathBuf {
    shard_path.join(WAL_PATH)
}

#[inline]
pub fn newest_clocks_path(shard_path: &Path) -> PathBuf {
    shard_path.join(NEWEST_CLOCKS_PATH)
}

#[inline]
pub fn oldest_clocks_path(shard_path: &Path) -> PathBuf {
    shard_path.join(OLDEST_CLOCKS_PATH)
}

#[inline]
pub fn applied_seq_path(shard_path: &Path) -> PathBuf {
    shard_path.join(APPLIED_SEQ_FILE)
}

#[inline]
pub fn get_shard_data_files(shard_path: &Path) -> ShardDataFiles {
    ShardDataFiles {
        wal_path: wal_path(shard_path),
        segments_path: segments_path(shard_path),
        newest_clocks_path: newest_clocks_path(shard_path),
        oldest_clocks_path: oldest_clocks_path(shard_path),
        applied_seq_path: applied_seq_path(shard_path),
    }
}

/// Checks if path have local shard data present
pub fn check_data(shard_path: &Path) -> bool {
    let wal_path = wal_path(shard_path);
    let segments_path = segments_path(shard_path);
    wal_path.exists() && segments_path.exists()
}

pub fn clear_data(shard_path: &Path) -> std::io::Result<()> {
    let shard_data_files = get_shard_data_files(shard_path);
    let ShardDataFiles {
        wal_path,
        segments_path,
        newest_clocks_path,
        oldest_clocks_path,
        applied_seq_path,
    } = shard_data_files;

    if wal_path.exists() {
        fs::remove_dir_all(wal_path)?;
    }

    if segments_path.exists() {
        fs::remove_dir_all(segments_path)?;
    }

    if newest_clocks_path.exists() {
        fs::remove_file(newest_clocks_path)?;
    }

    if oldest_clocks_path.exists() {
        fs::remove_file(oldest_clocks_path)?;
    }

    if applied_seq_path.exists() {
        fs::remove_file(applied_seq_path)?;
    }

    Ok(())
}

pub fn move_data(from_shard_path: &Path, to_shard_path: &Path) -> std::io::Result<()> {
    let from_shard_data_files = get_shard_data_files(from_shard_path);
    let to_shard_data_files = get_shard_data_files(to_shard_path);

    let ShardDataFiles {
        wal_path: from_wal_path,
        segments_path: from_segments_path,
        newest_clocks_path: from_newest_clocks_path,
        oldest_clocks_path: from_oldest_clocks_path,
        applied_seq_path: from_applied_seq_path,
    } = from_shard_data_files;

    let ShardDataFiles {
        wal_path: to_wal_path,
        segments_path: to_segments_path,
        newest_clocks_path: to_newest_clocks_path,
        oldest_clocks_path: to_oldest_clocks_path,
        applied_seq_path: to_applied_seq_path,
    } = to_shard_data_files;

    if from_wal_path.exists() {
        common::fs::move_dir(&from_wal_path, &to_wal_path)?;
    }

    if from_segments_path.exists() {
        common::fs::move_dir(&from_segments_path, &to_segments_path)?;
    }

    if from_newest_clocks_path.exists() {
        common::fs::move_file(&from_newest_clocks_path, &to_newest_clocks_path)?;
    }

    if from_oldest_clocks_path.exists() {
        common::fs::move_file(&from_oldest_clocks_path, &to_oldest_clocks_path)?;
    }

    if from_applied_seq_path.exists() {
        common::fs::move_file(&from_applied_seq_path, &to_applied_seq_path)?;
    }

    Ok(())
}
