// This file contains functions to verify that current file system is POSIX compliant.
// There are some possible checks we can run here:
// 1. Read information about the file system. If it is unknown or known to be not POSIX compliant, return false.
// 2. Try to create, fill and save mmap file with some dummy data. If file is possible to read after it is closed, return true.
// Some file systems are known to fail this test, so we need to check for that and notify user before it is too late.

use std::fs::create_dir_all;
use std::io;
use std::path::Path;

#[cfg(fs_type_check_supported)]
use nix::sys::statfs::statfs;

use crate::madvise::{Advice, AdviceSetting};
use crate::mmap_ops::{create_and_ensure_length, open_read_mmap, open_write_mmap};

#[derive(Debug)]
pub enum FsCheckResult {
    Good,
    Unknown(String),
    Bad(String),
}

const MAGIC_QDRANT_BYTES: &[u8] = b"qdrant";
const MAGIC_FILE_SIZE: usize = 32 * 1024; // 32 Kb

const MAGIC_BYTES_POSITION: usize = MAGIC_FILE_SIZE / 2; // write in the middle

const MAGIC_FILE_NAME: &str = ".qdrant_fs_check";

#[derive(Debug, PartialEq)]
pub enum FsType {
    Ext234,
    Btrfs,
    Xfs,
    Nfs,
    Fuse,
    Tmpfs,
    Ntfs,
    Hfs,
    Overlayfs,
    Squashfs,
    Cifs,
    Other,
}

impl FsType {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn from_magic(magic: i64) -> Self {
        match magic {
            0xEF53 => Self::Ext234,
            0x9123683E => Self::Btrfs,
            0x58465342 => Self::Xfs,
            0x6969 => Self::Nfs,
            0x65735546 => Self::Fuse,
            0x01021994 => Self::Tmpfs,
            0x5346544e => Self::Ntfs,
            0x4244 => Self::Hfs,
            0x794c7630 => Self::Overlayfs,
            0x73717368 => Self::Squashfs,
            0xFF534D42 => Self::Cifs,
            _ => Self::Other,
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn from_name(name: &str) -> Self {
        // Names reference is taken from
        // https://github.com/happyfish100/libfastcommon/blob/7f1a85b025675671905447da13b7727323eb0c28/src/system_info.c#L203

        match name {
            "ext2" | "ext3" | "ext4" => Self::Ext234,
            "btrfs" => Self::Btrfs,
            "xfs" => Self::Xfs,
            "ntfs" => Self::Ntfs,
            "nfs" => Self::Nfs,
            "hfs" => Self::Hfs,
            "fuse" => Self::Fuse,
            "overlayfs" => Self::Overlayfs,
            "squashfs" => Self::Squashfs,
            "cifs" => Self::Cifs,
            "tmpfs" => Self::Tmpfs,
            _ => Self::Other,
        }
    }
}

/// Return a string representing the file system type of a given path.
/// It uses nix::sys::statfs to retrieve the magic number.
#[cfg(fs_type_check_supported)]
fn get_filesystem_type(path: impl AsRef<Path>) -> Result<FsType, String> {
    let stat = statfs(path.as_ref()).map_err(|e| format!("statfs failed: {e}"))?;

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let fs_name = stat.filesystem_type_name();
        let fs_type = FsType::from_name(fs_name);
        Ok(fs_type)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let f_type = stat.filesystem_type().0;

        let fs_type = FsType::from_magic(f_type);
        Ok(fs_type)
    }
}

/// Check filesystem information to identify known non-POSIX filesystems
#[cfg(fs_type_check_supported)]
pub fn _check_fs_info(path: impl AsRef<Path>) -> FsCheckResult {
    let path = path.as_ref();

    let Ok(fs_type) = get_filesystem_type(path) else {
        return FsCheckResult::Unknown(
            format!("Failed to get filesystem type for path: {path:?}",),
        );
    };
    match fs_type {
        FsType::Ext234 => FsCheckResult::Good,
        FsType::Btrfs => FsCheckResult::Good,
        FsType::Xfs => FsCheckResult::Good,
        FsType::Nfs => FsCheckResult::Bad(
            "NFS may cause data corruption due to inconsistent file locking".to_string(),
        ),
        FsType::Fuse => FsCheckResult::Bad(
            "FUSE filesystems may cause data corruption due to caching issues".to_string(),
        ),
        FsType::Tmpfs => FsCheckResult::Unknown(
            "Data will be lost on system restart - tmpfs is memory-based".to_string(),
        ),
        FsType::Ntfs => FsCheckResult::Good,
        FsType::Hfs => {
            FsCheckResult::Unknown("HFS/HFS+ filesystem support is untested".to_string())
        }
        FsType::Overlayfs => FsCheckResult::Unknown(
            "Container filesystem detected - storage might be lost with container re-creation"
                .to_string(),
        ),
        FsType::Squashfs => {
            FsCheckResult::Unknown("Read-only filesystem detected - writes will fail".to_string())
        }
        FsType::Cifs => FsCheckResult::Bad(
            "CIFS/SMB may cause data corruption due to inconsistent file locking".to_string(),
        ),
        FsType::Other => FsCheckResult::Unknown(
            "Unrecognized filesystem - cannot guarantee data safety".to_string(),
        ),
    }
}

pub fn check_fs_info(path: impl AsRef<Path>) -> FsCheckResult {
    if !path.as_ref().exists() {
        return FsCheckResult::Unknown("Path does not exist".to_string());
    }

    #[cfg(fs_type_check_supported)]
    {
        _check_fs_info(path)
    }

    #[cfg(not(fs_type_check_supported))]
    {
        FsCheckResult::Unknown(
            "Filesystem type check is not supported on this platform".to_string(),
        )
    }
}

/// This function simulates an access pattern we use in vector storage and gridstore
/// This check fails, it means that fundamental assumptions about file system are violated
/// therefore, there are no guarantees that data will be safe
pub fn check_mmap_functionality(path: impl AsRef<Path>) -> io::Result<bool> {
    let path = path.as_ref();
    let magic_file_path = path.join(MAGIC_FILE_NAME);

    // Remove file and folder if they exist
    if magic_file_path.exists() {
        std::fs::remove_file(&magic_file_path)?;
    }

    create_dir_all(path)?;

    create_and_ensure_length(&magic_file_path, MAGIC_FILE_SIZE)?;

    let mut mmap = open_write_mmap(&magic_file_path, AdviceSetting::Global, false)?;

    let mmap_seq = open_read_mmap(
        &magic_file_path,
        AdviceSetting::Advice(Advice::Sequential),
        false,
    )?;

    mmap[MAGIC_BYTES_POSITION..MAGIC_BYTES_POSITION + MAGIC_QDRANT_BYTES.len()]
        .copy_from_slice(MAGIC_QDRANT_BYTES);

    mmap.flush()?;
    drop(mmap);
    drop(mmap_seq);

    if !magic_file_path.exists() {
        return Ok(false);
    }

    // Check the size of the file
    let file_size = magic_file_path.metadata()?.len() as usize;
    if file_size != MAGIC_FILE_SIZE {
        log::debug!("File size is not equal to MAGIC_FILE_SIZE: {file_size} != {MAGIC_FILE_SIZE}");
        return Ok(false);
    }

    let mmap = open_read_mmap(&magic_file_path, AdviceSetting::Global, false)?;
    let mut result = true;

    result &= mmap[MAGIC_BYTES_POSITION..MAGIC_BYTES_POSITION + MAGIC_QDRANT_BYTES.len()]
        == *MAGIC_QDRANT_BYTES;

    drop(mmap);

    if result {
        // If ok, we can remove the file
        // But if not, we might need to for further investigation
        std::fs::remove_file(&magic_file_path)?;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_posix_fs_check() {
        let temp_dir = TempDir::new().unwrap();
        let result = check_fs_info(temp_dir.path());
        println!("Result: {result:?}");
        let result = check_mmap_functionality(temp_dir.path()).unwrap();
        println!("Result: {result}");
    }
}
