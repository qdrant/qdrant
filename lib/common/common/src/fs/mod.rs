pub mod atomic;
mod check;
mod fadvise;
mod r#move;
mod ops;
mod safe_delete;
mod sync;

pub use std::fs::{FileType, Metadata};

pub use check::{FsCheckResult, check_fs_info, check_mmap_functionality};
pub use fadvise::{OneshotFile, clear_disk_cache};
pub use fs_err::{
    DirEntry, File, OpenOptions, ReadDir, copy, create_dir, create_dir_all, read, read_dir,
    read_to_string, remove_dir, remove_dir_all, remove_file, rename, write,
};
pub use r#move::{move_dir, move_file};
pub use ops::{
    Error as FileOperationError, FileOperationResult, FileStorageError, atomic_save,
    atomic_save_bin, atomic_save_json, read_bin, read_json,
};
#[cfg(not(target_arch = "wasm32"))]
pub use safe_delete::sync_parent_dir_async;
pub use safe_delete::{safe_delete_in_tmp, safe_delete_with_suffix, sync_parent_dir};
pub use sync::bulk_sync_dir;

pub fn exists(path: impl AsRef<std::path::Path>) -> std::io::Result<bool> {
    Ok(path.as_ref().exists())
}
