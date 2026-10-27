mod check;
mod fadvise;
#[cfg_attr(
    not(any(target_os = "linux", target_os = "android")),
    path = "fallocate_stub.rs"
)]
mod fallocate;
mod r#move;
mod ops;
mod safe_delete;
mod sync;

pub use check::{FsCheckResult, check_fs_info, check_mmap_functionality};
pub use fadvise::{OneshotFile, clear_disk_cache};
pub use fallocate::FallocatedPages;
pub use r#move::{move_dir, move_file};
pub use ops::{atomic_save, atomic_save_bin, atomic_save_json, read_bin, read_json};
pub use safe_delete::{
    safe_delete_in_tmp, safe_delete_with_suffix, sync_parent_dir, sync_parent_dir_async,
};
pub use sync::bulk_sync_dir;
