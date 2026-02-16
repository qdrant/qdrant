pub mod file_operations {
    pub use common::fs::{
        FileOperationResult as Result, FileOperationResult, FileStorageError as Error,
        FileStorageError, atomic_save, atomic_save_bin, atomic_save_json, read_bin, read_json,
    };
}

pub mod move_files {
    pub use common::fs::{move_dir, move_file};
}

pub mod safe_delete {
    pub use common::fs::{
        safe_delete_in_tmp, safe_delete_with_suffix, sync_parent_dir, sync_parent_dir_async,
    };
}

pub mod storage_version {
    pub use common::storage_version::{StorageVersion, VERSION_FILE};
}
