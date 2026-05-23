#[cfg(not(target_os = "windows"))]
#[expect(dead_code, reason = "Not yet used")]
mod disk_cache;
mod error;
#[cfg(target_os = "linux")]
mod io_uring;
mod local_file_ops;
mod mmap;
mod shard_context;
mod traits;
mod types;
mod wrappers;

pub use self::error::UniversalIoError;
#[cfg(target_os = "linux")]
pub use self::io_uring::IoUringFile;
pub use self::mmap::MmapFile;
pub use self::shard_context::{MmapBackendConfig, ShardStorageContext};
pub use self::traits::{
    BorrowedReadPipeline, Item, OwnedReadPipeline, UniversalRead, UniversalReadFileOps,
    UniversalWrite, UserData,
};
pub use self::types::{
    AccessHint, ByteOffset, CacheHint, FileIndex, Flusher, OpenOptions, OpenOptionsExtra, Populate,
    ReadRange, Result, UniversalKind, read_json_via,
};
pub use self::wrappers::{ReadOnly, SliceBufferedUpdateWrapper, StoredStruct, TypedStorage};
