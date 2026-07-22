mod cached_fs;
#[cfg(not(target_os = "windows"))]
#[expect(dead_code, reason = "Not yet used")]
mod disk_cache;
mod error;
#[cfg(target_os = "linux")]
mod io_uring;
mod local_file_ops;
mod mmap;
mod oneshot;
mod simple_disk_cache;
mod sorted_block_index;
mod traits;
mod types;
mod wrappers;

#[cfg(any(test, feature = "testing"))]
pub mod conformance;
#[cfg(test)]
mod tests;

pub use self::cached_fs::{CachedFs, CachedReadFsContext};
pub use self::error::{IsNotFound, OkNotFound, UniversalIoError};
#[cfg(target_os = "linux")]
pub use self::io_uring::{IoUringFile, IoUringFs, IoUringOpenExtra};
pub use self::mmap::{MmapFile, MmapFs};
pub use self::oneshot::OneshotFile;
pub use self::simple_disk_cache::{
    DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, DiskCacheRemote,
};
pub use self::sorted_block_index::SortedBlockIndex;
pub use self::traits::{
    CachedReadFs, Item, OpenExtra, OwnedPipeline, ReadPipeline, UniversalAppend, UniversalFlush,
    UniversalRead, UniversalReadFileOps, UniversalReadFs, UniversalWrite, UniversalWriteFileOps,
    UserData,
};
pub use self::types::{
    ByteOffset, FileIndex, Flusher, ListedFile, OpenOptions, Populate, ReadBytesItem, ReadRange,
    UioResult, UniversalKind, read_bin_via, read_json_via, read_whole_via,
};
pub use self::wrappers::{ReadOnly, SliceBufferedUpdateWrapper, StoredStruct, TypedStorage};
