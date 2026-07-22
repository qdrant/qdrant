use blobstore::Blobstore;
use blobstore::config::{
    DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_PAGE_SIZE_BYTES, DEFAULT_REGION_SIZE_BLOCKS,
    GridstoreOptions, StorageOptions,
};

use self::inner::MutableFullTextIndexInner;

pub(super) mod inner;
mod lifecycle;
pub mod read_only;
mod read_ops;
#[cfg(test)]
mod tests;

pub(super) const GRIDSTORE_OPTIONS: StorageOptions = StorageOptions::Mutable(GridstoreOptions {
    page_size_bytes: DEFAULT_PAGE_SIZE_BYTES,
    block_size_bytes: DEFAULT_BLOCK_SIZE_BYTES,
    region_size_blocks: DEFAULT_REGION_SIZE_BLOCKS,
    compression: blobstore::config::Compression::None,
});

pub struct MutableFullTextIndex {
    pub(super) inner: MutableFullTextIndexInner,
    pub(super) storage: Blobstore<Vec<u8>>,
}
