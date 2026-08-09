use blobstore::Blobstore;
use blobstore::config::{
    CreateOptions, DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_PAGE_SIZE_BYTES, StorageConfig,
};

use self::inner::MutableFullTextIndexInner;

pub(super) mod inner;
mod lifecycle;
pub mod read_only;
mod read_ops;
#[cfg(test)]
mod tests;
pub mod update_only;

pub(super) fn storage_options() -> StorageConfig {
    crate::common::blobstore_config::storage_config(CreateOptions {
        page_size_bytes: DEFAULT_PAGE_SIZE_BYTES,
        block_size_bytes: DEFAULT_BLOCK_SIZE_BYTES,
        compression: blobstore::config::Compression::None,
    })
}

pub struct MutableFullTextIndex {
    pub(super) inner: MutableFullTextIndexInner,
    pub(super) storage: Blobstore<Vec<u8>>,
}
