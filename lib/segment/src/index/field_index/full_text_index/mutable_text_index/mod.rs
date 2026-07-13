use blobstore::Blobstore;
use blobstore::config::StorageOptions;

use self::inner::MutableFullTextIndexInner;

pub(super) mod inner;
mod lifecycle;
pub mod read_only;
mod read_ops;
#[cfg(test)]
mod tests;

pub(super) const GRIDSTORE_OPTIONS: StorageOptions = StorageOptions {
    compression: Some(blobstore::config::Compression::None),
    page_size_bytes: None,
    block_size_bytes: None,
    region_size_blocks: None,
    mode: None,
};

pub struct MutableFullTextIndex {
    pub(super) inner: MutableFullTextIndexInner,
    pub(super) storage: Blobstore<Vec<u8>>,
}
