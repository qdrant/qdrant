use gridstore::Gridstore;
use gridstore::config::StorageOptions;

use self::inner::MutableFullTextIndexInner;

pub(super) mod inner;
mod lifecycle;
pub mod read_only;
mod read_ops;
#[cfg(test)]
mod tests;

pub(super) const GRIDSTORE_OPTIONS: StorageOptions = StorageOptions {
    compression: Some(gridstore::config::Compression::None),
    page_size_bytes: None,
    block_size_bytes: None,
    region_size_blocks: None,
};

pub struct MutableFullTextIndex {
    pub(super) inner: MutableFullTextIndexInner,
    pub(super) storage: Gridstore<Vec<u8>>,
}
