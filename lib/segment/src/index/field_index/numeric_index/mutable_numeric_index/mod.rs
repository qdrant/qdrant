use std::collections::BTreeSet;

use blobstore::config::{CreateOptions, DEFAULT_REGION_SIZE_BLOCKS, StorageConfig};
use blobstore::{Blob, Blobstore};

use super::Encodable;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};

mod lifecycle;
pub mod read_only;
mod read_ops;
pub mod update_only;

/// Default options for the backing storage
pub(super) fn storage_options<T: Sized>() -> StorageConfig {
    let block_size = size_of::<T>();
    crate::common::blobstore_config::storage_config(CreateOptions {
        // Scale page size down with block size, prevents overhead of first page when there's (almost) no values
        page_size_bytes: block_size * DEFAULT_REGION_SIZE_BLOCKS * 32, // 4 to 8 MiB = block_size * region_blocks * regions,
        // Size of numeric values in index
        block_size_bytes: block_size,
        // Compressing numeric values is unreasonable
        compression: blobstore::config::Compression::None,
    })
}

pub struct MutableNumericIndex<T: Encodable + Numericable>
where
    Vec<T>: Blob,
{
    // Backing storage, source of state, persists deletions
    pub(super) storage: Blobstore<Vec<T>>,
    pub(super) in_memory_index: InMemoryNumericIndex<T>,
}

// Numeric Index with insertions and deletions without persistence
pub struct InMemoryNumericIndex<T: Encodable + Numericable> {
    pub map: BTreeSet<Point<T>>,
    pub histogram: Histogram<T>,
    pub points_count: usize,
    pub max_values_per_point: usize,
    pub point_to_values: Vec<Vec<T>>,
}
