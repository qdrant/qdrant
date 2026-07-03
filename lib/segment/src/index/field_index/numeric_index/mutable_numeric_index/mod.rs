use std::collections::BTreeSet;

use gridstore::config::StorageOptions;
use gridstore::{Blob, Gridstore};

use super::Encodable;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};

mod lifecycle;
pub mod read_only;
mod read_ops;

/// Default options for Gridstore storage
pub(super) const fn default_gridstore_options<T: Sized>() -> StorageOptions {
    let block_size = size_of::<T>();
    StorageOptions {
        // Size of numeric values in index
        block_size_bytes: Some(block_size),
        // Compressing numeric values is unreasonable
        compression: Some(gridstore::config::Compression::None),
        // Scale page size down with block size, prevents overhead of first page when there's (almost) no values
        page_size_bytes: Some(block_size * 8192 * 32), // 4 to 8 MiB = block_size * region_blocks * regions,
        region_size_blocks: None,
        mode: None,
    }
}

pub struct MutableNumericIndex<T: Encodable + Numericable>
where
    Vec<T>: Blob,
{
    // Backing storage, source of state, persists deletions
    pub(super) storage: Gridstore<Vec<T>>,
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
