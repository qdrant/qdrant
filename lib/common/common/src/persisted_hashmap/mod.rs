mod mmap;
mod serialize;
mod structures;

#[cfg(any(test, feature = "testing"))]
pub mod fixtures;
#[cfg(test)]
mod tests;

pub use mmap::{MmapHashMap, READ_ENTRY_OVERHEAD};
pub use serialize::serialize_hashmap;
pub use structures::Key;
use structures::{BucketOffset, Header, ValuesLen};
