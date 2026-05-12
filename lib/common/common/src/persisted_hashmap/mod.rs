mod mmap;
mod serialize;
mod structures;
mod uio;

#[cfg(any(test, feature = "testing"))]
pub mod fixtures;
#[cfg(test)]
mod tests;

pub use mmap::{MmapHashMap, READ_ENTRY_OVERHEAD};
pub use serialize::serialize_hashmap;
pub use structures::Key;
use structures::{
    BucketOffset, Header, MaybeIncompleteEntry, MaybeIncompleteEntryKind, ReadResult, ValuesLen,
};
pub use uio::UniversalHashMap;

fn read_err(msg: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}
