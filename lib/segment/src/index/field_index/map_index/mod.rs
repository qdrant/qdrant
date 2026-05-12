use common::types::PointOffsetType;
use gridstore::Blob;

pub use self::builders::{MapIndexBuilder, MapIndexGridstoreBuilder, MapIndexMmapBuilder};
use self::immutable_map_index::ImmutableMapIndex;
pub use self::key::MapIndexKey;
use self::mmap_map_index::MmapMapIndex;
use self::mutable_map_index::MutableMapIndex;

mod builders;
mod facet_index_impl;
pub mod immutable_map_index;
pub mod key;
mod lifecycle;
pub mod mmap_map_index;
pub mod mutable_map_index;
mod payload_index_impl;
mod read_ops;
#[cfg(test)]
mod tests;
mod value_indexer_impl;

/// Block size in Gridstore for keyword map index.
/// Keyword(s) are stored as cbor vector.
/// - "text" - 6 bytes
/// - "some", "text", "here" - 16 bytes
pub(super) const BLOCK_SIZE_KEYWORD: usize = 16;

pub type IdRefIter<'a> = Box<dyn Iterator<Item = &'a PointOffsetType> + 'a>;
pub type IdIter<'a> = Box<dyn Iterator<Item = PointOffsetType> + 'a>;

pub enum MapIndex<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    Mutable(MutableMapIndex<N>),
    Immutable(ImmutableMapIndex<N>),
    Mmap(Box<MmapMapIndex<N>>),
}
