use common::universal_io::UniversalRead;
use gridstore::Blob;

use crate::index::field_index::map_index::MapIndexKey;
use crate::index::field_index::map_index::mutable_map_index::read_only::ReadOnlyAppendableMapIndex;
use crate::index::field_index::map_index::universal_map_index::UniversalMapIndex;

mod read_ops;

pub enum ReadOnlyMapIndex<N: MapIndexKey + ?Sized, S: UniversalRead>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    /// Loads into RAM from appendable storage format
    Appendable(ReadOnlyAppendableMapIndex<N, S>),
    /// Directly reads from storage in immutable format
    Immutable(UniversalMapIndex<N, S>),
}
