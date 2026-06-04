use gridstore::{Blob, Gridstore};

use self::in_memory::InMemoryMapIndex;
use super::MapIndexKey;

pub(super) mod in_memory;
mod lifecycle;
pub mod read_only;
mod read_ops;

pub struct MutableMapIndex<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(super) in_memory_index: InMemoryMapIndex<N>,
    pub(super) storage: Gridstore<Vec<<N as MapIndexKey>::Owned>>,
}
