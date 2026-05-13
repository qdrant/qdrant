use gridstore::Gridstore;

pub(super) mod inner;
mod lifecycle;
pub mod read_only;
mod read_ops;

pub use self::inner::InMemoryGeoMapIndex;
use crate::types::RawGeoPoint;

pub struct MutableGeoMapIndex {
    pub(super) in_memory_index: InMemoryGeoMapIndex,
    pub(super) storage: Gridstore<Vec<RawGeoPoint>>,
}
