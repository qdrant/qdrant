use gridstore::Gridstore;

pub(super) mod inner;
mod lifecycle;
pub mod read_only;
mod read_ops;

pub use self::inner::InMemoryGeoIndex;
use crate::types::RawGeoPoint;

pub struct MutableGeoIndex {
    pub(super) in_memory_index: InMemoryGeoIndex,
    pub(super) storage: Gridstore<Vec<RawGeoPoint>>,
}
