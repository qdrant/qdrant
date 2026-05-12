use std::collections::HashMap;

use gridstore::{Blob, Gridstore};
use roaring::RoaringBitmap;

use super::MapIndexKey;

mod lifecycle;
mod read_ops;

pub struct MutableMapIndex<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(super) map: HashMap<<N as MapIndexKey>::Owned, RoaringBitmap>,
    pub(super) point_to_values: Vec<Vec<<N as MapIndexKey>::Owned>>,
    /// Amount of point which have at least one indexed payload value
    pub(super) indexed_points: usize,
    pub(super) values_count: usize,
    pub(super) storage: Storage<<N as MapIndexKey>::Owned>,
}

pub(super) enum Storage<T>
where
    Vec<T>: Blob + Send + Sync,
{
    Gridstore(Gridstore<Vec<T>>),
}
