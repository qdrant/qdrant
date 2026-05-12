use gridstore::{Blob, Gridstore};

use self::inner::MutableMapIndexInner;
use super::MapIndexKey;

pub(super) mod inner;
mod lifecycle;
pub mod read_only;
mod read_ops;

pub struct MutableMapIndex<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(super) inner: MutableMapIndexInner<N>,
    pub(super) storage: Gridstore<Vec<<N as MapIndexKey>::Owned>>,
}
