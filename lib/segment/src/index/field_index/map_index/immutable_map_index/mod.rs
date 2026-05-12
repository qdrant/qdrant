use std::collections::HashMap;
use std::ops::Range;

use bitvec::vec::BitVec;
use common::persisted_hashmap::Key;
use common::types::PointOffsetType;

use super::MapIndexKey;
use super::mmap_map_index::MmapMapIndex;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;

mod lifecycle;
mod read_ops;

pub struct ImmutableMapIndex<N: MapIndexKey + Key + ?Sized> {
    pub(super) value_to_points: HashMap<<N as MapIndexKey>::Owned, ContainerSegment>,
    /// Container holding a slice of point IDs per value. `value_to_point` holds the range per value.
    /// Each slice MUST be sorted so that we can binary search over it.
    pub(super) value_to_points_container: Vec<PointOffsetType>,
    pub(super) deleted_value_to_points_container: BitVec,
    pub(super) point_to_values: ImmutablePointToValues<<N as MapIndexKey>::Owned>,
    /// Amount of point which have at least one indexed payload value
    pub(super) indexed_points: usize,
    pub(super) values_count: usize,
    // Backing storage, source of state, persists deletions
    pub(super) storage: Storage<N>,
    /// Snapshot of approximate RAM usage at construction time.
    /// Not refreshed on `remove_point`.
    pub(super) cached_ram_usage_bytes: usize,
}

pub(super) enum Storage<N: MapIndexKey + Key + ?Sized> {
    Mmap(Box<MmapMapIndex<N>>),
}

pub(super) struct ContainerSegment {
    /// Range in the container which holds point IDs for the value.
    range: Range<u32>,
    /// Number of available point IDs in the range, excludes number of deleted points.
    count: u32,
}
