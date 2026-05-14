use std::path::PathBuf;

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, TypedStorage, UniversalRead};
use serde::{Deserialize, Serialize};

use crate::index::field_index::geo_hash::GeoHashRaw;
use crate::index::field_index::stored_point_to_values::StoredPointToValues;
use crate::types::GeoPoint;

mod lifecycle;
mod read_ops;

pub(super) const DELETED_PATH: &str = "deleted.bin";
pub(super) const COUNTS_PER_HASH: &str = "counts_per_hash.bin";
pub(super) const POINTS_MAP: &str = "points_map.bin";
pub(super) const POINTS_MAP_IDS: &str = "points_map_ids.bin";
pub(super) const STATS_PATH: &str = "mmap_field_index_stats.json";

#[repr(C)]
#[derive(Copy, Clone, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub(in super::super) struct Counts {
    pub hash: GeoHashRaw,
    pub points: u32,
    pub values: u32,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub(in super::super) struct PointKeyValue {
    pub hash: GeoHashRaw,
    pub ids_start: u32,
    pub ids_end: u32,
}

///
///   points_map
///  ┌─────────────────────────────────────────┐
///  │ (ABC, 10, 20)|(ABD, 20, 40)             │
///  └────────┬──┬──────────┬───┬──────────────┘
///           │  │          │   │
///    ┌──────┘  └────────┐ │   └───────────────────┐
///    │                  │ └───┐                   │
///    │                  │     │                   │
///  ┌─▼──────────────────▼─────▼───────────────────▼──────────┐
///  │ 1, 8, 10, 18, 129, 213, 12, 13, 14, 87, 99, 199         │
///  └─────────────────────────────────────────────────────────┘
///   points_map_ids
///
/// Mmap-backed immutable geo index.
///
/// On-disk state (`counts_per_hash.bin`, `points_map.bin`, `points_map_ids.bin`,
/// `deleted.bin`, `point_to_values.*`, etc.) is written once during
/// [`Self::build`] and not mutated afterwards: `deleted.bin` records only the
/// points whose payload was empty at build time.
///
/// Runtime deletions live in the in-memory `Storage::deleted` bitvec. They are
/// **not persisted** — [`Self::flusher`] is a no-op and [`Self::remove_point`]
/// only updates the in-memory bitvec. Callers must re-supply the authoritative
/// deletion set (typically `id_tracker.deleted_point_bitslice()`) via the
/// `deleted_points` argument to [`Self::open`] on reload.
pub struct StoredGeoMapIndex<S: UniversalRead = MmapFile> {
    pub(super) path: PathBuf,
    pub(in super::super) storage: Storage<S>,
    pub(in super::super) deleted_count: usize,
    pub(super) points_values_count: usize,
    pub(super) max_values_per_point: usize,
    pub(super) is_on_disk: bool,
}

pub(in super::super) struct Storage<S: UniversalRead = MmapFile> {
    /// Stores GeoHash, points count and values count.
    /// Sorted by geohash, so we binary search the region.
    pub(in super::super) counts_per_hash: TypedStorage<S, Counts>,
    /// Stores GeoHash and associated range of offsets in the points_map_ids.
    /// Sorted by geohash, so we binary search the region.
    pub(in super::super) points_map: TypedStorage<S, PointKeyValue>,
    /// A storage of associations between geo-hashes and point ids. (See the diagram above)
    pub(in super::super) points_map_ids: TypedStorage<S, PointOffsetType>,
    /// One-to-many mapping of the PointOffsetType to the GeoPoint.
    pub(in super::super) point_to_values: StoredPointToValues<GeoPoint, S>,
    /// In-memory deletion bitmap. Reconstructed at load time as the union of
    /// the build-time empty-payload bits read from `deleted.bin` and the
    /// segment-level deleted bitslice supplied by the id-tracker. Not persisted.
    pub(in super::super) deleted: BitVec,
}

impl<S: UniversalRead> Storage<S> {
    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            counts_per_hash,
            points_map,
            points_map_ids,
            point_to_values,
            deleted,
        } = self;

        counts_per_hash.ram_usage_bytes()
            + points_map.ram_usage_bytes()
            + points_map_ids.ram_usage_bytes()
            + point_to_values.ram_usage_bytes()
            + deleted.capacity().div_ceil(u8::BITS as usize)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredGeoMapIndexStat {
    pub(super) points_values_count: usize,
    pub(super) max_values_per_point: usize,
}
