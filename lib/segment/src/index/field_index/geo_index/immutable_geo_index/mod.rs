use common::types::PointOffsetType;
use common::universal_io::{MmapFile, UniversalRead};

use super::on_disk_geo_index::OnDiskGeoIndex;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::types::GeoPoint;

mod lifecycle;
mod live_reload;
mod read_ops;

pub(super) const DELETED_SENTINEL: PointOffsetType = PointOffsetType::MAX;

#[derive(Copy, Clone, Debug)]
pub(super) struct Counts {
    pub(super) hash: GeoHash,
    pub(super) points: u32,
    pub(super) values: u32,
}

impl From<super::on_disk_geo_index::Counts> for Counts {
    #[inline]
    fn from(counts: super::on_disk_geo_index::Counts) -> Self {
        let super::on_disk_geo_index::Counts {
            hash,
            points,
            values,
        } = counts;
        Self {
            hash: hash.normalize(),
            points,
            values,
        }
    }
}

/// RAM-loaded immutable geo index using flat parallel arrays instead of per-hash
/// `AHashSet`s. This dramatically reduces memory overhead:
///
/// - `points_map_hashes[i]` is the sorted geohash for the i-th entry.
/// - `points_map_offsets[i]..points_map_offsets[i+1]` is the range in
///   `points_map_ids` holding the point IDs for that hash.
/// - Deleted entries in `points_map_ids` are marked with `DELETED_SENTINEL`.
pub struct ImmutableGeoIndex<S: UniversalRead = MmapFile> {
    pub(super) counts_per_hash: Vec<Counts>,
    pub(super) points_map_hashes: Vec<GeoHash>,
    pub(super) points_map_offsets: Vec<u32>,
    pub(super) points_map_ids: Vec<PointOffsetType>,
    pub(super) point_to_values: ImmutablePointToValues<GeoPoint>,
    pub(super) points_count: usize,
    pub(super) points_values_count: usize,
    pub(super) max_values_per_point: usize,
    pub(super) storage: OnDiskGeoIndex<S>,
    pub(super) cached_ram_usage_bytes: usize,
}
