use std::fs::{create_dir_all, remove_dir};
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use memmap2::MmapMut;
use memory::madvise::AdviceSetting;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use memory::mmap_type::{MmapBitSlice, MmapSlice};
use serde::{Deserialize, Serialize};

use super::mutable_geo_index::InMemoryGeoMapIndex;
use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::index::field_index::geo_hash::GeoHash;
use crate::index::field_index::mmap_point_to_values::MmapPointToValues;
use crate::types::GeoPoint;

const DELETED_PATH: &str = "deleted.bin";
const COUNTS_PER_HASH: &str = "counts_per_hash.bin";
const POINTS_MAP: &str = "points_map.bin";
const POINTS_MAP_IDS: &str = "points_map_ids.bin";
const STATS_PATH: &str = "mmap_field_index_stats.json";

#[repr(C)]
#[derive(Clone, Debug)]
struct Counts {
    hash: GeoHash,
    points: u32,
    values: u32,
}

#[repr(C)]
#[derive(Clone, Debug)]
struct PointKeyValue {
    hash: GeoHash,
    ids_start: u32,
    ids_end: u32,
}

///
///   points_map
///
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
pub struct MmapGeoMapIndex {
    path: PathBuf,
    /// Stores GeoHash, points count and values count.
    /// Sorted by geohash, so we binary search the region.
    counts_per_hash: MmapSlice<Counts>,
    /// Stores GeoHash and associated range of offsets in the points_map_ids.
    /// Sorted by geohash, so we binary search the region.
    points_map: MmapSlice<PointKeyValue>,
    /// A storage of associations between geo-hashes and point ids. (See the diagram above)
    points_map_ids: MmapSlice<PointOffsetType>,
    /// One-to-many mapping of the PointOffsetType to the GeoPoint.
    point_to_values: MmapPointToValues<GeoPoint>,
    /// Deleted flags for each PointOffsetType
    deleted: MmapBitSliceBufferedUpdateWrapper,
    deleted_count: usize,
    points_values_count: usize,
    max_values_per_point: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MmapGeoMapIndexStat {
    points_values_count: usize,
    max_values_per_point: usize,
}

impl MmapGeoMapIndex {
    pub fn new(dynamic_index: InMemoryGeoMapIndex, path: &Path) -> OperationResult<Self> {
        create_dir_all(path)?;

        let deleted_path = path.join(DELETED_PATH);
        let stats_path = path.join(STATS_PATH);
        let counts_per_hash_path = path.join(COUNTS_PER_HASH);
        let points_map_path = path.join(POINTS_MAP);
        let points_map_ids_path = path.join(POINTS_MAP_IDS);

        // Create the point-to-value mapping and persist in the mmap file
        MmapPointToValues::<GeoPoint>::from_iter(
            path,
            dynamic_index
                .point_to_values
                .iter()
                .enumerate()
                .map(|(idx, values)| (idx as PointOffsetType, values.iter().cloned())),
        )?;

        {
            let points_map_file = create_and_ensure_length(
                &points_map_path,
                dynamic_index.points_map.len() * std::mem::size_of::<PointKeyValue>(),
            )?;
            let points_map_file = unsafe { MmapMut::map_mut(&points_map_file)? };
            let mut points_map = unsafe { MmapSlice::<PointKeyValue>::try_from(points_map_file)? };

            let points_map_ids_file = create_and_ensure_length(
                &points_map_ids_path,
                dynamic_index
                    .points_map
                    .values()
                    .map(|v| v.len())
                    .sum::<usize>()
                    * std::mem::size_of::<PointOffsetType>(),
            )?;
            let points_map_ids_file = unsafe { MmapMut::map_mut(&points_map_ids_file)? };
            let mut points_map_ids =
                unsafe { MmapSlice::<PointOffsetType>::try_from(points_map_ids_file)? };

            let mut ids_offset = 0;
            for (i, (hash, ids)) in dynamic_index.points_map.iter().enumerate() {
                points_map[i].hash = *hash;
                points_map[i].ids_start = ids_offset as u32;
                points_map[i].ids_end = (ids_offset + ids.len()) as u32;
                points_map_ids[ids_offset..ids_offset + ids.len()].copy_from_slice(
                    &ids.iter()
                        .map(|v| *v as PointOffsetType)
                        .collect::<Vec<_>>(),
                );
                ids_offset += ids.len();
            }
        }

        {
            let counts_per_hash_file = create_and_ensure_length(
                &counts_per_hash_path,
                std::cmp::min(
                    dynamic_index.points_per_hash.len(),
                    dynamic_index.values_per_hash.len(),
                ) * std::mem::size_of::<Counts>(),
            )?;
            let counts_per_hash_file = unsafe { MmapMut::map_mut(&counts_per_hash_file)? };
            let mut counts_per_hash =
                unsafe { MmapSlice::<Counts>::try_from(counts_per_hash_file)? };
            for ((hash, points), dst) in dynamic_index
                .points_per_hash
                .iter()
                .zip(counts_per_hash.iter_mut())
            {
                if let Some(values) = dynamic_index.values_per_hash.get(hash) {
                    dst.hash = *hash;
                    dst.points = *points as u32;
                    dst.values = *values as u32;
                }
            }
        }

        {
            let deleted_flags_count = dynamic_index.point_to_values.len();
            let deleted_file = create_and_ensure_length(
                &deleted_path,
                deleted_flags_count
                    .div_ceil(u8::BITS as usize)
                    .next_multiple_of(std::mem::size_of::<usize>()),
            )?;
            let mut deleted_mmap = unsafe { MmapMut::map_mut(&deleted_file)? };
            deleted_mmap.fill(0);
            let mut deleted_bitflags = MmapBitSlice::from(deleted_mmap, 0);
            for (idx, values) in dynamic_index.point_to_values.iter().enumerate() {
                if values.is_empty() {
                    deleted_bitflags.set(idx, true);
                }
            }
        }

        atomic_save_json(
            &stats_path,
            &MmapGeoMapIndexStat {
                points_values_count: dynamic_index.points_values_count,
                max_values_per_point: dynamic_index.max_values_per_point,
            },
        )?;

        Self::load(path)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        let deleted_path = path.join(DELETED_PATH);
        let stats_path = path.join(STATS_PATH);
        let counts_per_hash_path = path.join(COUNTS_PER_HASH);
        let points_map_path = path.join(POINTS_MAP);
        let points_map_ids_path = path.join(POINTS_MAP_IDS);

        let stats: MmapGeoMapIndexStat = read_json(&stats_path)?;
        let counts_per_hash = unsafe {
            MmapSlice::try_from(open_write_mmap(
                &counts_per_hash_path,
                AdviceSetting::Global,
                false,
            )?)?
        };
        let points_map = unsafe {
            MmapSlice::try_from(open_write_mmap(
                &points_map_path,
                AdviceSetting::Global,
                false,
            )?)?
        };
        let points_map_ids = unsafe {
            MmapSlice::try_from(open_write_mmap(
                &points_map_ids_path,
                AdviceSetting::Global,
                false,
            )?)?
        };
        let point_to_values = MmapPointToValues::open(path)?;

        let deleted = open_write_mmap(&deleted_path, AdviceSetting::Global, false)?;
        let deleted = MmapBitSlice::from(deleted, 0);
        let deleted_count = deleted.count_ones();

        Ok(Self {
            path: path.to_owned(),
            counts_per_hash,
            points_map,
            points_map_ids,
            point_to_values,
            deleted: MmapBitSliceBufferedUpdateWrapper::new(deleted),
            deleted_count,
            points_values_count: stats.points_values_count,
            max_values_per_point: stats.max_values_per_point,
        })
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&GeoPoint) -> bool,
    ) -> bool {
        self.deleted
            .get(idx as usize)
            .filter(|b| !b)
            .map(|_| self.point_to_values.check_values_any(idx, |v| check_fn(&v)))
            .unwrap_or(false)
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        self.deleted
            .get(idx as usize)
            .filter(|b| !b)
            .and_then(|_| self.point_to_values.get_values_count(idx))
            .unwrap_or(0)
    }

    pub fn points_per_hash(&self) -> impl Iterator<Item = (GeoHash, usize)> + '_ {
        self.counts_per_hash
            .iter()
            .map(|counts| (counts.hash, counts.points as usize))
    }

    pub fn points_of_hash(&self, hash: &GeoHash) -> usize {
        if let Ok(index) = self.counts_per_hash.binary_search_by(|x| x.hash.cmp(hash)) {
            self.counts_per_hash[index].points as usize
        } else {
            0
        }
    }

    pub fn values_of_hash(&self, hash: &GeoHash) -> usize {
        if let Ok(index) = self.counts_per_hash.binary_search_by(|x| x.hash.cmp(hash)) {
            self.counts_per_hash[index].values as usize
        } else {
            0
        }
    }

    pub fn clear(self) -> OperationResult<()> {
        let files = self.files();
        let Self { path, .. } = self;
        for file in files {
            std::fs::remove_file(file)?;
        }
        let _ = remove_dir(path);
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(DELETED_PATH),
            self.path.join(COUNTS_PER_HASH),
            self.path.join(POINTS_MAP),
            self.path.join(POINTS_MAP_IDS),
            self.path.join(STATS_PATH),
        ];
        files.extend(self.point_to_values.files());
        files
    }

    pub fn flusher(&self) -> Flusher {
        self.deleted.flusher()
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) {
        let idx = idx as usize;
        if let Some(deleted) = self.deleted.get(idx) {
            if !deleted {
                self.deleted.set(idx, true);
                self.deleted_count += 1;
            }
        }
    }

    /// Returns an iterator over all point IDs which have the `geohash` prefix.
    /// Note. Point ID may be repeated multiple times in the iterator.
    pub fn stored_sub_regions(
        &self,
        geohash: GeoHash,
    ) -> impl Iterator<Item = PointOffsetType> + '_ {
        let start_index = self
            .points_map
            .binary_search_by(|point_key_value| point_key_value.hash.cmp(&geohash))
            .unwrap_or_else(|index| index);
        self.points_map[start_index..]
            .iter()
            .take_while(move |point_key_value| point_key_value.hash.starts_with(geohash))
            .filter_map(|point_key_value| {
                Some(
                    self.points_map_ids
                        .get(point_key_value.ids_start as usize..point_key_value.ids_end as usize)?
                        .iter()
                        .copied()
                        .filter(|idx| !self.deleted.get(*idx as usize).unwrap_or(true)),
                )
            })
            .flatten()
    }

    pub fn points_count(&self) -> usize {
        self.point_to_values
            .len()
            .saturating_sub(self.deleted_count)
    }

    pub fn points_values_count(&self) -> usize {
        self.points_values_count
    }

    pub fn max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }
}
