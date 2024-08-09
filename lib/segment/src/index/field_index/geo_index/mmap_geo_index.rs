use std::fs::{create_dir_all, remove_dir};
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use memmap2::MmapMut;
use memory::mmap_ops::{self, create_and_ensure_length};
use serde::{Deserialize, Serialize};

use super::mutable_geo_index::DynamicGeoMapIndex;
use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::mmap_type::{MmapBitSlice, MmapSlice};
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::index::field_index::geo_hash::{GeoHash, GeoHashRef, GEOHASH_MAX_LENGTH};
use crate::index::field_index::mmap_point_to_values::MmapPointToValues;
use crate::types::GeoPoint;

const DELETED_PATH: &str = "deleted.bin";
const COUNTS_PER_HASH: &str = "counts_per_hash.bin";
const POINTS_MAP: &str = "points_map.bin";
const POINTS_MAP_IDS: &str = "points_map_ids.bin";
const CONFIG_PATH: &str = "mmap_field_index_config.json";

type MmapGeoHash = [u8; GEOHASH_MAX_LENGTH + 1];

#[repr(C)]
#[derive(Clone, Debug)]
struct Counts {
    hash: MmapGeoHash,
    points: u32,
    values: u32,
}

#[repr(C)]
#[derive(Clone, Debug)]
struct PointKeyValue {
    hash: MmapGeoHash,
    ids_start: u32,
    ids_end: u32,
}

pub struct MmapGeoMapIndex {
    path: PathBuf,
    counts_per_hash: MmapSlice<Counts>,
    points_map: MmapSlice<PointKeyValue>,
    points_map_ids: MmapSlice<PointOffsetType>,
    point_to_values: MmapPointToValues<GeoPoint>,
    deleted: MmapBitSliceBufferedUpdateWrapper,
    deleted_count: usize,
    points_values_count: usize,
    max_values_per_point: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MmapGeoMapIndexConfig {
    points_values_count: usize,
    max_values_per_point: usize,
}

impl MmapGeoMapIndex {
    pub fn new(dynamic_index: DynamicGeoMapIndex, path: &Path) -> OperationResult<Self> {
        create_dir_all(path)?;

        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);
        let counts_per_hash_path = path.join(COUNTS_PER_HASH);
        let points_map_path = path.join(POINTS_MAP);
        let points_map_ids_path = path.join(POINTS_MAP_IDS);

        atomic_save_json(
            &config_path,
            &MmapGeoMapIndexConfig {
                points_values_count: dynamic_index.points_values_count,
                max_values_per_point: dynamic_index.max_values_per_point,
            },
        )?;

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
                points_map[i].hash = into_mmap_hash(hash);
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
                    dst.hash = into_mmap_hash(hash);
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

        Self::load(path)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);
        let counts_per_hash_path = path.join(COUNTS_PER_HASH);
        let points_map_path = path.join(POINTS_MAP);
        let points_map_ids_path = path.join(POINTS_MAP_IDS);

        let config: MmapGeoMapIndexConfig = read_json(&config_path)?;
        let counts_per_hash =
            unsafe { MmapSlice::try_from(mmap_ops::open_write_mmap(&counts_per_hash_path)?)? };
        let points_map =
            unsafe { MmapSlice::try_from(mmap_ops::open_write_mmap(&points_map_path)?)? };
        let points_map_ids =
            unsafe { MmapSlice::try_from(mmap_ops::open_write_mmap(&points_map_ids_path)?)? };
        let point_to_values = MmapPointToValues::open(path)?;

        let deleted = mmap_ops::open_write_mmap(&deleted_path)?;
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
            points_values_count: config.points_values_count,
            max_values_per_point: config.max_values_per_point,
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

    pub fn get_points_per_hash(&self) -> impl Iterator<Item = (GeoHashRef, usize)> {
        self.counts_per_hash
            .iter()
            .filter_map(|counts| Some((from_mmap_hash(&counts.hash)?, counts.points as usize)))
    }

    pub fn get_points_of_hash(&self, hash: &GeoHash) -> usize {
        let hash = into_mmap_hash(hash);
        if let Ok(index) = self
            .counts_per_hash
            .binary_search_by(|x| mmap_geo_hash_cmp(&x.hash, &hash))
        {
            self.counts_per_hash[index].points as usize
        } else {
            0
        }
    }

    pub fn get_values_of_hash(&self, hash: &GeoHash) -> usize {
        let hash = into_mmap_hash(hash);
        if let Ok(index) = self
            .counts_per_hash
            .binary_search_by(|x| mmap_geo_hash_cmp(&x.hash, &hash))
        {
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
        todo!()
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

    pub fn get_stored_sub_regions(
        &self,
        geo: &GeoHash,
    ) -> impl Iterator<Item = (GeoHashRef, impl Iterator<Item = PointOffsetType> + '_)> + '_ {
        let mmap_geo = into_mmap_hash(geo);
        let start_index = self
            .points_map
            .binary_search_by(|point_key_value| mmap_geo_hash_cmp(&point_key_value.hash, &mmap_geo))
            .unwrap_or_else(|index| index);
        let geo_clone = geo.clone();
        self.points_map[start_index..]
            .iter()
            .take_while(move |point_key_value| {
                from_mmap_hash(&point_key_value.hash)
                    .map(|hash| hash.starts_with(geo_clone.as_str()))
                    .unwrap_or(false)
            })
            .filter_map(|point_key_value| {
                Some((
                    from_mmap_hash(&point_key_value.hash)?,
                    self.points_map_ids
                        .get(point_key_value.ids_start as usize..point_key_value.ids_end as usize)?
                        .iter()
                        .cloned()
                        .filter(|idx| !self.deleted.get(*idx as usize).unwrap_or(true)),
                ))
            })
    }

    pub fn get_indexed_points(&self) -> usize {
        self.point_to_values
            .len()
            .saturating_sub(self.deleted_count)
    }

    pub fn get_points_values_count(&self) -> usize {
        self.points_values_count
    }

    pub fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }
}

fn mmap_geo_hash_cmp(a: &MmapGeoHash, b: &MmapGeoHash) -> std::cmp::Ordering {
    from_mmap_hash(a).cmp(&from_mmap_hash(b))
}

fn from_mmap_hash(hash: &MmapGeoHash) -> Option<GeoHashRef> {
    let len = hash[0] as usize;
    std::str::from_utf8(&hash[1..=len]).ok()
}

fn into_mmap_hash(hash: &GeoHash) -> MmapGeoHash {
    let len = hash.len();
    let mut mmap_hash = [0; GEOHASH_MAX_LENGTH + 1];
    mmap_hash[0] = len as u8;
    mmap_hash[1..=len].copy_from_slice(hash.as_bytes());
    mmap_hash
}
