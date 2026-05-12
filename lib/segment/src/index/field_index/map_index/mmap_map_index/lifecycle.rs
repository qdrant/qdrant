use std::borrow::Borrow;
use std::ops::BitOrAssign;
use std::path::{Path, PathBuf};

use ahash::HashMap;
use common::bitvec::{BitSlice, BitSliceExt};
use common::fs::{atomic_save_json, clear_disk_cache, read_json};
use common::mmap::create_and_ensure_length;
use common::persisted_hashmap::{Key, UniversalHashMap, serialize_hashmap};
use common::stored_bitslice::MmapBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, OpenOptions};
use fs_err as fs;

use super::super::MapIndexKey;
use super::{CONFIG_PATH, DELETED_PATH, HASHMAP_PATH, MmapMapIndex, MmapMapIndexConfig, Storage};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::stored_point_to_values::StoredPointToValues;

impl<N: MapIndexKey + Key + ?Sized> MmapMapIndex<N> {
    /// Open and load mmap map index from the given path
    pub fn open(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let hashmap_path = path.join(HASHMAP_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        // If config doesn't exist, assume the index doesn't exist on disk
        if !config_path.is_file() {
            return Ok(None);
        }

        let config: MmapMapIndexConfig = read_json(&config_path)?;

        let do_populate = !is_on_disk;

        let value_to_points = UniversalHashMap::open(
            &hashmap_path,
            OpenOptions {
                writeable: false,
                populate: Some(do_populate),
                ..OpenOptions::default()
            },
        )?;
        let point_to_values = StoredPointToValues::open(path, do_populate)?;

        let mut deleted = deleted_points.to_owned();

        let deleted_payload_mmap = MmapBitSlice::open(&deleted_path, OpenOptions::default())?;
        let deleted_payloads_bitslice = deleted_payload_mmap.read_all()?;

        // `deleted` length must match `point_to_values.len()` because it only
        // tracks the index's contents. The id-tracker's deleted mask can be
        // shorter or longer; if shorter, the missing entries default to live
        // (the id-tracker is the source of truth for deletions, and a shorter
        // mask just means it doesn't yet know about those higher offsets).
        deleted.resize(point_to_values.len(), false);
        deleted.bitor_assign(deleted_payloads_bitslice.as_ref());

        let deleted_count = deleted.count_ones();

        Ok(Some(Self {
            path: path.to_path_buf(),
            storage: Storage {
                value_to_points,
                point_to_values,
                deleted,
            },
            deleted_count,
            total_key_value_pairs: config.total_key_value_pairs,
            is_on_disk,
        }))
    }

    pub fn build(
        path: &Path,
        point_to_values: Vec<Vec<<N as MapIndexKey>::Owned>>,
        values_to_points: HashMap<<N as MapIndexKey>::Owned, Vec<PointOffsetType>>,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Self> {
        fs::create_dir_all(path)?;

        let hashmap_path = path.join(HASHMAP_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        atomic_save_json(
            &config_path,
            &MmapMapIndexConfig {
                total_key_value_pairs: point_to_values.iter().map(|v| v.len()).sum(),
            },
        )?;

        serialize_hashmap(
            &hashmap_path,
            values_to_points
                .iter()
                .map(|(value, ids)| (value.borrow(), ids.iter().copied())),
        )?;

        StoredPointToValues::<N, MmapFile>::from_iter(
            path,
            point_to_values.iter().enumerate().map(|(idx, values)| {
                (
                    idx as PointOffsetType,
                    values.iter().map(|value| value.borrow()),
                )
            }),
        )?;

        {
            let deleted_flags_count = point_to_values.len();
            let _ = create_and_ensure_length(
                &deleted_path,
                deleted_flags_count
                    .div_ceil(u8::BITS as usize)
                    .next_multiple_of(size_of::<u64>()),
            )?;

            let mut deleted = MmapBitSlice::open(&deleted_path, OpenOptions::default())?;
            deleted.set_ascending_bits_batch(
                point_to_values
                    .iter()
                    .enumerate()
                    .filter(|(_, values)| values.is_empty())
                    .map(|(idx, _)| (idx as u64, true)),
            )?;
            deleted.flusher()()?;
        }

        Self::open(path, is_on_disk, deleted_points)?.ok_or_else(|| {
            OperationError::service_error("Failed to open MmapMapIndex after building it")
        })
    }

    /// No-op flusher: the on-disk state is build-time only. See the type-level
    /// docs on [`MmapMapIndex`] for the deletion durability contract.
    pub fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    pub fn wipe(self) -> OperationResult<()> {
        let files = self.files();
        let path = self.path.clone();
        // drop mmap handles before deleting files
        drop(self);
        for file in files {
            fs::remove_file(file)?;
        }
        let _ = fs::remove_dir(path);
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(HASHMAP_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.storage.point_to_values.files());
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(HASHMAP_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.storage.point_to_values.immutable_files());
        files
    }

    /// Marks `idx` as deleted in the in-memory deletion bitvec.
    ///
    /// Not persisted: on reopen, deletions must be re-supplied via the
    /// `deleted_points` argument to [`Self::open`].
    pub fn remove_point(&mut self, idx: PointOffsetType) {
        let idx = idx as usize;
        if idx < self.storage.deleted.len() && !self.storage.deleted.get_bit(idx).unwrap_or(true) {
            self.storage.deleted.set(idx, true);
            self.deleted_count += 1;
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.storage.value_to_points.populate()?;
        self.storage.point_to_values.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            path,
            storage,
            deleted_count: _,
            total_key_value_pairs: _,
            is_on_disk: _,
        } = self;
        let Storage {
            value_to_points,
            point_to_values,
            deleted: _,
        } = storage;
        value_to_points.clear_ram_cache()?;
        clear_disk_cache(&path.join(DELETED_PATH))?;
        point_to_values.clear_cache()?;
        Ok(())
    }

    pub(crate) fn ram_usage_bytes(&self) -> usize {
        self.storage.ram_usage_bytes()
    }
}
