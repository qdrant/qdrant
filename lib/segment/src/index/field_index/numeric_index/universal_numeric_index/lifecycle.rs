use std::borrow::Borrow;
use std::ops::BitOrAssign;
use std::path::{Path, PathBuf};

use common::bitvec::{BitSlice, BitSliceExt};
use common::fs::{atomic_save_json, clear_disk_cache};
use common::mmap::{AdviceSetting, MmapSlice, create_and_ensure_length};
use common::stored_bitslice::{MmapBitSlice, StoredBitSlice};
use common::types::PointOffsetType;
use common::universal_io::{
    MmapFs, OkNotFound, OpenOptions, Populate, TypedStorage, UniversalRead, read_json_via,
};
use fs_err as fs;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};

use super::super::Encodable;
use super::super::mutable_numeric_index::InMemoryNumericIndex;
use super::{CONFIG_PATH, DELETED_PATH, PAIRS_PATH, Storage, UniversalNumericIndex};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stored_point_to_values::{StoredPointToValues, StoredValue};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UniversalNumericIndexConfig {
    max_values_per_point: usize,
}

impl<T, S> UniversalNumericIndex<T, S>
where
    T: Encodable + Numericable + Default + StoredValue + bytemuck::Pod,
    S: UniversalRead,
{
    /// TODO: save using `S::Fs` too
    pub fn build(
        fs: &S::Fs,
        in_memory_index: InMemoryNumericIndex<T>,
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Self> {
        fs::create_dir_all(path)?;

        let pairs_path = path.join(PAIRS_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        atomic_save_json(
            &config_path,
            &UniversalNumericIndexConfig {
                max_values_per_point: in_memory_index.max_values_per_point,
            },
        )?;

        in_memory_index.histogram.save(path)?;

        StoredPointToValues::<T, S>::from_iter(
            fs,
            path,
            in_memory_index
                .point_to_values
                .iter()
                .enumerate()
                .map(|(idx, values)| (idx as PointOffsetType, values.iter().map(|v| v.borrow()))),
        )?;

        {
            let pairs_file = create_and_ensure_length(
                &pairs_path,
                in_memory_index.map.len() * size_of::<Point<T>>(),
            )?;
            let pairs_mmap = unsafe { MmapMut::map_mut(&pairs_file)? };
            let mut pairs = unsafe { MmapSlice::<Point<T>>::try_from(pairs_mmap)? };
            for (src, dst) in in_memory_index.map.iter().zip(pairs.iter_mut()) {
                *dst = *src;
            }
        }

        {
            let deleted_flags_count = in_memory_index.point_to_values.len();
            let _ = create_and_ensure_length(
                &deleted_path,
                deleted_flags_count
                    .div_ceil(u8::BITS as usize)
                    .next_multiple_of(size_of::<u64>()),
            )?;

            let mut deleted = MmapBitSlice::open(
                &MmapFs,
                &deleted_path,
                OpenOptions {
                    writeable: true,
                    need_sequential: false,
                    populate: Populate::Auto,
                    advice: AdviceSetting::Global,
                },
                (),
            )?;
            deleted.set_ascending_bits_batch(
                in_memory_index
                    .point_to_values
                    .iter()
                    .enumerate()
                    .filter(|(_, values)| values.is_empty())
                    .map(|(idx, _)| (idx as u64, true)),
            )?;
            deleted.flusher()()?;
        }

        Self::open(fs, path, is_on_disk, deleted_points)?.ok_or_else(|| {
            OperationError::service_error("Failed to open UniversalNumericIndex after building it")
        })
    }

    /// Open and load mmap numeric index from the given path
    pub fn open(
        fs: &S::Fs,
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let pairs_path = path.join(PAIRS_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        let Some(config) =
            read_json_via::<_, UniversalNumericIndexConfig>(fs, &config_path).ok_not_found()?
        else {
            // If config doesn't exist, assume the index doesn't exist on disk
            return Ok(None);
        };

        let histogram = Histogram::<T>::load_via(fs, path)?;
        let do_populate = !is_on_disk;

        let pairs_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::from(do_populate),
            advice: AdviceSetting::Global,
        };
        let pairs = TypedStorage::open(fs, pairs_path, pairs_options, Default::default())?;

        let point_to_values = StoredPointToValues::open(fs, path, do_populate)?;
        let mut deleted = deleted_points.to_owned();

        let deleted_payload_mmap = StoredBitSlice::<S>::open(
            fs,
            &deleted_path,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Auto,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;
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
                deleted,
                pairs,
                point_to_values,
            },
            histogram,
            deleted_count,
            max_values_per_point: config.max_values_per_point,
            is_on_disk,
        }))
    }
}

impl<T: Encodable + Numericable + Default + StoredValue + 'static> UniversalNumericIndex<T> {
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
            self.path.join(PAIRS_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.storage.point_to_values.files());
        files.extend(Histogram::<T>::files(&self.path));
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(PAIRS_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.storage.point_to_values.immutable_files());
        files.extend(Histogram::<T>::immutable_files(&self.path));
        files
    }

    /// No-op flusher: the on-disk state is build-time only. See the type-level
    /// docs on [`UniversalNumericIndex`] for the deletion durability contract.
    pub fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
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
        self.storage.pairs.populate()?;
        self.storage.point_to_values.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            path,
            storage,
            histogram: _,
            deleted_count: _,
            max_values_per_point: _,
            is_on_disk: _,
        } = self;
        let Storage {
            deleted: _,
            pairs,
            point_to_values,
        } = storage;
        pairs.clear_ram_cache()?;
        clear_disk_cache(&path.join(DELETED_PATH))?;
        point_to_values.clear_cache()?;
        Ok(())
    }

    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            path: _,
            storage,
            histogram,
            deleted_count: _,
            max_values_per_point: _,
            is_on_disk: _,
        } = self;

        histogram.ram_usage_bytes() + storage.ram_usage_bytes()
    }
}
