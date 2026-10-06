mod deleted_storage;
mod mappings_storage;
mod versions_storage;

#[cfg(test)]
pub(super) mod tests;

pub mod read_only;
pub mod update_only;

use std::fmt::Debug;
use std::io::{BufReader, BufWriter, Write};
use std::mem::{size_of, size_of_val};
use std::path::{Path, PathBuf};

use common::bitvec::{BitSlice, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::clear_disk_cache;
use common::mmap::{AdviceSetting, create_and_ensure_length};
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    OpenOptions, Populate, SliceBufferedUpdateWrapper, TypedStorage, UniversalWrite,
};
use fs_err::File;

pub use self::deleted_storage::DELETED_FILE_NAME;
pub(crate) use self::deleted_storage::{deleted_path, tombstone_points_in_stored_mask};
pub use self::mappings_storage::{MAPPINGS_FILE_NAME, mappings_path};
use self::mappings_storage::{load_mapping, store_mapping};
pub use self::versions_storage::VERSION_MAPPING_FILE_NAME;
use self::versions_storage::mmap_size;
pub(crate) use self::versions_storage::version_mapping_path;
use crate::common::Flusher;
use crate::common::buffered_update_bitslice::BufferedUpdateBitSlice;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::{
    DELETED_POINT_VERSION, IdTracker, IdTrackerRead, PointMappingsRefEnum,
    default_external_ids_batch, default_internal_versions_batch,
};
use crate::types::{PointIdType, SeqNumberType};

#[derive(Debug)]
pub struct ImmutableIdTracker<S: UniversalWrite> {
    path: PathBuf,

    deleted_wrapper: BufferedUpdateBitSlice<S>,

    pub(super) internal_to_version: CompressedVersions,
    internal_to_version_wrapper: SliceBufferedUpdateWrapper<S, SeqNumberType>,

    pub(super) mappings: CompressedPointMappings,
}

impl<S> ImmutableIdTracker<S>
where
    S: UniversalWrite + Send + Sync + 'static,
{
    /// Approximate RAM usage in bytes for in-memory data structures.
    ///
    /// ImmutableIdTracker loads all mappings and versions into compressed
    /// in-memory structures. The mmap files are used for persistence but
    /// the working data lives in RAM.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            path: _,
            deleted_wrapper: _, // mmap-backed, accounted via files
            internal_to_version,
            internal_to_version_wrapper: _, // mmap-backed, accounted via files
            mappings,
        } = self;

        internal_to_version.ram_usage_bytes() + mappings.ram_usage_bytes()
    }

    pub fn from_in_memory_tracker(
        fs: &S::Fs,
        in_memory_tracker: InMemoryIdTracker,
        path: &Path,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let (internal_to_version, mappings) = in_memory_tracker.into_internal();
        let compressed_mappings = CompressedPointMappings::from_mappings(mappings);
        let id_tracker = Self::new(
            fs,
            path,
            &internal_to_version,
            compressed_mappings,
            hw_counter,
        )?;

        Ok(id_tracker)
    }

    pub fn open(
        fs: &S::Fs,
        segment_path: &Path,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        let deleted_storage = StoredBitSlice::open(
            fs,
            deleted_path(segment_path),
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Blocking,
                advice: AdviceSetting::Global,
            },
            Default::default(),
            hw_counter,
        )?;

        let mut deleted_bitvec = BitVec::new();
        deleted_bitvec.extend_from_bitslice(deleted_storage.read_all(hw_counter)?.as_ref());

        let deleted_wrapper = BufferedUpdateBitSlice::new(deleted_storage);

        let internal_to_version_file = TypedStorage::<S, SeqNumberType>::open(
            fs,
            version_mapping_path(segment_path),
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Blocking,
                advice: AdviceSetting::Global,
            },
            Default::default(),
            hw_counter,
        )?;

        let internal_to_version_slice = internal_to_version_file.read_whole(hw_counter)?;

        let internal_to_version = CompressedVersions::from_slice(&internal_to_version_slice);
        let internal_to_version_wrapper =
            SliceBufferedUpdateWrapper::new(internal_to_version_file.inner, hw_counter)?;

        let reader = BufReader::new(File::open(mappings_path(segment_path))?);
        let mappings = load_mapping(reader, Some(deleted_bitvec))?;

        Ok(Self {
            path: segment_path.to_path_buf(),
            deleted_wrapper,
            internal_to_version_wrapper,
            internal_to_version,
            mappings,
        })
    }

    pub fn new(
        fs: &S::Fs,
        path: &Path,
        internal_to_version: &[SeqNumberType],
        mappings: CompressedPointMappings,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Self> {
        // Create mmap file for deleted bitvec
        let deleted_filepath = deleted_path(path);

        debug_assert!(mappings.deleted().len() <= mappings.total_point_count());

        let _ = create_and_ensure_length(
            &deleted_filepath,
            mappings
                .total_point_count()
                .div_ceil(u8::BITS as usize)
                .next_multiple_of(size_of::<u64>()),
        )?;

        let mut deleted_storage = StoredBitSlice::open(
            fs,
            &deleted_filepath,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Auto,
                advice: AdviceSetting::Global,
            },
            Default::default(),
            hw_counter,
        )?;

        // Set bits for deleted points from the mappings,
        deleted_storage.write_bitslice(mappings.deleted(), hw_counter)?;
        // plus any trailing points beyond mappings.deleted().len() are also marked deleted.
        deleted_storage.set_ascending_bits_batch(
            (mappings.deleted().len()..mappings.total_point_count()).map(|i| (i as u64, true)),
            hw_counter,
        )?;

        deleted_storage.flusher()()?;

        let deleted_wrapper = BufferedUpdateBitSlice::new(deleted_storage);

        // Create mmap file for internal-to-version list
        let version_filepath = version_mapping_path(path);

        // Amount of points without version
        let missing_version_count = mappings
            .total_point_count()
            .saturating_sub(internal_to_version.len());

        let missing_versions_size = missing_version_count * size_of::<SeqNumberType>();
        let internal_to_version_size = size_of_val(internal_to_version);
        let min_size = internal_to_version_size + missing_versions_size;
        {
            let version_size = mmap_size::<SeqNumberType>(min_size);
            create_and_ensure_length(&version_filepath, version_size)?;
        }

        let mut internal_to_version_file = TypedStorage::<S, SeqNumberType>::open(
            fs,
            &version_filepath,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
            hw_counter,
        )?;
        internal_to_version_file.write(0, internal_to_version)?;

        let internal_to_version =
            CompressedVersions::from_slice(&internal_to_version_file.read_whole(hw_counter)?);

        debug_assert_eq!(internal_to_version.len(), mappings.total_point_count());

        let internal_to_version_wrapper =
            SliceBufferedUpdateWrapper::new(internal_to_version_file.inner, hw_counter)?;

        // Write mappings to disk.
        let file = File::create(mappings_path(path))?;
        let mut writer = BufWriter::new(file);
        store_mapping(&mappings, &mut writer)?;

        // Explicitly fsync file contents to ensure durability
        writer.flush()?;
        let file = writer.into_inner().unwrap();
        file.sync_all()?;

        deleted_wrapper.flusher(hw_counter)()?;
        internal_to_version_wrapper.flusher()()?;

        Ok(Self {
            path: path.to_path_buf(),
            deleted_wrapper,
            internal_to_version_wrapper,
            internal_to_version,
            mappings,
        })
    }
}

impl<S: UniversalWrite + Send + Sync + 'static> IdTrackerRead for ImmutableIdTracker<S> {
    fn internal_version(
        &self,
        internal_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id)
    }

    fn internal_versions_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, SeqNumberType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        default_internal_versions_batch(self, internal_ids, callback, hw_counter)
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        _deferred_behavior: common::types::DeferredBehavior,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<PointOffsetType> {
        // Immutable mappings never carry deferred heads; behavior is moot.
        self.mappings.internal_id(&external_id)
    }

    fn external_id(
        &self,
        internal_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<PointIdType> {
        self.mappings.external_id(internal_id)
    }

    fn external_ids_batch(
        &self,
        internal_ids: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, PointIdType),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        default_external_ids_batch(self, internal_ids, callback, hw_counter)
    }

    type Backend = S;

    fn point_mappings(
        &self,
        _hw_counter: &HardwareCounterCell,
    ) -> PointMappingsRefEnum<'_, Self::Backend> {
        PointMappingsRefEnum::Compressed(&self.mappings)
    }

    fn total_point_count(&self) -> usize {
        self.mappings.total_point_count()
    }

    fn available_point_count(&self, _hw_counter: &HardwareCounterCell) -> usize {
        self.mappings.available_point_count()
    }

    fn deleted_point_count(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.total_point_count() - self.available_point_count(hw_counter)
    }

    fn deleted_point_bitslice(&self, _hw_counter: &HardwareCounterCell) -> &BitSlice {
        self.mappings.deleted()
    }

    fn is_deleted_point(&self, key: PointOffsetType, _hw_counter: &HardwareCounterCell) -> bool {
        self.mappings.is_deleted_point(key)
    }

    fn name(&self) -> &'static str {
        "immutable id tracker"
    }

    fn iter_internal_versions(
        &self,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_>> {
        Ok(Box::new(self.internal_to_version.iter()))
    }
}

impl<S: UniversalWrite + Debug + Send + Sync + 'static> IdTracker for ImmutableIdTracker<S> {
    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let has_version = self.internal_to_version.has(internal_id);
        debug_assert!(
            has_version,
            "Can't extend version list in immutable tracker",
        );
        if has_version {
            self.internal_to_version.set(internal_id, version);
            self.internal_to_version_wrapper.set(internal_id, version);
        }

        Ok(())
    }

    fn set_link(
        &mut self,
        _external_id: PointIdType,
        _internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        panic!("Trying to call a mutating function (`set_link`) of an immutable id tracker");
    }

    fn drop(
        &mut self,
        external_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let internal_id = self.mappings.drop(external_id);

        if let Some(internal_id) = internal_id {
            self.deleted_wrapper.set(internal_id as usize, true);
            self.set_internal_version(internal_id, DELETED_POINT_VERSION, hw_counter)?;
        }

        Ok(())
    }

    fn drop_internal(
        &mut self,
        internal_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let Some(external_id) = self.mappings.external_id(internal_id) {
            self.mappings.drop(external_id);
        }

        self.deleted_wrapper.set(internal_id as usize, true);
        self.set_internal_version(internal_id, DELETED_POINT_VERSION, hw_counter)?;

        Ok(())
    }

    /// Creates a flusher function, that writes the deleted points bitvec to disk.
    fn mapping_flusher(&self, hw_counter: &HardwareCounterCell) -> Flusher {
        // Only flush deletions because mappings are immutable
        self.deleted_wrapper.flusher(hw_counter)
    }

    /// Creates a flusher function, that writes the points versions to disk.
    fn versions_flusher(&self, _hw_counter: &HardwareCounterCell) -> Flusher {
        let flusher = self.internal_to_version_wrapper.flusher();
        Box::new(move || flusher().map_err(OperationError::from))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![
            deleted_path(&self.path),
            mappings_path(&self.path),
            version_mapping_path(&self.path),
        ]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![mappings_path(&self.path)]
    }

    fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            path,
            deleted_wrapper,
            internal_to_version: _, // kept in RAM
            internal_to_version_wrapper,
            mappings: _, // kept in RAM, file dropped via `path` below
        } = self;
        // `deleted` and `internal_to_version` are mmap-backed: page them out via
        // `madvise`, which works on actively mapped pages.
        deleted_wrapper.clear_cache()?;
        internal_to_version_wrapper.clear_cache()?;
        // `mappings` is read into RAM and is not mmap-backed, so its file pages
        // can't be reached through a mapping. Drop them with `fadvise` instead.
        clear_disk_cache(&mappings_path(path))?;
        Ok(())
    }
}
