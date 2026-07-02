//! Disk-resident id tracker: keeps the point-id mapping on disk (the
//! [on-disk format](on_disk_format) `i2e`/`e2i` files) instead of loading it into
//! RAM, so resident memory does not scale with point count.
//!
//! Two trackers share the [`reader`] core:
//!
//! - [`DiskIdTracker`] — writable, deletion-only (non-appendable, R6): usable in a
//!   regular [`Segment`](crate::segment::Segment) so ordinary deployments also
//!   avoid the full RAM load. `deleted` and `versions` are kept resident (small,
//!   mutated in place); only the mapping stays on disk.
//! - [`ReadOnlyDiskIdTracker`](read_only::ReadOnlyDiskIdTracker) — read-only,
//!   live-reload mirror for object-storage followers.
//!
//! Both are produced from the same on-disk files, written from a
//! [`CompressedPointMappings`] at build time when `serverless_compatible` is set.

pub mod mappings;
pub mod on_disk_format;
pub mod read_only;
mod reader;

#[cfg(test)]
mod tests;

use std::fmt::Debug;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use common::bitvec::{BitSlice, BitSliceExt as _, BitVec};
use common::fs::clear_disk_cache;
use common::mmap::{AdviceSetting, create_and_ensure_length};
use common::stored_bitslice::StoredBitSlice;
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::{
    OpenOptions, Populate, SliceBufferedUpdateWrapper, TypedStorage, UniversalWrite,
};
use fs_err::File;

pub use self::mappings::DiskMappingsSource;
use self::mappings::log_lookup_err;
pub use self::read_only::ReadOnlyDiskIdTracker;
use self::on_disk_format::{e2i_path, i2e_path, store_e2i, store_i2e};
use self::reader::DiskMappingReader;
use crate::common::Flusher;
use crate::common::buffered_update_bitslice::BufferedUpdateBitSlice;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::immutable_id_tracker::{deleted_path, version_mapping_path};
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::{DELETED_POINT_VERSION, IdTracker, IdTrackerRead, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

/// Writable, deletion-only disk-resident id tracker.
///
/// The mapping (`i2e`/`e2i`) is immutable and served from disk via
/// [`DiskMappingReader`]; the `deleted` bitvec and `versions` are kept resident
/// and mutated in place, mirroring [`ImmutableIdTracker`] minus the RAM mapping.
///
/// [`ImmutableIdTracker`]: crate::id_tracker::immutable_id_tracker::ImmutableIdTracker
#[derive(Debug)]
pub struct DiskIdTracker<S: UniversalWrite> {
    path: PathBuf,

    /// Lazy mapping read core (resident: headers + sparse index).
    reader: DiskMappingReader<S>,

    /// Resident deleted set (read source for lookups/search); persisted via the wrapper.
    deleted: BitVec,
    deleted_wrapper: BufferedUpdateBitSlice<S>,

    /// Resident per-point versions; persisted via the wrapper.
    internal_to_version: CompressedVersions,
    internal_to_version_wrapper: SliceBufferedUpdateWrapper<S, SeqNumberType>,
}

impl<S> DiskIdTracker<S>
where
    S: UniversalWrite + Send + Sync + 'static,
{
    /// Approximate resident RAM: versions + deleted bitvec. The mapping stays on
    /// disk and is not counted here.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            path: _,
            reader: _, // headers + sparse index only; negligible, on-disk mapping not counted
            deleted,
            deleted_wrapper: _, // mmap-backed, accounted via files
            internal_to_version,
            internal_to_version_wrapper: _, // mmap-backed, accounted via files
        } = self;
        internal_to_version.ram_usage_bytes() + deleted.capacity().div_ceil(u8::BITS as usize)
    }

    pub fn from_in_memory_tracker(
        fs: &S::Fs,
        in_memory_tracker: InMemoryIdTracker,
        path: &Path,
    ) -> OperationResult<Self> {
        let (internal_to_version, mappings) = in_memory_tracker.into_internal();
        let compressed_mappings = CompressedPointMappings::from_mappings(mappings);
        Self::new(fs, path, &internal_to_version, compressed_mappings)
    }

    /// Open an existing disk-resident id tracker: `deleted` and `versions` are
    /// read into RAM (small, mutated in place), the mapping stays on disk.
    pub fn open(fs: &S::Fs, segment_path: &Path) -> OperationResult<Self> {
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
        )?;
        let mut deleted = BitVec::new();
        deleted.extend_from_bitslice(deleted_storage.read_all()?.as_ref());
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
        )?;
        let internal_to_version =
            CompressedVersions::from_slice(&internal_to_version_file.read_whole()?);
        let internal_to_version_wrapper =
            SliceBufferedUpdateWrapper::new(internal_to_version_file.inner)?;

        let reader = DiskMappingReader::open(fs, segment_path)?;

        Ok(Self {
            path: segment_path.to_path_buf(),
            reader,
            deleted,
            deleted_wrapper,
            internal_to_version,
            internal_to_version_wrapper,
        })
    }

    pub fn new(
        fs: &S::Fs,
        path: &Path,
        internal_to_version: &[SeqNumberType],
        mappings: CompressedPointMappings,
    ) -> OperationResult<Self> {
        let total = mappings.total_point_count();
        debug_assert!(mappings.deleted().len() <= total);

        // Deleted bitvec file: one bit per point, rounded up to a `u64` multiple.
        let deleted_filepath = deleted_path(path);
        create_and_ensure_length(
            &deleted_filepath,
            total.div_ceil(u8::BITS as usize).next_multiple_of(size_of::<u64>()),
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
        )?;
        deleted_storage.write_bitslice(mappings.deleted())?;
        deleted_storage.set_ascending_bits_batch(
            (mappings.deleted().len()..total).map(|i| (i as u64, true)),
        )?;
        deleted_storage.flusher()()?;

        // Resident deleted mirror: same bits as on disk (trailing points beyond
        // `mappings.deleted()` are deleted).
        let mut deleted = BitVec::new();
        deleted.extend_from_bitslice(mappings.deleted());
        deleted.resize(total, true);

        let deleted_wrapper = BufferedUpdateBitSlice::new(deleted_storage);

        // Versions file: one `u64` per point.
        let version_filepath = version_mapping_path(path);
        let versions_count = internal_to_version.len().max(total);
        create_and_ensure_length(&version_filepath, versions_count * size_of::<SeqNumberType>())?;
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
        )?;
        internal_to_version_file.write(0, internal_to_version)?;
        let internal_to_version =
            CompressedVersions::from_slice(&internal_to_version_file.read_whole()?);
        debug_assert_eq!(internal_to_version.len(), versions_count);
        let internal_to_version_wrapper =
            SliceBufferedUpdateWrapper::new(internal_to_version_file.inner)?;

        // Mapping files (immutable): i2e + e2i.
        write_mapping_file(i2e_path(path), |writer| store_i2e(&mappings, writer))?;
        write_mapping_file(e2i_path(path), |writer| store_e2i(&mappings, writer))?;

        deleted_wrapper.flusher()()?;
        internal_to_version_wrapper.flusher()()?;

        let reader = DiskMappingReader::open(fs, path)?;

        Ok(Self {
            path: path.to_path_buf(),
            reader,
            deleted,
            deleted_wrapper,
            internal_to_version,
            internal_to_version_wrapper,
        })
    }

    fn mapping_files(&self) -> Vec<PathBuf> {
        vec![i2e_path(&self.path), e2i_path(&self.path)]
    }
}

/// Create a mapping file and write it with an explicit fsync.
fn write_mapping_file(
    path: PathBuf,
    write: impl FnOnce(&mut BufWriter<File>) -> OperationResult<()>,
) -> OperationResult<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    write(&mut writer)?;
    writer.flush()?;
    writer.into_inner().unwrap().sync_all()?;
    Ok(())
}

impl<S: UniversalWrite + Send + Sync + 'static> DiskMappingsSource for DiskIdTracker<S> {
    type Backend = S;

    fn mapping_reader(&self) -> &DiskMappingReader<S> {
        &self.reader
    }

    fn point_deleted(&self, offset: PointOffsetType) -> OperationResult<bool> {
        // Resident bitvec, so this never fails; out-of-range offsets are treated
        // as deleted (mirrors the in-RAM tracker).
        Ok(self.deleted.get_bit(offset as usize).unwrap_or(true))
    }

    fn deleted_bitslice(&self) -> OperationResult<&BitSlice> {
        // Resident bitvec, so this never fails.
        Ok(&self.deleted)
    }
}

impl<S: UniversalWrite + Send + Sync + 'static> IdTrackerRead for DiskIdTracker<S> {
    type Backend = S;

    fn point_mappings(&self) -> PointMappingsRefEnum<'_, Self::Backend> {
        PointMappingsRefEnum::Disk(self.mappings_ref_lossy())
    }

    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id)
    }

    fn internal_id_with_behavior(
        &self,
        external_id: PointIdType,
        _deferred_behavior: DeferredBehavior,
    ) -> Option<PointOffsetType> {
        log_lookup_err(self.resolve_internal(external_id))
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        log_lookup_err(self.resolve_external(internal_id))
    }

    fn total_point_count(&self) -> usize {
        self.reader.total_point_count() as usize
    }

    fn deleted_point_count(&self) -> usize {
        self.deleted.count_ones()
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        &self.deleted
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        // Resident bitvec, so this never actually errors.
        self.point_deleted(key).unwrap_or(true)
    }

    fn name(&self) -> &'static str {
        "disk id tracker"
    }

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        Box::new(self.internal_to_version.iter())
    }
}

impl<S: UniversalWrite + Debug + Send + Sync + 'static> IdTracker for DiskIdTracker<S> {
    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        let has_version = self.internal_to_version.has(internal_id);
        debug_assert!(has_version, "Can't extend version list in disk id tracker");
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
        panic!("Trying to call a mutating function (`set_link`) of a disk id tracker");
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        // Mutating path: propagate lookup/deletion-check errors instead of swallowing.
        if let Some(internal_id) = self.reader.lookup(external_id)? {
            if !self.point_deleted(internal_id)? {
                self.deleted.set(internal_id as usize, true);
                self.deleted_wrapper.set(internal_id as usize, true);
                self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
            }
        }
        Ok(())
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        self.deleted.set(internal_id as usize, true);
        self.deleted_wrapper.set(internal_id as usize, true);
        self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        Ok(())
    }

    /// Only deletions are flushed; the mapping is immutable.
    fn mapping_flusher(&self) -> Flusher {
        self.deleted_wrapper.flusher()
    }

    fn versions_flusher(&self) -> Flusher {
        let flusher = self.internal_to_version_wrapper.flusher();
        Box::new(move || flusher().map_err(OperationError::from))
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![deleted_path(&self.path), version_mapping_path(&self.path)];
        files.extend(self.mapping_files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.mapping_files()
    }

    fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            path,
            reader: _, // i2e/e2i mmap pages dropped via `mapping_files` below
            deleted: _, // kept in RAM
            deleted_wrapper,
            internal_to_version: _, // kept in RAM
            internal_to_version_wrapper,
        } = self;
        deleted_wrapper.clear_cache()?;
        internal_to_version_wrapper.clear_cache()?;
        for file in self.mapping_files() {
            clear_disk_cache(&file)?;
        }
        let _ = path;
        Ok(())
    }
}
