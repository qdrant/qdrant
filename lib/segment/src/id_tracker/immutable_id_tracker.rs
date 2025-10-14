use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::mem::{size_of, size_of_val};
use std::path::{Path, PathBuf};

use bitvec::prelude::BitSlice;
use bitvec::vec::BitVec;
use byteorder::{ReadBytesExt, WriteBytesExt};
use common::ext::BitSliceExt as _;
use common::types::PointOffsetType;
use fs_err::File;
use memory::madvise::AdviceSetting;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use memory::mmap_type::{MmapBitSlice, MmapSlice};
use uuid::Uuid;

use crate::common::Flusher;
use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::mmap_slice_buffered_update_wrapper::MmapSliceBufferedUpdateWrapper;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::compressed::external_to_internal::CompressedExternalToInternal;
use crate::id_tracker::compressed::internal_to_external::CompressedInternalToExternal;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::point_mappings::FileEndianess;
use crate::id_tracker::{DELETED_POINT_VERSION, IdTracker};
use crate::types::{ExtendedPointId, PointIdType, SeqNumberType};

pub const DELETED_FILE_NAME: &str = "id_tracker.deleted";
pub const MAPPINGS_FILE_NAME: &str = "id_tracker.mappings";
pub const VERSION_MAPPING_FILE_NAME: &str = "id_tracker.versions";

#[derive(Copy, Clone)]
#[repr(u8)]
enum ExternalIdType {
    Number = 0,
    Uuid = 1,
}

impl ExternalIdType {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            x if x == Self::Number as u8 => Some(Self::Number),
            x if x == Self::Uuid as u8 => Some(Self::Uuid),
            _ => None,
        }
    }

    fn from_point_id(point_id: &PointIdType) -> Self {
        match point_id {
            PointIdType::NumId(_) => Self::Number,
            PointIdType::Uuid(_) => Self::Uuid,
        }
    }
}

#[derive(Debug)]
pub struct ImmutableIdTracker {
    path: PathBuf,

    deleted_wrapper: MmapBitSliceBufferedUpdateWrapper,

    internal_to_version: CompressedVersions,
    internal_to_version_wrapper: MmapSliceBufferedUpdateWrapper<SeqNumberType>,

    mappings: CompressedPointMappings,
}

impl ImmutableIdTracker {
    pub fn from_in_memory_tracker(
        in_memory_tracker: InMemoryIdTracker,
        path: &Path,
    ) -> OperationResult<Self> {
        let (internal_to_version, mappings) = in_memory_tracker.into_internal();
        let compressed_mappings = CompressedPointMappings::from_mappings(mappings);
        let id_tracker = Self::new(path, &internal_to_version, compressed_mappings)?;

        Ok(id_tracker)
    }

    /// Loads a `CompressedPointMappings` from the given reader. Applies an optional filter of deleted items
    /// to prevent allocating unneeded data.
    fn load_mapping<R: BufRead>(
        mut reader: R,
        deleted: Option<BitVec>,
    ) -> OperationResult<CompressedPointMappings> {
        // Deserialize the header
        let len = reader.read_u64::<FileEndianess>()? as usize;

        let mut deleted = deleted.unwrap_or_else(|| BitVec::repeat(false, len));

        deleted.truncate(len);

        let mut internal_to_external = CompressedInternalToExternal::with_capacity(len);
        let mut external_to_internal_num: Vec<(u64, PointOffsetType)> = Vec::new();
        let mut external_to_internal_uuid: Vec<(Uuid, PointOffsetType)> = Vec::new();

        // Deserialize the list entries
        for i in 0..len {
            let (internal_id, external_id) = Self::read_entry(&mut reader)
                .map_err(|err| {
                    OperationError::inconsistent_storage(format!("Immutable ID tracker failed to read next mapping, reading {} out of {len}, assuming malformed storage: {err}", i + 1))
                })?;

            // Need to push this regardless of point deletion as the vecs index represents the internal id
            // which would become wrong if we leave out entries.
            if internal_to_external.len() <= internal_id as usize {
                internal_to_external.resize(internal_id as usize + 1, PointIdType::NumId(0));
            }

            internal_to_external.set(internal_id, external_id);

            let point_deleted = deleted.get_bit(i).unwrap_or(false);
            if point_deleted {
                continue;
            }

            match external_id {
                ExtendedPointId::NumId(num) => {
                    external_to_internal_num.push((num, internal_id));
                }
                ExtendedPointId::Uuid(uuid) => {
                    external_to_internal_uuid.push((uuid, internal_id));
                }
            }
        }

        // Check that the file has been fully read.
        #[cfg(debug_assertions)] // Only for dev builds
        {
            debug_assert_eq!(reader.bytes().map(Result::unwrap).count(), 0,);
        }

        let external_to_internal = CompressedExternalToInternal::from_vectors(
            external_to_internal_num,
            external_to_internal_uuid,
        );

        Ok(CompressedPointMappings::new(
            deleted,
            internal_to_external,
            external_to_internal,
        ))
    }

    /// Loads a single entry from a reader. Expects the reader to be aligned so, that the next read
    /// byte is the first byte of a new entry.
    /// This function reads exact one entry which means after calling this function, the reader
    /// will be at the start of the next entry.
    pub(crate) fn read_entry<R: Read>(
        mut reader: R,
    ) -> OperationResult<(PointOffsetType, ExtendedPointId)> {
        let point_id_type = reader.read_u8().map_err(|err| {
            OperationError::inconsistent_storage(format!(
                "failed to read point ID type from file: {err}"
            ))
        })?;

        let external_id = match ExternalIdType::from_byte(point_id_type) {
            None => {
                return Err(OperationError::inconsistent_storage(
                    "invalid byte for point ID type",
                ));
            }
            Some(ExternalIdType::Number) => {
                let num = reader.read_u64::<FileEndianess>().map_err(|err| {
                    OperationError::inconsistent_storage(format!(
                        "failed to read numeric point ID from file: {err}"
                    ))
                })?;
                PointIdType::NumId(num)
            }
            Some(ExternalIdType::Uuid) => {
                let uuid_u128 = reader.read_u128::<FileEndianess>().map_err(|err| {
                    OperationError::inconsistent_storage(format!(
                        "failed to read UUID point ID from file: {err}"
                    ))
                })?;
                PointIdType::Uuid(Uuid::from_u128_le(uuid_u128))
            }
        };

        let internal_id = reader.read_u32::<FileEndianess>().map_err(|err| {
            OperationError::inconsistent_storage(format!(
                "failed to read internal point ID from file: {err}"
            ))
        })? as PointOffsetType;
        Ok((internal_id, external_id))
    }

    /// Serializes the `PointMappings` into the given writer using the file format specified below.
    ///
    /// ## File format
    /// In general the format looks like this:
    /// +---------------------------+-----------------+
    /// | Header (list length: u64) | List of entries |
    /// +---------------------------+-----------------+
    ///
    /// A single list entry:
    /// +-----------------+-----------------------+------------------+
    /// | PointIdType: u8 | Number/UUID: u64/u128 | Internal ID: u32 |
    /// +-----------------+-----------------------+------------------+
    /// A single entry is thus either 1+8+4=13 or 1+16+4=21 bytes in size depending
    /// on the PointIdType.
    fn store_mapping<W: Write>(
        mappings: &CompressedPointMappings,
        mut writer: W,
    ) -> OperationResult<()> {
        let number_of_entries = mappings.total_point_count();

        // Serialize the header (=length).
        writer.write_u64::<FileEndianess>(number_of_entries as u64)?;

        // Serialize all entries
        for (internal_id, external_id) in mappings.iter_internal_raw() {
            Self::write_entry(&mut writer, internal_id, external_id)?;
        }

        Ok(())
    }

    fn write_entry<W: Write>(
        mut writer: W,
        internal_id: PointOffsetType,
        external_id: PointIdType,
    ) -> OperationResult<()> {
        // Byte to distinguish between Number and UUID
        writer.write_u8(ExternalIdType::from_point_id(&external_id) as u8)?;

        // Serializing External ID
        match external_id {
            PointIdType::NumId(num) => {
                // The PointID's number
                writer.write_u64::<FileEndianess>(num)?;
            }
            PointIdType::Uuid(uuid) => {
                // The PointID's UUID
                writer.write_u128::<FileEndianess>(uuid.to_u128_le())?;
            }
        }

        // Serializing Internal ID
        writer.write_u32::<FileEndianess>(internal_id)?;

        Ok(())
    }

    pub fn open(segment_path: &Path) -> OperationResult<Self> {
        let deleted_raw = open_write_mmap(
            &Self::deleted_file_path(segment_path),
            AdviceSetting::Global,
            true,
        )?;
        let deleted_mmap = MmapBitSlice::try_from(deleted_raw, 0)?;
        let deleted_bitvec = deleted_mmap.to_bitvec();
        let deleted_wrapper = MmapBitSliceBufferedUpdateWrapper::new(deleted_mmap);

        let internal_to_version_map = open_write_mmap(
            &Self::version_mapping_file_path(segment_path),
            AdviceSetting::Global,
            true,
        )?;
        let internal_to_version_mapslice: MmapSlice<SeqNumberType> =
            unsafe { MmapSlice::try_from(internal_to_version_map)? };
        let internal_to_version = CompressedVersions::from_slice(&internal_to_version_mapslice);
        let internal_to_version_wrapper =
            MmapSliceBufferedUpdateWrapper::new(internal_to_version_mapslice);

        let reader = BufReader::new(File::open(Self::mappings_file_path(segment_path))?);
        let mappings = Self::load_mapping(reader, Some(deleted_bitvec))?;

        Ok(Self {
            path: segment_path.to_path_buf(),
            deleted_wrapper,
            internal_to_version_wrapper,
            internal_to_version,
            mappings,
        })
    }

    pub fn new(
        path: &Path,
        internal_to_version: &[SeqNumberType],
        mappings: CompressedPointMappings,
    ) -> OperationResult<Self> {
        // Create mmap file for deleted bitvec
        let deleted_filepath = Self::deleted_file_path(path);
        {
            let deleted_size = bitmap_mmap_size(mappings.total_point_count());
            create_and_ensure_length(&deleted_filepath, deleted_size)?;
        }

        debug_assert!(mappings.deleted().len() <= mappings.total_point_count());

        let deleted_mmap = open_write_mmap(&deleted_filepath, AdviceSetting::Global, false)?;
        let mut deleted_new = MmapBitSlice::try_from(deleted_mmap, 0)?;
        deleted_new[..mappings.deleted().len()].copy_from_bitslice(mappings.deleted());

        for i in mappings.deleted().len()..mappings.total_point_count() {
            deleted_new.set(i, true);
        }

        let deleted_wrapper = MmapBitSliceBufferedUpdateWrapper::new(deleted_new);

        // Create mmap file for internal-to-version list
        let version_filepath = Self::version_mapping_file_path(path);

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
        let mut internal_to_version_wrapper = unsafe {
            MmapSlice::try_from(open_write_mmap(
                &version_filepath,
                AdviceSetting::Global,
                false,
            )?)?
        };

        internal_to_version_wrapper[..internal_to_version.len()]
            .copy_from_slice(internal_to_version);
        let internal_to_version = CompressedVersions::from_slice(&internal_to_version_wrapper);

        debug_assert_eq!(internal_to_version.len(), mappings.total_point_count());

        let internal_to_version_wrapper =
            MmapSliceBufferedUpdateWrapper::new(internal_to_version_wrapper);

        // Write mappings to disk.
        let file = File::create(Self::mappings_file_path(path))?;
        let mut writer = BufWriter::new(file);
        Self::store_mapping(&mappings, &mut writer)?;

        // Explicitly fsync file contents to ensure durability
        writer.flush()?;
        let file = writer.into_inner().unwrap();
        file.sync_all()?;

        deleted_wrapper.flusher()()?;
        internal_to_version_wrapper.flusher()()?;

        Ok(Self {
            path: path.to_path_buf(),
            deleted_wrapper,
            internal_to_version_wrapper,
            internal_to_version,
            mappings,
        })
    }

    fn deleted_file_path(base: &Path) -> PathBuf {
        base.join(DELETED_FILE_NAME)
    }

    fn version_mapping_file_path(base: &Path) -> PathBuf {
        base.join(VERSION_MAPPING_FILE_NAME)
    }

    pub(crate) fn mappings_file_path(base: &Path) -> PathBuf {
        base.join(MAPPINGS_FILE_NAME)
    }
}

/// Returns the required mmap filesize for a given length of a slice of type `T`.
fn mmap_size<T>(len: usize) -> usize {
    let item_width = size_of::<T>();
    len.div_ceil(item_width) * item_width // Make it a multiple of usize-width.
}

/// Returns the required mmap filesize for a `BitSlice`.
fn bitmap_mmap_size(number_of_elements: usize) -> usize {
    mmap_size::<usize>(number_of_elements.div_ceil(u8::BITS as usize))
}

impl IdTracker for ImmutableIdTracker {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id)
    }

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        let has_version = self.internal_to_version.has(internal_id);
        debug_assert!(
            has_version,
            "Can't extend version list in immutable tracker",
        );
        if has_version {
            self.internal_to_version.set(internal_id, version);
            self.internal_to_version_wrapper
                .set(internal_id as usize, version);
        }

        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.mappings.internal_id(&external_id)
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.mappings.external_id(internal_id)
    }

    fn set_link(
        &mut self,
        _external_id: PointIdType,
        _internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        panic!("Trying to call a mutating function (`set_link`) of an immutable id tracker");
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        let internal_id = self.mappings.drop(external_id);

        if let Some(internal_id) = internal_id {
            self.deleted_wrapper.set(internal_id as usize, true);
            self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        }

        Ok(())
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        if let Some(external_id) = self.mappings.external_id(internal_id) {
            self.mappings.drop(external_id);
        }

        self.deleted_wrapper.set(internal_id as usize, true);
        self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;

        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        self.mappings.iter_external()
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.mappings.iter_internal()
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        self.mappings.iter_from(external_id)
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
    }

    fn iter_random(&self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        self.mappings.iter_random()
    }

    /// Creates a flusher function, that writes the deleted points bitvec to disk.
    fn mapping_flusher(&self) -> Flusher {
        // Only flush deletions because mappings are immutable
        self.deleted_wrapper.flusher()
    }

    /// Creates a flusher function, that writes the points versions to disk.
    fn versions_flusher(&self) -> Flusher {
        self.internal_to_version_wrapper.flusher()
    }

    fn total_point_count(&self) -> usize {
        self.mappings.total_point_count()
    }

    fn available_point_count(&self) -> usize {
        self.mappings.available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.total_point_count() - self.available_point_count()
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        self.mappings.deleted()
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        self.mappings.is_deleted_point(key)
    }

    fn name(&self) -> &'static str {
        "immutable id tracker"
    }

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        Box::new(self.internal_to_version.iter())
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![
            Self::deleted_file_path(&self.path),
            Self::mappings_file_path(&self.path),
            Self::version_mapping_file_path(&self.path),
        ]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![Self::mappings_file_path(&self.path)]
    }
}

#[cfg(test)]
pub(super) mod test {
    use std::collections::{HashMap, HashSet};

    use itertools::Itertools;
    #[cfg(feature = "rocksdb")]
    use rand::Rng;
    use rand::prelude::*;
    use tempfile::Builder;
    use uuid::Uuid;

    use super::*;
    #[cfg(feature = "rocksdb")]
    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;

    const RAND_SEED: u64 = 42;

    #[test]
    fn test_iterator() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let mut id_tracker = InMemoryIdTracker::new();

        id_tracker.set_link(200.into(), 0).unwrap();
        id_tracker.set_link(100.into(), 1).unwrap();
        id_tracker.set_link(150.into(), 2).unwrap();
        id_tracker.set_link(120.into(), 3).unwrap();
        id_tracker.set_link(180.into(), 4).unwrap();
        id_tracker.set_link(110.into(), 5).unwrap();
        id_tracker.set_link(115.into(), 6).unwrap();
        id_tracker.set_link(190.into(), 7).unwrap();
        id_tracker.set_link(177.into(), 8).unwrap();
        id_tracker.set_link(118.into(), 9).unwrap();

        let id_tracker =
            ImmutableIdTracker::from_in_memory_tracker(id_tracker, dir.path()).unwrap();

        let first_four = id_tracker.iter_from(None).take(4).collect_vec();

        assert_eq!(first_four.len(), 4);
        assert_eq!(first_four[0].0, 100.into());

        let last = id_tracker.iter_from(Some(first_four[3].0)).collect_vec();
        assert_eq!(last.len(), 7);
    }

    pub const TEST_POINTS: &[PointIdType] = &[
        PointIdType::NumId(100),
        PointIdType::Uuid(Uuid::from_u128(123_u128)),
        PointIdType::Uuid(Uuid::from_u128(156_u128)),
        PointIdType::NumId(150),
        PointIdType::NumId(120),
        PointIdType::Uuid(Uuid::from_u128(12_u128)),
        PointIdType::NumId(180),
        PointIdType::NumId(110),
        PointIdType::NumId(115),
        PointIdType::Uuid(Uuid::from_u128(673_u128)),
        PointIdType::NumId(190),
        PointIdType::NumId(177),
        PointIdType::Uuid(Uuid::from_u128(971_u128)),
    ];

    #[test]
    fn test_mixed_types_iterator() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let id_tracker = make_immutable_tracker(dir.path());

        let sorted_from_tracker = id_tracker.iter_from(None).map(|(k, _)| k).collect_vec();

        let mut values = TEST_POINTS.to_vec();
        values.sort();

        assert_eq!(sorted_from_tracker, values);
    }

    #[test]
    fn test_load_store() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let (old_mappings, old_versions) = {
            let id_tracker = make_immutable_tracker(dir.path());
            (id_tracker.mappings, id_tracker.internal_to_version)
        };

        let mut loaded_id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();

        // We may extend the length of deleted bitvec as memory maps need to be aligned to
        // a multiple of `usize-width`.
        assert_eq!(
            old_versions.len(),
            loaded_id_tracker.internal_to_version.len()
        );
        for i in 0..old_versions.len() as u32 {
            assert_eq!(
                old_versions.get(i),
                loaded_id_tracker.internal_to_version.get(i),
                "Version mismatch at index {i}",
            );
        }

        assert_eq!(old_mappings, loaded_id_tracker.mappings);

        loaded_id_tracker.drop(PointIdType::NumId(180)).unwrap();
    }

    /// Mutates an ID tracker and stores it to disk. Tests whether loading results in the exact same
    /// ID tracker.
    #[test]
    fn test_store_load_mutated() {
        let mut rng = StdRng::seed_from_u64(RAND_SEED);

        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let (dropped_points, custom_version) = {
            let mut id_tracker = make_immutable_tracker(dir.path());

            let mut dropped_points = HashSet::new();
            let mut custom_version = HashMap::new();

            for (index, point) in TEST_POINTS.iter().enumerate() {
                if index % 2 == 0 {
                    continue;
                }

                if index % 3 == 0 {
                    id_tracker.drop(*point).unwrap();
                    dropped_points.insert(*point);
                    continue;
                }

                if index % 5 == 0 {
                    let new_version = rng.next_u64();
                    id_tracker
                        .set_internal_version(index as PointOffsetType, new_version)
                        .unwrap();
                    custom_version.insert(index as PointOffsetType, new_version);
                }
            }

            id_tracker.mapping_flusher()().unwrap();
            id_tracker.versions_flusher()().unwrap();

            (dropped_points, custom_version)
        };

        let id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();
        for (index, point) in TEST_POINTS.iter().enumerate() {
            let internal_id = index as PointOffsetType;

            if dropped_points.contains(point) {
                assert!(id_tracker.is_deleted_point(internal_id));
                assert_eq!(id_tracker.external_id(internal_id), None);
                assert!(id_tracker.mappings.internal_id(point).is_none());

                continue;
            }

            // Check version
            let expect_version = custom_version
                .get(&internal_id)
                .copied()
                .unwrap_or(DEFAULT_VERSION);

            assert_eq!(
                id_tracker.internal_to_version.get(internal_id),
                Some(expect_version)
            );

            // Check that unmodified points still haven't changed.
            assert_eq!(
                id_tracker.external_id(index as PointOffsetType),
                Some(*point)
            );
        }
    }

    #[test]
    fn test_all_points_have_version() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let id_tracker = make_immutable_tracker(dir.path());
        for i in id_tracker.iter_ids() {
            assert!(id_tracker.internal_version(i).is_some());
        }
    }

    #[test]
    fn test_point_deletion_correctness() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let mut id_tracker = make_immutable_tracker(dir.path());

        let deleted_points = id_tracker.total_point_count() - id_tracker.available_point_count();

        let point_to_delete = PointIdType::NumId(100);

        assert!(id_tracker.iter_external().contains(&point_to_delete));

        assert_eq!(id_tracker.internal_id(point_to_delete), Some(0));

        id_tracker.drop(point_to_delete).unwrap();

        let point_exists = id_tracker.internal_id(point_to_delete).is_some()
            && id_tracker.iter_external().contains(&point_to_delete)
            && id_tracker.iter_from(None).any(|i| i.0 == point_to_delete);

        assert!(!point_exists);

        let new_deleted_points =
            id_tracker.total_point_count() - id_tracker.available_point_count();

        assert_eq!(new_deleted_points, deleted_points + 1);
    }

    #[test]
    fn test_point_deletion_persists_reload() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let point_to_delete = PointIdType::NumId(100);

        let old_mappings = {
            let mut id_tracker = make_immutable_tracker(dir.path());
            let intetrnal_id = id_tracker
                .internal_id(point_to_delete)
                .expect("Point to delete exists.");
            assert!(!id_tracker.is_deleted_point(intetrnal_id));
            id_tracker.drop(point_to_delete).unwrap();
            id_tracker.mapping_flusher()().unwrap();
            id_tracker.versions_flusher()().unwrap();
            id_tracker.mappings
        };

        // Point should still be gone
        let id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();
        assert_eq!(id_tracker.internal_id(point_to_delete), None);

        old_mappings
            .iter_internal_raw()
            .zip(id_tracker.mappings.iter_internal_raw())
            .for_each(
                |((old_internal, old_external), (new_internal, new_external))| {
                    assert_eq!(old_internal, new_internal);
                    assert_eq!(old_external, new_external);
                },
            );
    }

    /// Tests de/serializing of whole `PointMappings`.
    #[test]
    fn test_point_mappings_de_serialization() {
        let mut rng = StdRng::seed_from_u64(RAND_SEED);

        let mut buf = vec![];

        // Test different sized PointMappings, growing exponentially to also test large ones.
        // This way we test up to 2^16 entries.
        for size_exp in (0..16u32).step_by(3) {
            buf.clear();

            let size = 2usize.pow(size_exp);

            let mappings = CompressedPointMappings::random(&mut rng, size as u32);

            ImmutableIdTracker::store_mapping(&mappings, &mut buf).unwrap();

            // 16 is the min byte size of an entry. The exact number is not that important
            // we just want to ensure that the written bytes correlate to the amount of entries.
            assert!(buf.len() >= size * 16);

            let new_mappings = ImmutableIdTracker::load_mapping(&*buf, None).unwrap();

            assert_eq!(new_mappings.total_point_count(), size);
            assert_eq!(mappings, new_mappings);
        }
    }

    /// Verifies that de/serializing works properly for empty `PointMappings`.
    #[test]
    fn test_point_mappings_de_serialization_empty() {
        let mut rng = StdRng::seed_from_u64(RAND_SEED);
        let mappings = CompressedPointMappings::random(&mut rng, 0);

        let mut buf = vec![];

        ImmutableIdTracker::store_mapping(&mappings, &mut buf).unwrap();

        // We still have a header!
        assert!(!buf.is_empty());

        let new_mappings = ImmutableIdTracker::load_mapping(&*buf, None).unwrap();

        assert_eq!(new_mappings.total_point_count(), 0);
        assert_eq!(mappings, new_mappings);
    }

    /// Tests de/serializing of only single ID mappings.
    #[test]
    fn test_point_mappings_de_serialization_single() {
        let mut rng = StdRng::seed_from_u64(RAND_SEED);

        const SIZE: usize = 400_000;

        let mappings = CompressedPointMappings::random(&mut rng, SIZE as u32);

        for i in 0..SIZE {
            let mut buf = vec![];

            let internal_id = i as PointOffsetType;

            let expected_external = mappings.external_id(internal_id).unwrap();

            ImmutableIdTracker::write_entry(&mut buf, internal_id, expected_external).unwrap();

            let (got_internal, got_external) = ImmutableIdTracker::read_entry(&*buf).unwrap();

            assert_eq!(i as PointOffsetType, got_internal);
            assert_eq!(expected_external, got_external);
        }
    }

    const DEFAULT_VERSION: SeqNumberType = 42;

    fn make_in_memory_tracker_from_memory() -> InMemoryIdTracker {
        let mut id_tracker = InMemoryIdTracker::new();

        for value in TEST_POINTS.iter() {
            let internal_id = id_tracker.total_point_count() as PointOffsetType;
            id_tracker.set_link(*value, internal_id).unwrap();
            id_tracker
                .set_internal_version(internal_id, DEFAULT_VERSION)
                .unwrap()
        }

        id_tracker
    }

    fn make_immutable_tracker(path: &Path) -> ImmutableIdTracker {
        let id_tracker = make_in_memory_tracker_from_memory();
        ImmutableIdTracker::from_in_memory_tracker(id_tracker, path).unwrap()
    }

    #[test]
    fn test_id_tracker_equal() {
        let in_memory_id_tracker = make_in_memory_tracker_from_memory();

        let immutable_id_tracker_dir = Builder::new()
            .prefix("storage_dir_immutable")
            .tempdir()
            .unwrap();
        let immutable_id_tracker = make_immutable_tracker(immutable_id_tracker_dir.path());

        assert_eq!(
            in_memory_id_tracker.available_point_count(),
            immutable_id_tracker.available_point_count()
        );
        assert_eq!(
            in_memory_id_tracker.total_point_count(),
            immutable_id_tracker.total_point_count()
        );

        for (internal, external) in TEST_POINTS.iter().enumerate() {
            let internal = internal as PointOffsetType;

            assert_eq!(
                in_memory_id_tracker.internal_id(*external),
                immutable_id_tracker.internal_id(*external)
            );

            assert_eq!(
                in_memory_id_tracker
                    .internal_version(internal)
                    .unwrap_or_default(),
                immutable_id_tracker
                    .internal_version(internal)
                    .unwrap_or_default()
            );

            assert_eq!(
                in_memory_id_tracker.external_id(internal),
                immutable_id_tracker.external_id(internal)
            );
        }
    }

    #[test]
    #[cfg(feature = "rocksdb")]
    fn simple_id_tracker_vs_immutable_tracker_congruence() {
        use crate::common::rocksdb_wrapper::{DB_VECTOR_CF, open_db};

        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut id_tracker = InMemoryIdTracker::new();
        let mut simple_id_tracker = SimpleIdTracker::open(db).unwrap();

        // Insert 100 random points into id_tracker

        let num_points = 200;
        let mut rng = StdRng::seed_from_u64(RAND_SEED);

        for _ in 0..num_points {
            // Generate num id in range from 0 to 100

            let point_id = PointIdType::NumId(rng.random_range(0..num_points as u64));

            let version = rng.random_range(0..1000);

            let internal_id_mmap = id_tracker.total_point_count() as PointOffsetType;
            let internal_id_simple = simple_id_tracker.total_point_count() as PointOffsetType;

            assert_eq!(internal_id_mmap, internal_id_simple);

            if id_tracker.internal_id(point_id).is_some() {
                id_tracker.drop(point_id).unwrap();
            }
            id_tracker.set_link(point_id, internal_id_mmap).unwrap();
            id_tracker
                .set_internal_version(internal_id_mmap, version)
                .unwrap();

            if simple_id_tracker.internal_id(point_id).is_some() {
                simple_id_tracker.drop(point_id).unwrap();
            }
            simple_id_tracker
                .set_link(point_id, internal_id_simple)
                .unwrap();
            simple_id_tracker
                .set_internal_version(internal_id_simple, version)
                .unwrap();
        }

        let immutable_id_tracker =
            ImmutableIdTracker::from_in_memory_tracker(id_tracker, dir.path()).unwrap();
        drop(immutable_id_tracker);

        let immutable_id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();

        for (external_id, internal_id) in simple_id_tracker.iter_from(None) {
            assert_eq!(
                simple_id_tracker.internal_version(internal_id).unwrap(),
                immutable_id_tracker.internal_version(internal_id).unwrap()
            );
            assert_eq!(
                simple_id_tracker.external_id(internal_id),
                immutable_id_tracker.external_id(internal_id)
            );
            assert_eq!(
                external_id,
                immutable_id_tracker.external_id(internal_id).unwrap()
            );
            assert_eq!(
                simple_id_tracker.external_id(internal_id).unwrap(),
                immutable_id_tracker.external_id(internal_id).unwrap()
            );
        }

        for (external_id, internal_id) in immutable_id_tracker.iter_from(None) {
            assert_eq!(
                simple_id_tracker.internal_version(internal_id).unwrap(),
                immutable_id_tracker.internal_version(internal_id).unwrap()
            );
            assert_eq!(
                simple_id_tracker.external_id(internal_id),
                immutable_id_tracker.external_id(internal_id)
            );
            assert_eq!(
                external_id,
                simple_id_tracker.external_id(internal_id).unwrap()
            );
            assert_eq!(
                simple_id_tracker.external_id(internal_id).unwrap(),
                immutable_id_tracker.external_id(internal_id).unwrap()
            );
        }
    }
}
