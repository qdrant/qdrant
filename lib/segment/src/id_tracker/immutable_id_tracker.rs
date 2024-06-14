use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::mem::size_of_val;
use std::path::{Path, PathBuf};

use bitvec::prelude::BitSlice;
use bitvec::vec::BitVec;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use common::types::PointOffsetType;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use uuid::Uuid;

use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::mmap_slice_buffered_update_wrapper::MmapSliceBufferedUpdateWrapper;
use crate::common::mmap_type::{MmapBitSlice, MmapSlice};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::id_tracker::IdTracker;
use crate::types::{ExtendedPointId, PointIdType, SeqNumberType};

pub const DELETED_FILE_NAME: &str = "id_tracker.deleted";
pub const MAPPINGS_FILE_NAME: &str = "id_tracker.mappings";
pub const VERSION_MAPPING_FILE_NAME: &str = "id_tracker.versions";

pub struct ImmutableIdTracker {
    path: PathBuf,

    deleted: BitVec,
    deleted_wrapper: MmapBitSliceBufferedUpdateWrapper,

    internal_to_version: Vec<SeqNumberType>,
    internal_to_version_wrapper: MmapSliceBufferedUpdateWrapper<SeqNumberType>,

    mappings: PointMappings,
}

#[derive(Clone, PartialEq, Debug)]
pub struct PointMappings {
    pub(crate) internal_to_external: Vec<PointIdType>,

    // Having two separate maps allows us iterating only over one type at a time without having to filter.
    pub(crate) external_to_internal_num: BTreeMap<u64, PointOffsetType>,
    pub(crate) external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType>,
}

type Byteorder = LittleEndian;

impl PointMappings {
    const EXTERNAL_ID_NUMBER_BYTE: u8 = 0;
    const EXTERNAL_ID_UUID_BYTE: u8 = 1;

    // TODO:
    // - Describe storage layout in comment
    // - Add code comments
    // - Add tests for this load/store
    // - Improve readability
    // - Fix failing test causted by differently built BTreeMaps

    pub fn load(file: &Path, filter: Option<&BitSlice>) -> OperationResult<Self> {
        let mut reader = BufReader::new(File::open(file)?);

        let len = reader.read_u64::<Byteorder>()? as usize;

        let mut internal_to_external = Vec::with_capacity(len);
        let mut external_to_internal_num: BTreeMap<u64, PointOffsetType> = BTreeMap::new();
        let mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType> = BTreeMap::new();

        for i in 0..len {
            let external_id_type = reader.read_u8()?;

            let external_id = if external_id_type == Self::EXTERNAL_ID_NUMBER_BYTE {
                let num = reader.read_u64::<Byteorder>()?;
                PointIdType::NumId(num)
            } else if external_id_type == Self::EXTERNAL_ID_UUID_BYTE {
                let uuid_u128 = reader.read_u128::<Byteorder>()?;
                PointIdType::Uuid(Uuid::from_u128(uuid_u128))
            } else {
                return Err(OperationError::InconsistentStorage {
                    description: "Invalid byte read when deserializing Immutable id tracker"
                        .to_string(),
                });
            };

            let internal_id = reader.read_u32::<Byteorder>()? as PointOffsetType;

            // Need to push this regardless of point deletion as the vecs index represents the internal id
            // which would become wrong if we leave out entries.
            internal_to_external.push(external_id);

            let deleted = filter
                .as_ref()
                .and_then(|deleted| deleted.get(i).as_deref().copied())
                .unwrap_or_default();

            if deleted {
                continue;
            }

            match external_id {
                ExtendedPointId::NumId(num) => {
                    external_to_internal_num.insert(num, internal_id);
                }
                ExtendedPointId::Uuid(uuid) => {
                    external_to_internal_uuid.insert(uuid, internal_id);
                }
            }
        }

        // assert that the file has ben fully read.
        #[cfg(debug_assertions)] // Only for dev builds
        {
            let mut buf = vec![];
            let read_bytes = reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf.len(), 0);
            assert_eq!(read_bytes, 0);
        }

        Ok(PointMappings {
            internal_to_external,
            external_to_internal_num,
            external_to_internal_uuid,
        })
    }

    pub fn store(&self, file: &Path) -> OperationResult<()> {
        let mut writer = BufWriter::new(File::create(file)?);

        // Serialize the length
        writer.write_u64::<Byteorder>(self.internal_to_external.len() as u64)?;

        for external_id in self.internal_to_external.iter() {
            // Serializing External ID
            match external_id {
                PointIdType::NumId(num) => {
                    writer.write_u8(Self::EXTERNAL_ID_NUMBER_BYTE)?;
                    writer.write_u64::<Byteorder>(*num)?;
                }
                PointIdType::Uuid(uuid) => {
                    writer.write_u8(Self::EXTERNAL_ID_UUID_BYTE)?;
                    writer.write_u128::<Byteorder>(uuid.to_u128_le())?;
                }
            }

            let internal_id = match external_id {
                PointIdType::NumId(n) => self.external_to_internal_num.get(n),
                PointIdType::Uuid(u) => self.external_to_internal_uuid.get(u),
            }
            .unwrap();

            // Serializing Internal ID
            writer.write_u32::<Byteorder>(*internal_id)?;
        }

        writer.flush()?;
        Ok(())
    }
}

impl ImmutableIdTracker {
    pub fn open(segment_path: &Path) -> OperationResult<Self> {
        let deleted_raw = open_write_mmap(&Self::deleted_file_path(segment_path))?;
        let deleted_mmap = MmapBitSlice::try_from(deleted_raw, 0)?;
        let deleted_bitvec = deleted_mmap.to_bitvec();
        let deleted_wrapper = MmapBitSliceBufferedUpdateWrapper::new(deleted_mmap);

        let internal_to_version_map =
            open_write_mmap(&Self::version_mapping_file_path(segment_path))?;
        let internal_to_version_mapslice: MmapSlice<SeqNumberType> =
            unsafe { MmapSlice::try_from(internal_to_version_map)? };
        let internal_to_version = internal_to_version_mapslice.to_vec();
        let internal_to_version_wrapper =
            MmapSliceBufferedUpdateWrapper::new(internal_to_version_mapslice);

        let mappings = PointMappings::load(
            &Self::mappings_file_path(segment_path),
            Some(&deleted_bitvec),
        )?;

        Ok(Self {
            path: segment_path.to_path_buf(),
            deleted: deleted_bitvec,
            deleted_wrapper,
            internal_to_version_wrapper,
            internal_to_version,
            mappings,
        })
    }

    pub(super) fn new(
        path: &Path,
        deleted: &BitSlice,
        internal_to_version: &[SeqNumberType],
        mappings: PointMappings,
    ) -> OperationResult<Self> {
        // Create mmap file for deleted bitvec
        let deleted_filepath = Self::deleted_file_path(path);
        {
            let deleted_size = bitmap_mmap_size(deleted);
            create_and_ensure_length(&deleted_filepath, deleted_size)?;
        }

        let deleted_mmap = open_write_mmap(&deleted_filepath)?;
        let mut deleted_new = MmapBitSlice::try_from(deleted_mmap, 0)?;
        deleted_new[..deleted.len()].copy_from_bitslice(deleted);
        let deleted_wrapper = MmapBitSliceBufferedUpdateWrapper::new(deleted_new);

        // Create mmap file for internal-to-version list
        let version_filepath = Self::version_mapping_file_path(path);
        {
            let version_size = size_of_val(internal_to_version);
            create_and_ensure_length(&version_filepath, version_size)?;
        }
        let mut internal_to_version_wrapper =
            unsafe { MmapSlice::try_from(open_write_mmap(&version_filepath)?)? };
        internal_to_version_wrapper.copy_from_slice(internal_to_version);
        let internal_to_version = internal_to_version_wrapper.to_vec();
        let internal_to_version_wrapper =
            MmapSliceBufferedUpdateWrapper::new(internal_to_version_wrapper);

        // Write mappings to disk.
        mappings.store(&Self::mappings_file_path(path))?;

        Ok(Self {
            path: path.to_path_buf(),
            deleted: deleted.to_bitvec(),
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

/// Returns the required mmap filesize for a `BitSlice`.
fn bitmap_mmap_size(deleted: &BitSlice) -> usize {
    let usize_bytes = std::mem::size_of::<usize>();
    let num_bytes = deleted.len().div_ceil(8); // used bytes
    num_bytes.div_ceil(usize_bytes) * usize_bytes // Make it a multiple of usize-width.
}

impl IdTracker for ImmutableIdTracker {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id as usize).copied()
    }

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        if self.external_id(internal_id).is_some() {
            if let Some(old_version) = self.internal_to_version.get_mut(internal_id as usize) {
                *old_version = version;
                self.internal_to_version_wrapper
                    .set(internal_id as usize, version);
            }
        }

        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        match external_id {
            PointIdType::NumId(num) => self.mappings.external_to_internal_num.get(&num).copied(),
            PointIdType::Uuid(uuid) => self.mappings.external_to_internal_uuid.get(&uuid).copied(),
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        if *self.deleted.get(internal_id as usize)? {
            return None;
        }

        self.mappings
            .internal_to_external
            .get(internal_id as usize)
            .map(|i| i.into())
    }

    fn set_link(
        &mut self,
        _external_id: PointIdType,
        _internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        panic!("Trying to call a mutating function (`set_link`) of an immutable id tracker");
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        let internal_id = match external_id {
            // We "temporarily" remove existing points from the BTreeMaps without writing them to disk
            // because we remove deleted points of a previous load directly when loading.
            PointIdType::NumId(num) => self.mappings.external_to_internal_num.remove(&num),
            PointIdType::Uuid(uuid) => self.mappings.external_to_internal_uuid.remove(&uuid),
        };

        if let Some(internal_id) = internal_id {
            self.deleted.set(internal_id as usize, true);
            self.deleted_wrapper.set(internal_id as usize, true);
        }

        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        let iter_num = self
            .mappings
            .external_to_internal_num
            .keys()
            .map(|i| PointIdType::NumId(*i));

        let iter_uuid = self
            .mappings
            .external_to_internal_uuid
            .keys()
            .map(|i| PointIdType::Uuid(*i));
        // order is important here, we want to iterate over the u64 ids first
        Box::new(iter_num.chain(iter_uuid))
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(
            (0..self.mappings.internal_to_external.len() as PointOffsetType)
                .filter(move |i| !self.deleted[*i as usize]),
        )
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        let full_num_iter = || {
            self.mappings
                .external_to_internal_num
                .iter()
                .map(|(k, v)| (PointIdType::NumId(*k), *v))
        };
        let offset_num_iter = |offset: u64| {
            self.mappings
                .external_to_internal_num
                .range(offset..)
                .map(|(k, v)| (PointIdType::NumId(*k), *v))
        };
        let full_uuid_iter = || {
            self.mappings
                .external_to_internal_uuid
                .iter()
                .map(|(k, v)| (PointIdType::Uuid(*k), *v))
        };
        let offset_uuid_iter = |offset: Uuid| {
            self.mappings
                .external_to_internal_uuid
                .range(offset..)
                .map(|(k, v)| (PointIdType::Uuid(*k), *v))
        };

        match external_id {
            None => {
                let iter_num = full_num_iter();
                let iter_uuid = full_uuid_iter();
                // order is important here, we want to iterate over the u64 ids first
                Box::new(iter_num.chain(iter_uuid))
            }
            Some(offset) => match offset {
                PointIdType::NumId(idx) => {
                    // Because u64 keys are less that uuid key, we can just use the full iterator for uuid
                    let iter_num = offset_num_iter(idx);
                    let iter_uuid = full_uuid_iter();
                    // order is important here, we want to iterate over the u64 ids first
                    Box::new(iter_num.chain(iter_uuid))
                }
                PointIdType::Uuid(uuid) => {
                    // if offset is a uuid, we can only iterate over uuids
                    Box::new(offset_uuid_iter(uuid))
                }
            },
        }
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
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
        self.mappings.internal_to_external.len()
    }

    fn available_point_count(&self) -> usize {
        self.mappings.external_to_internal_num.len() + self.mappings.external_to_internal_uuid.len()
    }

    fn deleted_point_count(&self) -> usize {
        self.total_point_count() - self.available_point_count()
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        &self.deleted
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        let key = key as usize;
        if key >= self.deleted.len() {
            return true;
        }
        self.deleted[key]
    }

    fn name(&self) -> &'static str {
        "immutable id tracker"
    }

    fn cleanup_versions(&mut self) -> OperationResult<()> {
        let mut to_remove = Vec::new();
        for internal_id in self.iter_internal() {
            if self.internal_version(internal_id).is_none() {
                if let Some(external_id) = self.external_id(internal_id) {
                    to_remove.push(external_id);
                } else {
                    debug_assert!(false, "internal id {} has no external id", internal_id);
                }
            }
        }
        for external_id in to_remove {
            self.drop(external_id)?;
            #[cfg(debug_assertions)] // Only for dev builds
            {
                log::debug!("dropped version for point {} without version", external_id);
            }
        }
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![
            Self::deleted_file_path(&self.path),
            Self::mappings_file_path(&self.path),
            Self::version_mapping_file_path(&self.path),
        ]
    }
}

/*
impl<'de> Deserialize<'de> for PointMappings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(PointMappingVisitor)
    }
}

impl Serialize for PointMappings {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.internal_to_external.len()))?;

        for external_id in self.internal_to_external.iter() {
            let internal_id = match external_id {
                StoredPointId::NumId(n) => self.external_to_internal_num.get(n),
                StoredPointId::Uuid(u) => self.external_to_internal_uuid.get(u),
                StoredPointId::String(str) => {
                    unimplemented!("cannot convert internal string id '{str}' to external id")
                }
            }
            .unwrap();

            sequence.serialize_element(&(external_id, internal_id))?;
        }

        sequence.end()
    }
}

mod point_mappings_deser {
    use std::collections::BTreeMap;
    use std::fmt::Formatter;

    use common::types::PointOffsetType;
    use serde::de::{SeqAccess, Visitor};
    use uuid::Uuid;

    use crate::id_tracker::immutable_id_tracker::PointMappings;
    use crate::id_tracker::simple_id_tracker::StoredPointId;

    pub(super) struct PointMappingVisitor;

    impl<'de> Visitor<'de> for PointMappingVisitor {
        type Value = PointMappings;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of tuples")
        }
        // Custom deserializer to prevent allocating a humongous intermediate list of points
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut internal_to_external = Vec::with_capacity(seq.size_hint().unwrap_or_default());
            let mut external_to_internal_num: BTreeMap<u64, PointOffsetType> = BTreeMap::new();
            let mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType> = BTreeMap::new();

            while let Some((external, internal)) =
                seq.next_element::<(StoredPointId, PointOffsetType)>()?
            {
                match &external {
                    StoredPointId::NumId(num) => {
                        external_to_internal_num.insert(*num, internal);
                    }
                    StoredPointId::Uuid(uuid) => {
                        external_to_internal_uuid.insert(*uuid, internal);
                    }
                    StoredPointId::String(str) => {
                        unimplemented!("cannot convert internal string id '{str}' to external id")
                    }
                }

                internal_to_external.push(external);
            }

            Ok(PointMappings {
                internal_to_external,
                external_to_internal_num,
                external_to_internal_uuid,
            })
        }
    }
}
*/

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
    use crate::id_tracker::IdTrackerEnum;

    #[test]
    fn test_iterator() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut id_tracker = SimpleIdTracker::open(db).unwrap();

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

        let id_tracker = id_tracker.make_immutable(dir.path()).unwrap();

        let first_four = id_tracker.iter_from(None).take(4).collect_vec();

        assert_eq!(first_four.len(), 4);
        assert_eq!(first_four[0].0, 100.into());

        let last = id_tracker.iter_from(Some(first_four[3].0)).collect_vec();
        assert_eq!(last.len(), 7);
    }

    fn make_values() -> Vec<PointIdType> {
        vec![
            100.into(),
            PointIdType::Uuid(Uuid::from_u128(123_u128)),
            PointIdType::Uuid(Uuid::from_u128(156_u128)),
            150.into(),
            120.into(),
            PointIdType::Uuid(Uuid::from_u128(12_u128)),
            180.into(),
            110.into(),
            115.into(),
            PointIdType::Uuid(Uuid::from_u128(673_u128)),
            190.into(),
            177.into(),
            PointIdType::Uuid(Uuid::from_u128(971_u128)),
        ]
    }

    fn make_immutable_tracker(path: &Path) -> ImmutableIdTracker {
        let db = open_db(path, &[DB_VECTOR_CF]).unwrap();

        let mut id_tracker = SimpleIdTracker::open(db).unwrap();

        let values = make_values();

        for (id, value) in values.iter().enumerate() {
            id_tracker.set_link(*value, id as PointOffsetType).unwrap();
        }

        match id_tracker.make_immutable(path).unwrap() {
            IdTrackerEnum::MutableIdTracker(_) => {
                unreachable!()
            }
            IdTrackerEnum::ImmutableIdTracker(m) => {
                m.mapping_flusher()().unwrap();
                m.versions_flusher()().unwrap();
                m
            }
        }
    }

    #[test]
    fn test_mixed_types_iterator() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let id_tracker = make_immutable_tracker(dir.path());

        let sorted_from_tracker = id_tracker.iter_from(None).map(|(k, _)| k).collect_vec();

        let mut values = make_values();
        values.sort();

        assert_eq!(sorted_from_tracker, values);
    }

    #[test]
    fn test_load_store() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let (old_deleted, old_mappings, old_versions) = {
            let id_tracker = make_immutable_tracker(dir.path());
            (
                id_tracker.deleted.to_bitvec(),
                id_tracker.mappings,
                id_tracker.internal_to_version,
            )
        };

        let mut loaded_id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();

        // We may extend the length of deleted bitvec as memory maps need to be aligned to
        // a multiple of `usize-width`.
        assert_eq!(old_deleted, loaded_id_tracker.deleted[..old_deleted.len()]);

        assert_eq!(old_versions, loaded_id_tracker.internal_to_version);

        loaded_id_tracker.drop(PointIdType::NumId(180)).unwrap();
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
        let id_tracker = make_immutable_tracker(dir.path());
        point_deletion_correctness(IdTrackerEnum::ImmutableIdTracker(id_tracker));
    }

    fn point_deletion_correctness(mut id_tracker: IdTrackerEnum) {
        // No deletions yet
        assert_eq!(
            id_tracker.total_point_count(),
            id_tracker.available_point_count()
        );

        let point_to_delete = PointIdType::NumId(100);

        assert!(id_tracker.iter_external().contains(&point_to_delete));

        assert_eq!(id_tracker.internal_id(point_to_delete), Some(0));

        id_tracker.drop(point_to_delete).unwrap();

        assert!(!point_exists(&id_tracker, point_to_delete));

        assert_eq!(
            id_tracker.available_point_count(),
            id_tracker.total_point_count() - 1
        );
    }

    fn point_exists(id_tracker: &IdTrackerEnum, point: PointIdType) -> bool {
        id_tracker.internal_id(point).is_some()
            && id_tracker.iter_external().contains(&point)
            && id_tracker.iter_from(None).any(|i| i.0 == point)
    }

    #[test]
    fn test_point_deletion_persists_reload() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let point_to_delete = PointIdType::NumId(100);

        {
            let mut id_tracker = make_immutable_tracker(dir.path());
            id_tracker.drop(point_to_delete).unwrap();
            id_tracker.versions_flusher()().unwrap();
            id_tracker.mapping_flusher()().unwrap();
        }

        // Point should still be gone
        let id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();
        assert_eq!(id_tracker.internal_id(point_to_delete), None);
    }
}
