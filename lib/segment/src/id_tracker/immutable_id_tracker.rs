use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::mem::size_of_val;
use std::path::{Path, PathBuf};

use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::mmap_type::{MmapBitSlice, MmapSlice};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::id_tracker::simple_id_tracker::StoredPointId;
use crate::id_tracker::{IdTracker, IdTrackerEnum};
use crate::types::{PointIdType, SeqNumberType};

pub const DELETED_FILE_NAME: &str = "id_tracker.deleted";
pub const MAPPINGS_FILE_NAME: &str = "id_tracker.mappings";
pub const VERSION_MAPPING_FILE_NAME: &str = "id_tracker.versions";

pub struct ImmutableIdTracker {
    deleted: MmapBitSlice,
    internal_to_version: MmapSlice<SeqNumberType>,
    mappings: PointMappings,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub(super) struct PointMappings {
    pub(crate) internal_to_external: Vec<StoredPointId>,
    pub(crate) external_to_internal: BTreeMap<StoredPointId, PointOffsetType>,
}

impl ImmutableIdTracker {
    pub fn open(segment_path: &Path) -> OperationResult<Self> {
        let mappings: PointMappings =
            Self::deserialize_file(segment_path.join(MAPPINGS_FILE_NAME).as_path())?;

        let deleted_map = open_write_mmap(&Self::deleted_file_path(segment_path))?;
        let deleted = MmapBitSlice::try_from(deleted_map, 0)?;

        let internal_to_version_map =
            open_write_mmap(&segment_path.join(VERSION_MAPPING_FILE_NAME))?;
        let internal_to_version: MmapSlice<SeqNumberType> =
            unsafe { MmapSlice::try_from(internal_to_version_map)? };

        Ok(Self {
            deleted,
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

        let mut deleted_new = MmapBitSlice::try_from(open_write_mmap(&deleted_filepath)?, 0)?;
        deleted_new[..deleted.len()].copy_from_bitslice(deleted);

        // Create mmap file for internal-to-version list
        let version_filepath = Self::version_mapping_file_path(path);
        {
            let version_size = size_of_val(internal_to_version);
            create_and_ensure_length(&version_filepath, version_size)?;
        }
        let mut internal_to_version_new =
            unsafe { MmapSlice::try_from(open_write_mmap(&version_filepath)?)? };
        internal_to_version_new.copy_from_slice(internal_to_version);

        // Serialize the mappings with bincode and write them to disk
        Self::serialize_file(&Self::mappings_file_path(path), &mappings)?;

        Ok(Self {
            deleted: deleted_new,
            internal_to_version: internal_to_version_new,
            mappings,
        })
    }

    fn deserialize_file<T: DeserializeOwned>(file: &Path) -> OperationResult<T> {
        let file = File::open(file)?;
        let reader = BufReader::new(file);
        bincode::deserialize_from(reader).map_err(|err| OperationError::InconsistentStorage {
            description: format!("id tracker can't be deserialized: {:?}", err),
        })
    }

    fn deleted_file_path(base: &Path) -> PathBuf {
        base.join(DELETED_FILE_NAME)
    }

    fn version_mapping_file_path(base: &Path) -> PathBuf {
        base.join(VERSION_MAPPING_FILE_NAME)
    }

    fn mappings_file_path(base: &Path) -> PathBuf {
        base.join(MAPPINGS_FILE_NAME)
    }

    fn serialize_file<T: Serialize>(path: &Path, value: &T) -> OperationResult<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, value).map_err(|err| {
            OperationError::InconsistentStorage {
                description: format!("id tracker can't be serialized: {:?}", err),
            }
        })?;
        Ok(())
    }
}

/// Returns the required mmap filesize for a `BitSlice`.
fn bitmap_mmap_size(deleted: &BitSlice) -> usize {
    let num_bytes = deleted.len().div_ceil(8); // used bytes
    num_bytes.div_ceil(8) * 8 // get next factor of 8
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
            }
        }

        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.mappings
            .external_to_internal
            .get(&external_id.into())
            .copied()
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
        let internal_id = self
            .mappings
            .external_to_internal
            .get(&external_id.into())
            .copied();

        if let Some(internal_id) = internal_id {
            self.deleted.set(internal_id as usize, true);
        }

        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        let iter_num = self
            .mappings
            .external_to_internal
            .keys()
            .filter(|i| i.is_num_id())
            .map(|i| i.into());

        let iter_uuid = self
            .mappings
            .external_to_internal
            .keys()
            .filter(|i| i.is_uuid())
            .map(|i| i.into());
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
                .external_to_internal
                .iter()
                .filter(|i| i.0.is_num_id())
                .map(|(k, v)| (k.into(), *v))
        };
        let offset_num_iter = |offset: u64| {
            self.mappings
                .external_to_internal
                .range(StoredPointId::NumId(offset)..)
                .filter(|i| i.0.is_num_id())
                .map(|(k, v)| (k.into(), *v))
        };
        let full_uuid_iter = || {
            self.mappings
                .external_to_internal
                .iter()
                .filter(|i| i.0.is_uuid())
                .map(|(k, v)| (k.into(), *v))
        };
        let offset_uuid_iter = |offset: Uuid| {
            self.mappings
                .external_to_internal
                .range(StoredPointId::Uuid(offset)..)
                .filter(|i| i.0.is_uuid())
                .map(|(k, v)| (k.into(), *v))
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

    /// Creates a flusher function, that flushes the deleted points bitvec to disk.
    fn mapping_flusher(&self) -> Flusher {
        self.deleted.flusher()
    }

    fn versions_flusher(&self) -> Flusher {
        self.internal_to_version.flusher()
    }

    fn total_point_count(&self) -> usize {
        self.mappings.internal_to_external.len()
    }

    fn available_point_count(&self) -> usize {
        self.mappings.external_to_internal.len()
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

    fn make_immutable(&self, _: &Path) -> OperationResult<IdTrackerEnum> {
        panic!("Can't make an immutable id tracker immutable again");
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
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;

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
            IdTrackerEnum::ImmutableIdTracker(m) => m,
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
    fn tets_load_store() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let (old_deleted, old_mappings) = {
            let id_tracker = make_immutable_tracker(dir.path());
            (id_tracker.deleted.to_bitvec(), id_tracker.mappings)
        };

        let mut loaded_id_tracker = ImmutableIdTracker::open(dir.path()).unwrap();

        assert_eq!(old_mappings, loaded_id_tracker.mappings);
        assert_eq!(old_deleted, loaded_id_tracker.deleted.to_bitvec());

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
}
