use std::path::{Path, PathBuf};

use bitvec::prelude::BitSlice;
use bitvec::vec::BitVec;
use common::types::PointOffsetType;
use uuid::Uuid;

use super::immutable_id_tracker::{ImmutableIdTracker, PointMappings};
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::id_tracker::IdTracker;
use crate::types::{PointIdType, SeqNumberType};

#[derive(Debug, Default)]
pub struct InMemoryIdTracker {
    deleted: BitVec,
    internal_to_version: Vec<SeqNumberType>,
    mappings: PointMappings,
}

impl InMemoryIdTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn make_immutable(mut self, segment_path: &Path) -> OperationResult<ImmutableIdTracker> {
        let internal_to_external_len = self.mappings.internal_to_external.len();
        if self.internal_to_version.len() < internal_to_external_len {
            self.internal_to_version.resize(internal_to_external_len, 0);
        }

        ImmutableIdTracker::new(
            segment_path,
            &self.deleted,
            &self.internal_to_version,
            self.mappings,
        )
    }
}

impl IdTracker for InMemoryIdTracker {
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
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        match external_id {
            PointIdType::NumId(idx) => {
                self.mappings
                    .external_to_internal_num
                    .insert(idx, internal_id);
            }
            PointIdType::Uuid(uuid) => {
                self.mappings
                    .external_to_internal_uuid
                    .insert(uuid, internal_id);
            }
        }

        let internal_id = internal_id as usize;
        if internal_id >= self.mappings.internal_to_external.len() {
            self.mappings
                .internal_to_external
                .resize(internal_id + 1, PointIdType::NumId(u64::MAX));
        }
        if internal_id >= self.deleted.len() {
            self.deleted.resize(internal_id + 1, true);
        }
        self.mappings.internal_to_external[internal_id] = external_id;
        self.deleted.set(internal_id, false);

        Ok(())
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
        // self.deleted_wrapper.flusher()
        Box::new(|| Ok(()))
    }

    /// Creates a flusher function, that writes the points versions to disk.
    fn versions_flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
        // self.internal_to_version_wrapper.flusher()
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
        "in memory id tracker"
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
        vec![]
    }
}

#[cfg(test)]
mod test {
    use tempfile::Builder;

    use super::super::immutable_id_tracker::test::TEST_POINTS;
    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
    use crate::id_tracker::IdTrackerEnum;

    fn make_in_memory_tracker(path: &Path) -> ImmutableIdTracker {
        let mut id_tracker = InMemoryIdTracker::new();

        for (id, value) in TEST_POINTS.iter().enumerate() {
            id_tracker.set_link(*value, id as PointOffsetType).unwrap();
        }

        match id_tracker.make_immutable(path).unwrap() {
            IdTrackerEnum::ImmutableIdTracker(id_tracker) => id_tracker,
            _ => unreachable!(),
        }
    }

    fn make_immutable_tracker(path: &Path) -> ImmutableIdTracker {
        let db = open_db(path, &[DB_VECTOR_CF]).unwrap();

        let mut id_tracker = SimpleIdTracker::open(db).unwrap();

        for (id, value) in TEST_POINTS.iter().enumerate() {
            id_tracker.set_link(*value, id as PointOffsetType).unwrap();
        }

        match id_tracker.make_immutable(path).unwrap() {
            IdTrackerEnum::ImmutableIdTracker(m) => {
                m.mapping_flusher()().unwrap();
                m.versions_flusher()().unwrap();
                m
            }
            _ => {
                unreachable!()
            }
        }
    }

    #[test]
    fn test_idtracker_equal() {
        let in_memory_dir = Builder::new()
            .prefix("storage_dir_memory")
            .tempdir()
            .unwrap();
        let in_memory_idtracker = make_in_memory_tracker(in_memory_dir.path());

        let immutable_id_tracker_dir = Builder::new()
            .prefix("storage_dir_immutable")
            .tempdir()
            .unwrap();
        let immutable_id_tracker = make_immutable_tracker(immutable_id_tracker_dir.path());

        assert_eq!(
            in_memory_idtracker.available_point_count(),
            immutable_id_tracker.available_point_count()
        );
        assert_eq!(
            in_memory_idtracker.total_point_count(),
            immutable_id_tracker.total_point_count()
        );

        for (internal, external) in TEST_POINTS.iter().enumerate() {
            let internal = internal as PointOffsetType;

            assert_eq!(
                in_memory_idtracker.internal_id(*external),
                immutable_id_tracker.internal_id(*external)
            );

            assert_eq!(
                in_memory_idtracker.internal_version(internal),
                immutable_id_tracker.internal_version(internal)
            );

            assert_eq!(
                in_memory_idtracker.external_id(internal),
                immutable_id_tracker.external_id(internal)
            );
        }
    }
}
