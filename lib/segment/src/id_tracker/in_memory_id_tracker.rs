use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ahash::AHashMap;
use bitvec::prelude::BitSlice;
use bitvec::vec::BitVec;
use common::types::PointOffsetType;
use itertools::{Either, Itertools};
use uuid::Uuid;

use super::immutable_id_tracker::{ImmutableIdTracker, PointMappings};
use super::IdTrackerEnum;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::id_tracker::IdTracker;
use crate::types::{ExtendedPointId, PointIdType, SeqNumberType};

/// An in-memory only ID-Tracker for efficient building of new ID-Tracker.
#[derive(Debug, Default)]
pub struct InMemoryIdTracker {
    deleted: BitVec,
    internal_to_version: Vec<SeqNumberType>,

    internal_to_external: Vec<PointIdType>,

    // We use a HashMap here to take advantage from preallocating and faster lookup/insertions
    // which is used a lot in building new ID tracker.
    external_to_internal: AHashMap<ExtendedPointId, PointOffsetType>,
}

impl InMemoryIdTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert `InMemoryIdTracker` to `ImmutableIdTracker` saved to `output_path`.
    pub fn make_immutable(mut self, output_path: &Path) -> OperationResult<IdTrackerEnum> {
        let internal_to_external_len = self.internal_to_external.len();
        if self.internal_to_version.len() < internal_to_external_len {
            self.internal_to_version.resize(internal_to_external_len, 0);
        }

        let (external_to_internal_num, external_to_internal_uuid): (
            BTreeMap<u64, _>,
            BTreeMap<Uuid, _>,
        ) = self
            .external_to_internal
            .into_iter()
            .partition_map(|(ext, int)| match ext {
                ExtendedPointId::NumId(n) => Either::Left((n, int)),
                ExtendedPointId::Uuid(u) => Either::Right((u, int)),
            });

        let mappings = PointMappings {
            internal_to_external: self.internal_to_external,
            external_to_internal_num,
            external_to_internal_uuid,
        };

        let immutable_id_tracker = ImmutableIdTracker::new(
            output_path,
            &self.deleted,
            &self.internal_to_version,
            mappings,
        )?;
        Ok(IdTrackerEnum::ImmutableIdTracker(immutable_id_tracker))
    }

    pub fn reserve(&mut self, size: usize) {
        self.deleted.resize(self.deleted.len() + size, true);
        self.internal_to_version
            .resize(self.internal_to_version.len() + size, 0);
        self.internal_to_external.reserve(size);
        self.external_to_internal.reserve(size);
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
        self.external_to_internal.get(&external_id).copied()
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        if *self.deleted.get(internal_id as usize)? {
            return None;
        }

        self.internal_to_external
            .get(internal_id as usize)
            .map(|i| i.into())
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        self.external_to_internal.insert(external_id, internal_id);

        let internal_id = internal_id as usize;
        if internal_id >= self.internal_to_external.len() {
            self.internal_to_external
                .resize(internal_id + 1, PointIdType::NumId(u64::MAX));
        }
        if internal_id >= self.deleted.len() {
            self.deleted.resize(internal_id + 1, true);
        }
        self.internal_to_external[internal_id] = external_id;
        self.deleted.set(internal_id, false);

        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        let internal_id = self.external_to_internal.remove(&external_id);

        if let Some(internal_id) = internal_id {
            self.deleted.set(internal_id as usize, true);
        }

        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        Box::new(self.external_to_internal.keys().copied())
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(
            (0..self.internal_to_external.len() as PointOffsetType)
                .filter(move |i| !self.deleted[*i as usize]),
        )
    }

    fn iter_from(
        &self,
        _external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        panic!("InMemoryIdTracker doesn't support iterating over ranges!")
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
    }

    fn mapping_flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    fn versions_flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    fn total_point_count(&self) -> usize {
        self.internal_to_external.len()
    }

    fn available_point_count(&self) -> usize {
        self.external_to_internal.len()
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
