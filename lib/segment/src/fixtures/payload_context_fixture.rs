use std::collections::HashMap;
use std::iter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::{BitSlice, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rand::SeedableRng;
use rand::prelude::StdRng;

use super::payload_fixtures::BOOL_KEY;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::fixtures::payload_fixtures::{
    FLT_KEY, GEO_KEY, INT_KEY, STR_KEY, TEXT_KEY, generate_diverse_payload,
};
use crate::id_tracker::IdTracker;
use crate::index::PayloadIndex;
use crate::index::plain_payload_index::PlainPayloadIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::payload_storage::PayloadStorage;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::query_checker::SimpleConditionChecker;
use crate::types::{PayloadSchemaType, PointIdType, SeqNumberType};

/// Warn: Use for tests only
///
/// This struct mimics the interface of `PointsIterator` and `IdTracker` only for basic cases
#[derive(Debug)]
pub struct FixtureIdTracker {
    ids: Vec<PointOffsetType>,
    deleted: BitVec,
    deleted_count: usize,
}

impl FixtureIdTracker {
    pub fn new(num_points: usize) -> Self {
        Self {
            ids: (0..num_points).map(|x| x as PointOffsetType).collect(),
            deleted: BitVec::repeat(false, num_points),
            deleted_count: 0,
        }
    }
}

impl IdTracker for FixtureIdTracker {
    fn internal_version(&self, _internal_id: PointOffsetType) -> Option<SeqNumberType> {
        Some(0)
    }

    fn set_internal_version(
        &mut self,
        _internal_id: PointOffsetType,
        _version: SeqNumberType,
    ) -> OperationResult<()> {
        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        match external_id {
            PointIdType::NumId(id) => {
                assert!(id < self.ids.len() as u64);
                let internal_id = id as PointOffsetType;
                (!self.is_deleted_point(internal_id)).then_some(internal_id)
            }
            PointIdType::Uuid(_) => unreachable!(),
        }
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        assert!(internal_id < self.ids.len() as PointOffsetType);
        let external_id = PointIdType::NumId(u64::from(internal_id));
        (!self.is_deleted_point(internal_id)).then_some(external_id)
    }

    fn set_link(
        &mut self,
        _external_id: PointIdType,
        _internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        let internal_id = self.internal_id(external_id).unwrap();
        self.drop_internal(internal_id)
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        if !self.deleted.replace(internal_id as usize, true) {
            self.deleted_count += 1;
        }
        self.set_internal_version(internal_id, 0)?;
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        Box::new(
            self.ids
                .iter()
                .copied()
                .filter(|internal_id| !self.is_deleted_point(*internal_id))
                .map(|internal_id| PointIdType::NumId(u64::from(internal_id))),
        )
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(
            self.ids
                .iter()
                .copied()
                .filter(|internal_id| !self.is_deleted_point(*internal_id)),
        )
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        let start = match external_id {
            None => 0,
            Some(id) => match id {
                PointIdType::NumId(num) => num,
                PointIdType::Uuid(_) => unreachable!(),
            },
        } as PointOffsetType;

        Box::new(
            self.ids
                .iter()
                .copied()
                .skip_while(move |internal_id| *internal_id < start)
                .filter(|internal_id| !self.is_deleted_point(*internal_id))
                .map(|internal_id| (PointIdType::NumId(u64::from(internal_id)), internal_id)),
        )
    }

    fn iter_random(&self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        unimplemented!("Not used for tests yet")
    }

    fn total_point_count(&self) -> usize {
        self.ids.len()
    }

    fn deleted_point_count(&self) -> usize {
        self.deleted_count
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(
            self.ids
                .iter()
                .copied()
                .filter(|id| !self.is_deleted_point(*id)),
        )
    }

    fn mapping_flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    fn versions_flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        let key = key as usize;
        if key >= self.deleted.len() {
            return true;
        }
        self.deleted[key]
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        &self.deleted
    }

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        Box::new(iter::empty())
    }

    fn fix_inconsistencies(&mut self) -> OperationResult<Vec<PointOffsetType>> {
        // This structure does not support cleaning up orphan versions
        Ok(vec![])
    }

    fn name(&self) -> &'static str {
        "fixture id tracker"
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }
}

/// Creates in-memory payload storage and fills it with random points
///
/// # Arguments
///
/// * `num_points` - how many random points to insert
///
/// # Result
///
/// Payload storage fixture
///
pub fn create_payload_storage_fixture(num_points: usize, seed: u64) -> InMemoryPayloadStorage {
    let mut payload_storage = InMemoryPayloadStorage::default();
    let mut rng = StdRng::seed_from_u64(seed);

    let hw_counter = HardwareCounterCell::new();

    for id in 0..num_points {
        let payload = generate_diverse_payload(&mut rng);
        payload_storage
            .set(id as PointOffsetType, &payload, &hw_counter)
            .unwrap();
    }

    payload_storage
}

/// Function generates `PlainPayloadIndex` with random payload for testing
///
/// # Arguments
///
/// * `path` - temp directory path
/// * `num_points` - how many payloads generate?
///
/// # Result
///
/// `PlainPayloadIndex`
///
pub fn create_plain_payload_index(path: &Path, num_points: usize, seed: u64) -> PlainPayloadIndex {
    let payload_storage = create_payload_storage_fixture(num_points, seed);
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(num_points)));

    let condition_checker = Arc::new(SimpleConditionChecker::new(
        Arc::new(AtomicRefCell::new(payload_storage.into())),
        id_tracker.clone(),
        HashMap::new(),
    ));

    PlainPayloadIndex::open(condition_checker, id_tracker, path).unwrap()
}

/// Function generates `StructPayloadIndex` with random payload for testing.
/// It will also create indexes for payloads
///
/// # Arguments
///
/// * `path` - temp directory path
/// * `num_points` - how many payloads generate?
///
/// # Result
///
/// `StructPayloadIndex`
///
pub fn create_struct_payload_index(
    path: &Path,
    num_points: usize,
    seed: u64,
) -> StructPayloadIndex {
    let payload_storage = Arc::new(AtomicRefCell::new(
        create_payload_storage_fixture(num_points, seed).into(),
    ));
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(num_points)));

    let mut index = StructPayloadIndex::open(
        payload_storage,
        id_tracker,
        std::collections::HashMap::new(),
        path,
        true,
        true,
    )
    .unwrap();

    let hw_counter = HardwareCounterCell::new();

    index
        .set_indexed(
            &STR_KEY.parse().unwrap(),
            PayloadSchemaType::Keyword,
            &hw_counter,
        )
        .unwrap();
    index
        .set_indexed(
            &INT_KEY.parse().unwrap(),
            PayloadSchemaType::Integer,
            &hw_counter,
        )
        .unwrap();
    index
        .set_indexed(
            &FLT_KEY.parse().unwrap(),
            PayloadSchemaType::Float,
            &hw_counter,
        )
        .unwrap();
    index
        .set_indexed(
            &GEO_KEY.parse().unwrap(),
            PayloadSchemaType::Geo,
            &hw_counter,
        )
        .unwrap();
    index
        .set_indexed(
            &TEXT_KEY.parse().unwrap(),
            PayloadSchemaType::Text,
            &hw_counter,
        )
        .unwrap();
    index
        .set_indexed(
            &BOOL_KEY.parse().unwrap(),
            PayloadSchemaType::Bool,
            &hw_counter,
        )
        .unwrap();

    index
}
