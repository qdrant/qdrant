use crate::entry::entry_point::OperationResult;
use crate::fixtures::payload_fixtures::{
    generate_diverse_payload, FLT_KEY, GEO_KEY, INT_KEY, STR_KEY,
};
use crate::id_tracker::points_iterator::PointsIterator;
use crate::id_tracker::IdTracker;
use crate::index::plain_payload_index::PlainPayloadIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::PayloadIndex;
use crate::payload_storage::in_memory_payload_storage::{
    InMemoryConditionChecker, InMemoryPayloadStorage,
};
use crate::payload_storage::PayloadStorage;
use crate::types::{PayloadSchemaType, PointIdType, PointOffsetType, SeqNumberType};
use atomic_refcell::AtomicRefCell;
use rand::prelude::StdRng;
use rand::SeedableRng;
use std::path::Path;
use std::sync::Arc;

/// Warn: Use for tests only
///
/// This struct mimics the interface of `PointsIterator` and `IdTracker` only for basic cases
struct IdsIterator {
    ids: Vec<PointOffsetType>,
}

impl IdsIterator {
    pub fn new(num_points: usize) -> Self {
        Self {
            ids: (0..num_points).map(|x| x as PointOffsetType).collect(),
        }
    }
}

impl PointsIterator for IdsIterator {
    fn points_count(&self) -> usize {
        self.ids.len()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(self.ids.iter().copied())
    }

    fn max_id(&self) -> PointOffsetType {
        self.ids.last().copied().unwrap()
    }
}

impl IdTracker for IdsIterator {
    fn version(&self, _external_id: PointIdType) -> Option<SeqNumberType> {
        Some(0)
    }

    fn set_version(
        &mut self,
        _external_id: PointIdType,
        _version: SeqNumberType,
    ) -> OperationResult<()> {
        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        Some(match external_id {
            PointIdType::NumId(id) => id as PointOffsetType,
            PointIdType::Uuid(_) => todo!(),
        })
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        Some(PointIdType::NumId(internal_id as u64))
    }

    fn set_link(
        &mut self,
        _external_id: PointIdType,
        _internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        Ok(())
    }

    fn drop(&mut self, _external_id: PointIdType) -> OperationResult<()> {
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        Box::new(
            self.ids
                .iter()
                .copied()
                .map(|id| PointIdType::NumId(id as u64)),
        )
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(self.ids.iter().copied())
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        let start = match external_id {
            None => 0,
            Some(id) => match id {
                PointIdType::NumId(num) => num,
                PointIdType::Uuid(_) => todo!(),
            },
        } as PointOffsetType;

        Box::new(
            self.ids
                .iter()
                .copied()
                .skip_while(move |x| *x < start)
                .map(|x| (PointIdType::NumId(x as u64), x)),
        )
    }

    fn flush(&self) -> OperationResult<()> {
        Ok(())
    }
}

fn create_payload_storage(num_points: usize) -> InMemoryPayloadStorage {
    let mut payload_storage = InMemoryPayloadStorage::default();
    let mut rng = StdRng::seed_from_u64(42);

    for id in 0..num_points {
        let payload = generate_diverse_payload(&mut rng);
        payload_storage
            .assign(id as PointOffsetType, &payload)
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
pub fn create_plain_payload_index(path: &Path, num_points: usize) -> PlainPayloadIndex {
    let payload_storage = create_payload_storage(num_points);
    let ids_iterator = Arc::new(AtomicRefCell::new(IdsIterator::new(num_points)));

    let condition_checker = Arc::new(InMemoryConditionChecker::new(
        Arc::new(AtomicRefCell::new(payload_storage)),
        ids_iterator.clone(),
    ));

    PlainPayloadIndex::open(condition_checker, ids_iterator, path).unwrap()
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
pub fn create_struct_payload_index(path: &Path, num_points: usize) -> StructPayloadIndex {
    let payload_storage = Arc::new(AtomicRefCell::new(create_payload_storage(num_points)));
    let ids_iterator = Arc::new(AtomicRefCell::new(IdsIterator::new(num_points)));

    let condition_checker = Arc::new(InMemoryConditionChecker::new(
        payload_storage.clone(),
        ids_iterator.clone(),
    ));

    let mut index = StructPayloadIndex::open(
        condition_checker,
        ids_iterator.clone(),
        payload_storage,
        ids_iterator,
        path,
    )
    .unwrap();

    index
        .set_indexed(STR_KEY, PayloadSchemaType::Keyword)
        .unwrap();
    index
        .set_indexed(INT_KEY, PayloadSchemaType::Integer)
        .unwrap();
    index
        .set_indexed(FLT_KEY, PayloadSchemaType::Float)
        .unwrap();
    index.set_indexed(GEO_KEY, PayloadSchemaType::Geo).unwrap();

    index
}
