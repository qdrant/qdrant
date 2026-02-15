use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rand::SeedableRng;
use rand::prelude::StdRng;

use super::payload_fixtures::BOOL_KEY;
use crate::fixtures::payload_fixtures::{
    FLT_KEY, GEO_KEY, INT_KEY, STR_KEY, TEXT_KEY, generate_diverse_payload,
};
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::{IdTracker, IdTrackerEnum};
use crate::index::PayloadIndex;
use crate::index::plain_payload_index::PlainPayloadIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::payload_storage::PayloadStorage;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::query_checker::SimpleConditionChecker;
use crate::types::{PayloadSchemaType, PointIdType};

/// Creates a simple `IdTrackerEnum` for testing with sequential point IDs (0..num_points).
pub fn create_id_tracker_fixture(num_points: usize) -> IdTrackerEnum {
    let mut id_tracker = InMemoryIdTracker::new();
    for i in 0..num_points {
        let external_id = PointIdType::NumId(i as u64);
        let internal_id = i as PointOffsetType;
        id_tracker.set_link(external_id, internal_id).unwrap();
        id_tracker.set_internal_version(internal_id, 0).unwrap();
    }
    IdTrackerEnum::InMemoryIdTracker(id_tracker)
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
    let id_tracker = Arc::new(AtomicRefCell::new(create_id_tracker_fixture(num_points)));

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
    let id_tracker = Arc::new(AtomicRefCell::new(create_id_tracker_fixture(num_points)));

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
