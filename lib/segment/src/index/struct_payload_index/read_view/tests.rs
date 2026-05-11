//! Smoke tests that construct `StructPayloadIndexReadView` directly,
//! without going through `StructPayloadIndex::with_view`.
//!
//! Proves the view is genuinely decoupled from the writable index --
//! exactly the property that PR 4 needs to wire a read-only segment
//! through the view.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;

use crate::common::utils::IndexesMap;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::index::PayloadIndexRead;
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::index::visited_pool::VisitedPool;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::types::{Filter, VectorNameBuf};
use crate::vector_storage::VectorStorageEnum;

/// Build a view directly over in-memory backends and exercise the
/// `PayloadIndexRead` surface. No `StructPayloadIndex` involved.
#[test]
fn smoke_view_over_in_memory_backends() {
    let payload: Arc<AtomicRefCell<InMemoryPayloadStorage>> =
        Arc::new(AtomicRefCell::new(InMemoryPayloadStorage::default()));
    let id_tracker = InMemoryIdTracker::new();
    let vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>> =
        HashMap::new();
    let field_indexes = IndexesMap::new();
    let config = PayloadConfig::default();
    let visited_pool = VisitedPool::new();

    let view: StructPayloadIndexReadView<'_, _, _, VectorStorageEnum, _> =
        StructPayloadIndexReadView {
            payload: &payload,
            id_tracker: &id_tracker,
            vector_storages: &vector_storages,
            field_indexes: &field_indexes,
            config: &config,
            visited_pool: &visited_pool,
        };

    let hw_counter = HardwareCounterCell::new();
    let is_stopped = AtomicBool::new(false);

    // `indexed_fields` reads from `config.indices`.
    let indexed = view.indexed_fields();
    assert!(indexed.is_empty(), "no indexed fields configured");

    // `query_points` over an empty filter on an empty tracker returns nothing.
    let empty_filter = Filter::default();
    let result = view
        .query_points(&empty_filter, &hw_counter, &is_stopped, None)
        .expect("query_points");
    assert!(result.is_empty(), "no points in tracker");

    // `available_point_count` (inherent helper) reflects the empty tracker.
    assert_eq!(view.available_point_count(), 0);
}
