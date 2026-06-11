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
use crate::id_tracker::IdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::index::PayloadIndexRead;
use crate::index::field_index::FieldIndex;
use crate::index::field_index::map_index::MapIndex;
use crate::index::payload_config::PayloadConfig;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::index::visited_pool::VisitedPool;
use crate::json_path::JsonPath;
use crate::payload_json;
use crate::payload_storage::PayloadStorage;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, MatchAny, MatchExcept, VectorNameBuf,
};
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
        .query_points(&empty_filter, &hw_counter, &is_stopped)
        .expect("query_points");
    assert!(result.is_empty(), "no points in tracker");

    // `available_point_count` (inherent helper) reflects the empty tracker.
    assert_eq!(view.available_point_count(), 0);
}

/// Regression test for <https://github.com/qdrant/qdrant/issues/9068>.
///
/// `Match::Except` against a keyword-indexed field must include points whose
/// payload value at the same key is *not* a string (e.g. a `bool` or number),
/// because the payload-aware checker in `condition_checker.rs::Match::Except`
/// treats type-mismatched values as passing the except predicate.
///
/// Before the fix the indexed `condition_checker` returned `false` for those
/// points (the keyword index has no entry for them), silently dropping them
/// from the result. The fix falls back to the payload checker per-point when
/// the index has zero values for the point.
#[test]
fn match_except_keyword_index_includes_non_string_payload_values() {
    use tempfile::Builder;

    let hw_counter = HardwareCounterCell::new();

    // Build an in-memory payload storage with mixed value types at `color`:
    //   id 0: "red"           (string — excluded)
    //   id 1: true            (bool   — non-string, should PASS Match::Except)
    //   id 2: "blue"          (string — passes Match::Except)
    //   id 3: {}              (key missing — must NOT match Match::Except)
    //   id 4: ["red", true]   (mixed-type array — the index sees only "red",
    //                          so `check_values_any` rejects the point; the
    //                          payload-aware path must accept it because the
    //                          `true` element satisfies Match::Except)
    let mut storage = InMemoryPayloadStorage::default();
    storage
        .set(0, &payload_json! {"color": "red"}, &hw_counter)
        .unwrap();
    storage
        .set(1, &payload_json! {"color": true}, &hw_counter)
        .unwrap();
    storage
        .set(2, &payload_json! {"color": "blue"}, &hw_counter)
        .unwrap();
    storage.set(3, &payload_json! {}, &hw_counter).unwrap();
    storage
        .set(4, &payload_json! {"color": ["red", true]}, &hw_counter)
        .unwrap();
    let payload = Arc::new(AtomicRefCell::new(storage));

    let mut id_tracker = InMemoryIdTracker::new();
    for i in 0u32..5 {
        id_tracker.set_link(u64::from(i).into(), i).unwrap();
        id_tracker.set_internal_version(i, 0).unwrap();
    }

    // Build a keyword (str-keyed) field index. Only string values are indexed;
    // the bool / missing-key / mixed-array points end up with zero or partial
    // indexed string values — the exact shapes that trigger the bug.
    let temp_dir = Builder::new()
        .prefix("test_except_keyword_mixed_types")
        .tempdir()
        .unwrap();
    let keyword_index = MapIndex::<str>::new_mutable(temp_dir.path().to_path_buf(), true)
        .unwrap()
        .unwrap();
    let mut field_index = FieldIndex::KeywordIndex(keyword_index);
    let red = serde_json::Value::String("red".into());
    let bool_true = serde_json::Value::Bool(true);
    let blue = serde_json::Value::String("blue".into());
    field_index.add_point(0, &[&red], &hw_counter).unwrap();
    // `add_point` for a non-string `Value` against a `MapIndex<str>` is a no-op
    // at the index level (the value is filtered out by `ValueIndexer::get_value`),
    // exactly matching production behavior when a keyword index is created over
    // a mixed-type field.
    field_index
        .add_point(1, &[&bool_true], &hw_counter)
        .unwrap();
    field_index.add_point(2, &[&blue], &hw_counter).unwrap();
    // Point 3 has no value for `color`, so it is not added to the index.
    // Point 4 has a mixed array; only the string element is stored.
    field_index
        .add_point(4, &[&red, &bool_true], &hw_counter)
        .unwrap();

    let mut field_indexes: IndexesMap = IndexesMap::new();
    field_indexes.insert(JsonPath::new("color"), vec![field_index]);

    let vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>> =
        HashMap::new();
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

    let is_stopped = AtomicBool::new(false);

    // Match::Except over ["red"] — expect ids 1 (bool), 2 ("blue"),
    // and 4 (mixed ["red", true] — the `true` element satisfies Except).
    let except_filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("color"),
        Match::Except(MatchExcept {
            except: AnyVariants::Strings(["red".to_string()].into_iter().collect()),
        }),
    )));
    let mut got = view
        .query_points(&except_filter, &hw_counter, &is_stopped)
        .expect("query_points");
    got.sort();
    assert_eq!(
        got,
        vec![1, 2, 4],
        "Match::Except must include the bool-valued point (id 1), the non-red string (id 2), \
         and the mixed-type array (id 4 — `true` satisfies Except); must exclude the red string \
         (id 0) and the missing-key point (id 3)",
    );

    // Parity check: Match::Any is unaffected by the fix.
    let any_filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("color"),
        Match::Any(MatchAny {
            any: AnyVariants::Strings(
                ["red".to_string(), "blue".to_string()]
                    .into_iter()
                    .collect(),
            ),
        }),
    )));
    let mut got = view
        .query_points(&any_filter, &hw_counter, &is_stopped)
        .expect("query_points");
    got.sort();
    assert_eq!(
        got,
        vec![0, 2, 4],
        "Match::Any over strings must match every point whose indexed strings include `red` or \
         `blue` (id 0, id 2, and id 4's `red` element)",
    );
}
