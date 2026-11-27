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

    let is_stopped = AtomicBool::new(false);

    // `indexed_fields` reads from `config.indices`.
    let indexed = view.indexed_fields();
    assert!(indexed.is_empty(), "no indexed fields configured");

    // `query_points` over an empty filter on an empty tracker returns nothing.
    let empty_filter = Filter::default();
    let result = view
        .query_points(&empty_filter, &is_stopped)
        .expect("query_points");
    assert!(result.is_empty(), "no points in tracker");

    // `available_point_count` (inherent helper) reflects the empty tracker.
    assert_eq!(view.available_point_count(), 0);
}

mod lazy_checks_hit_disk_tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use ahash::{AHashMap, AHashSet};
    use atomic_refcell::AtomicRefCell;
    use common::ambient;
    use common::ambient::AmbientContext;
    use common::ambient::hw::HwMetric;
    use common::flags::FeatureFlags;
    use common::generic_consts::AccessPattern;
    use common::types::{PointOffsetType, ScoredPointOffset};
    use common::universal_io::{MmapFile, MmapFs};
    use ordered_float::OrderedFloat;
    use serde_json::{Value, json};
    use tempfile::Builder;
    use uuid::Uuid;

    use crate::common::operation_error::OperationResult;
    use crate::data_types::index::{
        BoolIndexParams, BoolIndexType, DatetimeIndexParams, DatetimeIndexType, FloatIndexParams,
        FloatIndexType, GeoIndexParams, GeoIndexType, IntegerIndexParams, IntegerIndexType,
        KeywordIndexParams, KeywordIndexType, TextIndexParams, TextIndexType, UuidIndexParams,
        UuidIndexType,
    };
    use crate::data_types::query_context::{TextFieldStats, TextQueryContext};
    use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
    use crate::entry::entry_point::{NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry};
    use crate::id_tracker::IdTrackerEnum;
    use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
    use crate::index::PayloadIndexRead;
    use crate::index::condition_checker::ConditionCheckerEnum;
    use crate::index::field_index::facet_index::FacetIndexEnum;
    use crate::index::field_index::full_text_index::{Bm25Accept, Bm25Params};
    use crate::index::field_index::nested_index::ElementConditionChecker;
    use crate::index::field_index::numeric_index::NumericFieldIndex;
    use crate::index::field_index::{
        CardinalityEstimation, FacetIndex, FieldIndexRead, PayloadBlockCondition,
        PayloadFieldIndexRead,
    };
    use crate::index::payload_config::PayloadConfig;
    use crate::index::plain_payload_index::PlainPayloadIndex;
    use crate::index::query_optimization::point_set::{PointSet, PointSetContext};
    use crate::index::query_optimization::point_sink::PointSink;
    use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
    use crate::index::query_optimization::resolved_points::ResolvedPoints;
    use crate::index::struct_payload_index::StructPayloadIndexReadView;
    use crate::index::visited_pool::VisitedPool;
    use crate::json_path::JsonPath;
    use crate::payload_json;
    use crate::payload_storage::PayloadStorageRead;
    use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
    use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
    use crate::payload_storage::payload_storage_impl::PayloadStorageImpl;
    use crate::payload_storage::query_checker::SimpleConditionChecker;
    use crate::segment::Segment;
    use crate::segment::read_only::ReadOnlySegment;
    use crate::segment_constructor::build_segment;
    use crate::segment_constructor::segment_builder::SegmentBuilder;
    use crate::telemetry::PayloadIndexTelemetry;
    use crate::types::{
        AnyVariants, Condition, CustomIdChecker, CustomIdCheckerCondition, DateTimeWrapper,
        Distance, ExtendedPointId, FieldCondition, Filter, GeoBoundingBox, GeoLineString, GeoPoint,
        GeoPolygon, GeoRadius, HasIdCondition, HasVectorCondition, HnswConfig, HnswGlobalConfig,
        Indexes, IsEmptyCondition, IsNullCondition, Match, MatchPhrase, MatchTextAny, Memory,
        MinShould, OwnedPayloadRef, Payload, PayloadField, PayloadFieldSchema, PayloadKeyType,
        PayloadSchemaParams, PayloadStorageType, Range, SegmentConfig, SeqNumberType, Slice,
        SliceCondition, ValuesCount, VectorDataConfig, VectorNameBuf, VectorStorageType,
    };
    use crate::vector_storage::VectorStorageEnum;

    /// Mock payload storage that panics if any payload read is triggered during
    /// `lazy_checks_hit_disk`, proving zero payload I/O.
    struct PanicOnIoPayloadStorage {
        cold: bool,
    }

    impl PayloadStorageRead for PanicOnIoPayloadStorage {
        fn get(&self, _point_offset: PointOffsetType) -> OperationResult<Payload> {
            panic!("lazy_checks_hit_disk must not read payload storage (`get`)")
        }

        fn get_sequential(&self, _point_offset: PointOffsetType) -> OperationResult<Payload> {
            panic!("lazy_checks_hit_disk must not read payload storage (`get_sequential`)")
        }

        fn payload_ref(
            &self,
            _point_offset: PointOffsetType,
        ) -> OperationResult<OwnedPayloadRef<'_>> {
            panic!("lazy_checks_hit_disk must not read payload storage (`payload_ref`)")
        }

        fn read_payloads<P: AccessPattern, U: common::universal_io::UserData>(
            &self,
            _point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
            _callback: impl FnMut(U, Payload) -> OperationResult<()>,
        ) -> OperationResult<()> {
            panic!("lazy_checks_hit_disk must not read payload storage (`read_payloads`)")
        }

        fn read_payloads_raw<P: AccessPattern, U: common::universal_io::UserData>(
            &self,
            _point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
            _callback: impl FnMut(U, Option<&[u8]>) -> OperationResult<()>,
        ) -> OperationResult<()> {
            panic!("lazy_checks_hit_disk must not read payload storage (`read_payloads_raw`)")
        }

        fn read_payload_lens<P: AccessPattern, U: common::universal_io::UserData>(
            &self,
            _point_offsets: impl Iterator<Item = (U, PointOffsetType)>,
            _callback: impl FnMut(U, Option<usize>) -> OperationResult<()>,
        ) -> OperationResult<()> {
            panic!("lazy_checks_hit_disk must not read payload storage (`read_payload_lens`)")
        }

        fn iter<F>(&self, _callback: F) -> OperationResult<()>
        where
            F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
        {
            panic!("lazy_checks_hit_disk must not iterate payload storage")
        }

        fn get_storage_size_bytes(&self) -> OperationResult<usize> {
            Ok(0)
        }

        fn is_cold(&self) -> bool {
            self.cold
        }
    }

    /// Mock field index that answers only pure metadata predicates and panics on any
    /// posting-list materialization, cardinality estimate, or checker construction.
    #[derive(Clone)]
    struct PanicOnIoFieldIndex {
        cold: bool,
        posting: bool,
        checker: bool,
        element_array: Option<JsonPath>,
        special: bool,
    }

    impl PayloadFieldIndexRead for PanicOnIoFieldIndex {
        fn count_indexed_points(&self) -> OperationResult<usize> {
            panic!("lazy_checks_hit_disk must not call count_indexed_points")
        }

        fn filter<'a>(
            &'a self,
            _condition: &'a FieldCondition,
        ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
            panic!("lazy_checks_hit_disk must not materialize postings (`filter`)")
        }

        fn filter_into(
            &self,
            _condition: &FieldCondition,
            _sink: &mut impl PointSink,
        ) -> OperationResult<bool> {
            panic!("lazy_checks_hit_disk must not materialize postings (`filter_into`)")
        }

        fn estimate_cardinality(
            &self,
            _condition: &FieldCondition,
        ) -> OperationResult<Option<CardinalityEstimation>> {
            panic!("lazy_checks_hit_disk must not estimate cardinality")
        }

        fn for_each_payload_block(
            &self,
            _threshold: usize,
            _key: PayloadKeyType,
            _f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
        ) -> OperationResult<()> {
            panic!("lazy_checks_hit_disk must not iterate payload blocks")
        }

        fn condition_checker<'a>(
            &'a self,
            _condition: &FieldCondition,
        ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
            panic!("lazy_checks_hit_disk must not build condition_checker")
        }

        fn special_check_condition(
            &self,
            _condition: &FieldCondition,
            _payload_value: &Value,
        ) -> OperationResult<Option<bool>> {
            panic!("lazy_checks_hit_disk must not invoke special_check_condition")
        }
    }

    impl FieldIndexRead for PanicOnIoFieldIndex {
        fn get_telemetry_data(&self) -> OperationResult<PayloadIndexTelemetry> {
            panic!("lazy_checks_hit_disk must not read telemetry")
        }

        fn is_cold(&self) -> bool {
            self.cold
        }

        fn supports_posting_filter(&self, _condition: &FieldCondition) -> bool {
            self.posting
        }

        fn supports_condition_checker(&self, _condition: &FieldCondition) -> bool {
            self.checker
        }

        fn supports_element_condition(
            &self,
            array_key: &JsonPath,
            _condition: &FieldCondition,
        ) -> bool {
            self.element_array.as_ref() == Some(array_key) && self.checker
        }

        fn supports_special_check(&self, _condition: &FieldCondition) -> bool {
            self.special
        }

        fn values_count(&self, _point_id: PointOffsetType) -> OperationResult<usize> {
            panic!("lazy_checks_hit_disk must not read values_count")
        }

        fn values_is_empty(&self, _point_id: PointOffsetType) -> OperationResult<bool> {
            panic!("lazy_checks_hit_disk must not read values_is_empty")
        }

        fn value_retriever<'a, 'q>(&'a self) -> OperationResult<Option<VariableRetrieverFn<'q>>>
        where
            'a: 'q,
        {
            panic!("lazy_checks_hit_disk must not build value_retriever")
        }

        fn as_numeric(
            &self,
        ) -> Option<impl crate::index::field_index::numeric_index::NumericFieldIndexRead + '_>
        {
            None::<NumericFieldIndex<'_>>
        }

        fn fill_text_statistics(
            &self,
            _stats: &mut TextFieldStats,
            _corpus: Option<&[PointOffsetType]>,
            _is_stopped: &AtomicBool,
        ) -> OperationResult<bool> {
            panic!("lazy_checks_hit_disk must not read text statistics")
        }

        fn score_bm25(
            &self,
            _terms: &[String],
            _context: &TextQueryContext<'_>,
            _params: Bm25Params,
            _accept: &Bm25Accept<'_>,
            _limit: usize,
        ) -> OperationResult<Option<Vec<ScoredPointOffset>>> {
            panic!("lazy_checks_hit_disk must not score bm25")
        }

        fn as_facet_index(&self) -> Option<impl FacetIndex + '_> {
            None::<FacetIndexEnum<'_>>
        }

        fn element_condition_checker<'a>(
            &'a self,
            _array_key: &JsonPath,
            _condition: &FieldCondition,
        ) -> OperationResult<Option<ElementConditionChecker<'a>>> {
            panic!("lazy_checks_hit_disk must not build element_condition_checker")
        }
    }

    #[derive(Debug)]
    struct DummyCustomChecker;

    impl CustomIdCheckerCondition for DummyCustomChecker {
        fn estimate_cardinality(&self, points: usize) -> CardinalityEstimation {
            CardinalityEstimation::unknown(points)
        }

        fn check(&self, _point_id: ExtendedPointId) -> bool {
            true
        }
    }

    #[test]
    fn lazy_checks_hit_disk_never_reads_or_materializes_mock_backends() {
        let id_tracker = InMemoryIdTracker::new();
        let vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>> =
            HashMap::new();
        let config = PayloadConfig::default();
        let visited_pool = VisitedPool::new();

        let mut field_indexes: AHashMap<PayloadKeyType, Vec<PanicOnIoFieldIndex>> = AHashMap::new();
        // Cold index that supports posting materialization (e.g. cold int/keyword/range)
        field_indexes.insert(
            JsonPath::new("cold_materialized"),
            vec![PanicOnIoFieldIndex {
                cold: true,
                posting: true,
                checker: true,
                element_array: None,
                special: false,
            }],
        );
        // Cold index that can check per point but cannot materialize postings (e.g. prefix without dict)
        field_indexes.insert(
            JsonPath::new("cold_checker_only"),
            vec![PanicOnIoFieldIndex {
                cold: true,
                posting: false,
                checker: true,
                element_array: None,
                special: false,
            }],
        );
        // Hot index
        field_indexes.insert(
            JsonPath::new("hot_indexed"),
            vec![PanicOnIoFieldIndex {
                cold: false,
                posting: true,
                checker: true,
                element_array: None,
                special: false,
            }],
        );
        // Index that cannot serve the condition (e.g. values_count -> payload fallback)
        field_indexes.insert(
            JsonPath::new("unsupported_idx"),
            vec![PanicOnIoFieldIndex {
                cold: true,
                posting: false,
                checker: false,
                element_array: None,
                special: false,
            }],
        );
        // Per-element hot index on `hot_arr[]`
        field_indexes.insert(
            JsonPath::new("hot_arr[].a"),
            vec![PanicOnIoFieldIndex {
                cold: false,
                posting: true,
                checker: true,
                element_array: Some(JsonPath::new("hot_arr[]")),
                special: false,
            }],
        );
        // Per-element cold index on `cold_arr[]`
        field_indexes.insert(
            JsonPath::new("cold_arr[].a"),
            vec![PanicOnIoFieldIndex {
                cold: true,
                posting: true,
                checker: true,
                element_array: Some(JsonPath::new("cold_arr[]")),
                special: false,
            }],
        );
        // Flattened cold index with `special_check_condition` (e.g. cold full-text under nested)
        field_indexes.insert(
            JsonPath::new("flat_text[].body"),
            vec![PanicOnIoFieldIndex {
                cold: true,
                posting: true,
                checker: true,
                element_array: None,
                special: true,
            }],
        );

        let cold_payload = Arc::new(AtomicRefCell::new(PanicOnIoPayloadStorage { cold: true }));
        let hot_payload = Arc::new(AtomicRefCell::new(PanicOnIoPayloadStorage { cold: false }));

        let cold_view: StructPayloadIndexReadView<'_, _, _, VectorStorageEnum, _> =
            StructPayloadIndexReadView {
                payload: &cold_payload,
                id_tracker: &id_tracker,
                vector_storages: &vector_storages,
                field_indexes: &field_indexes,
                config: &config,
                visited_pool: &visited_pool,
            };
        let hot_view: StructPayloadIndexReadView<'_, _, _, VectorStorageEnum, _> =
            StructPayloadIndexReadView {
                payload: &hot_payload,
                id_tracker: &id_tracker,
                vector_storages: &vector_storages,
                field_indexes: &field_indexes,
                config: &config,
                visited_pool: &visited_pool,
            };

        let match_cond =
            |k: &str| Condition::Field(FieldCondition::new_match(JsonPath::new(k), 1.into()));
        let geo_cond = |k: &str| {
            Condition::Field(FieldCondition::new_geo_radius(
                JsonPath::new(k),
                GeoRadius {
                    center: GeoPoint::new_unchecked(0.0, 0.0),
                    radius: OrderedFloat(100.0),
                },
            ))
        };
        let values_count_cond = |k: &str| {
            Condition::Field(FieldCondition::new_values_count(
                JsonPath::new(k),
                ValuesCount {
                    lt: None,
                    gt: None,
                    gte: Some(1),
                    lte: None,
                },
            ))
        };

        // Empty filter -> never hits disk.
        assert!(!cold_view.lazy_checks_hit_disk(&Filter::default()));

        // Cold posting-materialized match -> serial path builds in-RAM PointSet -> false.
        assert!(
            !cold_view.lazy_checks_hit_disk(&Filter::new_must(match_cond("cold_materialized")))
        );
        // Same cold index with non-posting condition (geo) -> stays lazy on cold index -> true even on hot payload!
        assert!(hot_view.lazy_checks_hit_disk(&Filter::new_must(geo_cond("cold_materialized"))));
        // Cold checker-only index (e.g. prefix without prefix dict) -> stays lazy -> true even on hot payload!
        assert!(hot_view.lazy_checks_hit_disk(&Filter::new_must(match_cond("cold_checker_only"))));
        // Hot index on cold payload -> stays in RAM -> false.
        assert!(!cold_view.lazy_checks_hit_disk(&Filter::new_must(match_cond("hot_indexed"))));

        // Unsupported index / unindexed field -> payload fallback.
        assert!(cold_view.lazy_checks_hit_disk(&Filter::new_must(match_cond("unsupported_idx"))));
        assert!(!hot_view.lazy_checks_hit_disk(&Filter::new_must(match_cond("unsupported_idx"))));
        assert!(cold_view.lazy_checks_hit_disk(&Filter::new_must(match_cond("unindexed"))));
        assert!(!hot_view.lazy_checks_hit_disk(&Filter::new_must(match_cond("unindexed"))));

        // IsEmpty / IsNull: indexed hot -> false; indexed cold -> true; unindexed -> payload.is_cold().
        let is_empty = |k: &str| {
            Condition::IsEmpty(IsEmptyCondition {
                is_empty: PayloadField {
                    key: JsonPath::new(k),
                },
            })
        };
        let is_null = |k: &str| {
            Condition::IsNull(IsNullCondition {
                is_null: PayloadField {
                    key: JsonPath::new(k),
                },
            })
        };
        assert!(!cold_view.lazy_checks_hit_disk(&Filter::new_must(is_empty("hot_indexed"))));
        assert!(hot_view.lazy_checks_hit_disk(&Filter::new_must(is_empty("cold_materialized"))));
        assert!(cold_view.lazy_checks_hit_disk(&Filter::new_must(is_empty("unindexed"))));
        assert!(!hot_view.lazy_checks_hit_disk(&Filter::new_must(is_empty("unindexed"))));
        assert!(!cold_view.lazy_checks_hit_disk(&Filter::new_must(is_null("hot_indexed"))));
        assert!(hot_view.lazy_checks_hit_disk(&Filter::new_must(is_null("cold_materialized"))));
        assert!(cold_view.lazy_checks_hit_disk(&Filter::new_must(is_null("unindexed"))));
        assert!(!hot_view.lazy_checks_hit_disk(&Filter::new_must(is_null("unindexed"))));

        // Non-payload / in-memory condition variants never hit disk even on cold payload.
        let resolved_cond = Condition::ResolvedPoints(ResolvedPoints::new(
            Uuid::nil(),
            PointSet::union(vec![vec![0, 1]], PointSetContext::by_density(10)),
            &CardinalityEstimation::exact(2),
        ));
        let in_ram_filter = Filter {
            must: Some(vec![
                Condition::HasId(HasIdCondition::from(AHashSet::from_iter([1u64.into()]))),
                Condition::HasVector(HasVectorCondition::from(DEFAULT_VECTOR_NAME.to_owned())),
                Condition::Slice(SliceCondition {
                    slice: Slice {
                        total: NonZeroU32::new(4).unwrap(),
                        index: 0,
                    },
                }),
                Condition::CustomIdChecker(CustomIdChecker(Arc::new(DummyCustomChecker))),
                resolved_cond,
            ]),
            should: None,
            min_should: None,
            must_not: None,
        };
        assert!(!cold_view.lazy_checks_hit_disk(&in_ram_filter));

        // Nested coverage regimes:
        // 1. Hot per-element index covers nested filter even on cold payload -> false.
        let hot_nested = Filter::new_must(Condition::new_nested(
            JsonPath::new("hot_arr"),
            Filter::new_must(match_cond("a")),
        ));
        assert!(!cold_view.lazy_checks_hit_disk(&hot_nested));

        // 2. Cold per-element index covers nested filter -> no posting materialization -> true even on hot payload!
        let cold_nested = Filter::new_must(Condition::new_nested(
            JsonPath::new("cold_arr"),
            Filter::new_must(match_cond("a")),
        ));
        assert!(hot_view.lazy_checks_hit_disk(&cold_nested));

        // 3. Partial per-element coverage (one indexed + one unindexed) -> payload fallback.
        let partial_nested = Filter::new_must(Condition::new_nested(
            JsonPath::new("hot_arr"),
            Filter {
                must: Some(vec![match_cond("a"), match_cond("unindexed")]),
                ..Default::default()
            },
        ));
        assert!(cold_view.lazy_checks_hit_disk(&partial_nested));
        assert!(!hot_view.lazy_checks_hit_disk(&partial_nested));

        // 4. Nested with values_count -> payload fallback (per-element index doesn't count values).
        let values_count_nested = Filter::new_must(Condition::new_nested(
            JsonPath::new("hot_arr"),
            Filter::new_must(values_count_cond("a")),
        ));
        assert!(cold_view.lazy_checks_hit_disk(&values_count_nested));
        assert!(!hot_view.lazy_checks_hit_disk(&values_count_nested));

        // 5. Nested with no field conditions (only HasId -> no input_map) -> payload fallback.
        let no_field_nested = Filter::new_must(Condition::new_nested(
            JsonPath::new("hot_arr"),
            Filter::new_must(Condition::HasId(HasIdCondition::from(AHashSet::from_iter(
                [1u64.into()],
            )))),
        ));
        assert!(cold_view.lazy_checks_hit_disk(&no_field_nested));
        assert!(!hot_view.lazy_checks_hit_disk(&no_field_nested));

        // 6. Double-nested -> payload fallback.
        let double_nested = Filter::new_must(Condition::new_nested(
            JsonPath::new("hot_arr"),
            Filter::new_must(Condition::new_nested(
                JsonPath::new("inner"),
                Filter::new_must(match_cond("x")),
            )),
        ));
        assert!(cold_view.lazy_checks_hit_disk(&double_nested));
        assert!(!hot_view.lazy_checks_hit_disk(&double_nested));

        // 7. Nested payload fallback with hot payload + cold flattened special_check index -> true!
        let flat_text_nested = Filter::new_must(Condition::new_nested(
            JsonPath::new("flat_text"),
            Filter::new_must(match_cond("body")),
        ));
        assert!(hot_view.lazy_checks_hit_disk(&flat_text_nested));

        // Boolean combinators: must, should, must_not, min_should, nested Condition::Filter.
        let cold_leaf = match_cond("unindexed");
        let hot_leaf = match_cond("hot_indexed");
        assert!(cold_view.lazy_checks_hit_disk(&Filter {
            should: Some(vec![hot_leaf.clone(), cold_leaf.clone()]),
            ..Default::default()
        }));
        assert!(cold_view.lazy_checks_hit_disk(&Filter {
            must_not: Some(vec![cold_leaf.clone()]),
            ..Default::default()
        }));
        assert!(cold_view.lazy_checks_hit_disk(&Filter {
            min_should: Some(MinShould {
                conditions: vec![hot_leaf.clone(), cold_leaf.clone()],
                min_count: 1,
            }),
            ..Default::default()
        }));
        assert!(
            cold_view.lazy_checks_hit_disk(&Filter::new_must(Condition::Filter(
                Filter::new_must_not(cold_leaf),
            )))
        );
        assert!(!cold_view.lazy_checks_hit_disk(&Filter {
            must: Some(vec![hot_leaf.clone()]),
            should: Some(vec![match_cond("cold_materialized")]),
            min_should: Some(MinShould {
                conditions: vec![hot_leaf.clone()],
                min_count: 1,
            }),
            must_not: Some(vec![Condition::Filter(Filter::new_must(hot_leaf))]),
        }));
    }

    fn build_typed_segment(
        out_dir: &std::path::Path,
        payload_storage_type: PayloadStorageType,
        index_memory: Memory,
    ) -> Segment {
        let make_config = |index: Indexes| SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: 4,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::InRamChunkedMmap,
                    index,
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type,
            id_tracker_memory: None,
        };

        let plain_dir = Builder::new().prefix("plain_typed").tempdir().unwrap();
        let (mut plain, _) = build_segment(
            plain_dir.path(),
            &make_config(Indexes::Plain {}),
            None,
            true,
        )
        .unwrap();

        let sample_uuid = "550e8400-e29b-41d4-a716-446655440000";
        for n in 0..40u64 {
            let op = n as SeqNumberType + 1;
            plain
                .upsert_point(op, n.into(), only_default_vector(&[1.0, 0.0, 0.0, 0.0]))
                .unwrap();
            let payload = payload_json! {
                "int": (n % 5) as i64,
                "float": (n as f64) * 0.5,
                "dt": "2025-01-15T12:00:00Z",
                "kw": format!("alpha_{}", n % 4),
                "kw_prefix": format!("beta_{}", n % 4),
                "uuid": sample_uuid,
                "geo": {"lon": 13.4 + (n as f64) * 0.001, "lat": 52.5 + (n as f64) * 0.001},
                "text": "quick brown fox jumps over lazy dog",
                "flag": n % 2 == 0,
                "offers": [
                    {"price": (n % 10) as i64, "shop": "shop_a", "desc": "fresh apples daily"},
                ],
                "flat_items": [
                    {"desc": "warm sourdough bread"},
                ],
                "unindexed_items": [
                    {"k": (n % 3) as i64},
                ],
                "unindexed": "plain_val",
            };
            plain.set_full_payload(op, n.into(), &payload).unwrap();
        }

        let op = 100;
        let schemas: Vec<(&str, PayloadFieldSchema)> = vec![
            (
                "int",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Integer(IntegerIndexParams {
                    r#type: IntegerIndexType::Integer,
                    lookup: Some(true),
                    range: Some(true),
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
            (
                "float",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Float(FloatIndexParams {
                    r#type: FloatIndexType::Float,
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
            (
                "dt",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Datetime(
                    DatetimeIndexParams {
                        r#type: DatetimeIndexType::Datetime,
                        memory: Some(index_memory),
                        enable_hnsw: Some(false),
                        ..Default::default()
                    },
                )),
            ),
            (
                "kw",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Keyword(KeywordIndexParams {
                    r#type: KeywordIndexType::Keyword,
                    prefix: Some(false),
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
            (
                "kw_prefix",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Keyword(KeywordIndexParams {
                    r#type: KeywordIndexType::Keyword,
                    prefix: Some(true),
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
            (
                "uuid",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Uuid(UuidIndexParams {
                    r#type: UuidIndexType::Uuid,
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
            (
                "geo",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Geo(GeoIndexParams {
                    r#type: GeoIndexType::Geo,
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
            (
                "text",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
                    r#type: TextIndexType::Text,
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
            (
                "flag",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Bool(BoolIndexParams {
                    r#type: BoolIndexType::Bool,
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
            (
                "offers[].price",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Integer(IntegerIndexParams {
                    r#type: IntegerIndexType::Integer,
                    lookup: Some(true),
                    range: Some(true),
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    nested: Some(JsonPath::new("offers").array_key()),
                    ..Default::default()
                })),
            ),
            (
                "offers[].shop",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Keyword(KeywordIndexParams {
                    r#type: KeywordIndexType::Keyword,
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    nested: Some(JsonPath::new("offers").array_key()),
                    ..Default::default()
                })),
            ),
            (
                "offers[].desc",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
                    r#type: TextIndexType::Text,
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    nested: Some(JsonPath::new("offers").array_key()),
                    ..Default::default()
                })),
            ),
            (
                "flat_items[].desc",
                PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
                    r#type: TextIndexType::Text,
                    memory: Some(index_memory),
                    enable_hnsw: Some(false),
                    ..Default::default()
                })),
            ),
        ];
        for (key, schema) in &schemas {
            plain
                .create_field_index(op, &JsonPath::new(key), Some(schema))
                .unwrap();
        }

        #[expect(deprecated, reason = "HnswConfig still carries the on_disk flag")]
        let hnsw = HnswConfig {
            m: 4,
            ef_construct: 16,
            full_scan_threshold: 1,
            max_indexing_threads: 1,
            on_disk: Some(false),
            memory: None,
            payload_m: None,
            inline_storage: None,
        };
        let temp_dir = Builder::new().prefix("builder_typed").tempdir().unwrap();
        let mut builder = SegmentBuilder::new(
            temp_dir.path(),
            &make_config(Indexes::Hnsw(hnsw)),
            &HnswGlobalConfig::default(),
            FeatureFlags::default(),
        )
        .unwrap();
        builder.update(&[&plain], &AtomicBool::new(false)).unwrap();
        let segment = builder.build_for_test(out_dir);
        assert!(!segment.is_appendable());
        segment
    }

    #[test]
    fn lazy_checks_hit_disk_on_real_cold_and_hot_segments_and_zero_hw_io() {
        let _scope = ambient::test_guard();
        let d_cc = Builder::new().prefix("cc").tempdir().unwrap();
        let d_hc = Builder::new().prefix("hc").tempdir().unwrap();
        let d_ch = Builder::new().prefix("ch").tempdir().unwrap();

        let cold_both = build_typed_segment(d_cc.path(), PayloadStorageType::Mmap, Memory::Cold);
        let hot_payload_cold_idx =
            build_typed_segment(d_hc.path(), PayloadStorageType::InRamMmap, Memory::Cold);
        let cold_payload_hot_idx =
            build_typed_segment(d_ch.path(), PayloadStorageType::Mmap, Memory::Cached);

        let sample_uuid = "550e8400-e29b-41d4-a716-446655440000";
        let dt = DateTimeWrapper::from(
            chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
                .unwrap()
                .to_utc(),
        );

        // (name, filter, expected_on_cold_both, expected_on_hot_payload_cold_idx, expected_on_cold_payload_hot_idx)
        let cases: Vec<(&str, Filter, bool, bool, bool)> = vec![
            // Materialized cold match / range / text / bool / substring / prefix-with-dict -> false everywhere
            (
                "int_match",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("int"),
                    2.into(),
                ))),
                false,
                false,
                false,
            ),
            (
                "int_range",
                Filter::new_must(Condition::Field(FieldCondition::new_range(
                    JsonPath::new("int"),
                    Range {
                        gte: Some(OrderedFloat(1.0)),
                        ..Default::default()
                    },
                ))),
                false,
                false,
                false,
            ),
            (
                "float_range",
                Filter::new_must(Condition::Field(FieldCondition::new_range(
                    JsonPath::new("float"),
                    Range {
                        gte: Some(OrderedFloat(1.0)),
                        ..Default::default()
                    },
                ))),
                false,
                false,
                false,
            ),
            (
                "datetime_range",
                Filter::new_must(Condition::Field(FieldCondition::new_datetime_range(
                    JsonPath::new("dt"),
                    Range {
                        lt: None,
                        gt: None,
                        gte: Some(dt),
                        lte: None,
                    },
                ))),
                false,
                false,
                false,
            ),
            (
                "keyword_match",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("kw"),
                    "alpha_1".to_owned().into(),
                ))),
                false,
                false,
                false,
            ),
            (
                "keyword_substring",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("kw"),
                    Match::new_substring("pha"),
                ))),
                false,
                false,
                false,
            ),
            (
                "keyword_prefix_with_dict",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("kw_prefix"),
                    Match::new_prefix("beta_"),
                ))),
                false,
                false,
                false,
            ),
            (
                "uuid_match",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("uuid"),
                    sample_uuid.to_owned().into(),
                ))),
                false,
                false,
                false,
            ),
            (
                "bool_match",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("flag"),
                    true.into(),
                ))),
                false,
                false,
                false,
            ),
            (
                "full_text_top_level_materialized",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("text"),
                    Match::new_text("brown"),
                ))),
                false,
                false,
                false,
            ),
            // Unmaterialized cold field indexes -> true when index is cold, false when hot
            (
                "keyword_prefix_without_dict",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("kw"),
                    Match::new_prefix("alpha_"),
                ))),
                true,
                true,
                false,
            ),
            (
                "geo_radius",
                Filter::new_must(Condition::Field(FieldCondition::new_geo_radius(
                    JsonPath::new("geo"),
                    GeoRadius {
                        center: GeoPoint::new_unchecked(13.4, 52.5),
                        radius: OrderedFloat(50_000.0),
                    },
                ))),
                true,
                true,
                false,
            ),
            (
                "geo_bbox",
                Filter::new_must(Condition::Field(FieldCondition::new_geo_bounding_box(
                    JsonPath::new("geo"),
                    GeoBoundingBox {
                        top_left: GeoPoint::new_unchecked(13.0, 53.0),
                        bottom_right: GeoPoint::new_unchecked(14.0, 52.0),
                    },
                ))),
                true,
                true,
                false,
            ),
            (
                "geo_polygon",
                Filter::new_must(Condition::Field(FieldCondition::new_geo_polygon(
                    JsonPath::new("geo"),
                    GeoPolygon {
                        exterior: GeoLineString {
                            points: vec![
                                GeoPoint::new_unchecked(13.0, 52.0),
                                GeoPoint::new_unchecked(14.0, 52.0),
                                GeoPoint::new_unchecked(14.0, 53.0),
                                GeoPoint::new_unchecked(13.0, 53.0),
                                GeoPoint::new_unchecked(13.0, 52.0),
                            ],
                        },
                        interiors: None,
                    },
                ))),
                true,
                true,
                false,
            ),
            // Payload-fallback conditions -> true iff payload storage is cold
            (
                "values_count_on_indexed_field",
                Filter::new_must(Condition::Field(FieldCondition::new_values_count(
                    JsonPath::new("kw"),
                    ValuesCount {
                        lt: None,
                        gt: None,
                        gte: Some(1),
                        lte: None,
                    },
                ))),
                true,
                false,
                true,
            ),
            (
                "unindexed_field",
                Filter::new_must(Condition::Field(FieldCondition::new_match(
                    JsonPath::new("unindexed"),
                    "plain_val".to_owned().into(),
                ))),
                true,
                false,
                true,
            ),
            (
                "unindexed_is_empty",
                Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
                    is_empty: PayloadField {
                        key: JsonPath::new("unindexed"),
                    },
                })),
                true,
                false,
                true,
            ),
            (
                "unindexed_is_null",
                Filter::new_must(Condition::IsNull(IsNullCondition {
                    is_null: PayloadField {
                        key: JsonPath::new("unindexed"),
                    },
                })),
                true,
                false,
                true,
            ),
            // Indexed IsEmpty / IsNull -> served by in-RAM NullIndex -> false everywhere
            (
                "indexed_is_empty",
                Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
                    is_empty: PayloadField {
                        key: JsonPath::new("kw"),
                    },
                })),
                false,
                false,
                false,
            ),
            (
                "indexed_is_null",
                Filter::new_must(Condition::IsNull(IsNullCondition {
                    is_null: PayloadField {
                        key: JsonPath::new("kw"),
                    },
                })),
                false,
                false,
                false,
            ),
            // Nested without per-element indexes -> payload fallback
            (
                "nested_without_per_element_index",
                Filter::new_must(Condition::new_nested(
                    JsonPath::new("unindexed_items"),
                    Filter::new_must(Condition::Field(FieldCondition::new_match(
                        JsonPath::new("k"),
                        1.into(),
                    ))),
                )),
                true,
                false,
                true,
            ),
            // Nested with per-element indexes covering all conditions -> follows element index coldness
            (
                "nested_with_per_element_indexes",
                Filter::new_must(Condition::new_nested(
                    JsonPath::new("offers"),
                    Filter {
                        must: Some(vec![
                            Condition::Field(FieldCondition::new_range(
                                JsonPath::new("price"),
                                Range {
                                    gte: Some(OrderedFloat(2.0)),
                                    ..Default::default()
                                },
                            )),
                            Condition::Field(FieldCondition::new_match(
                                JsonPath::new("shop"),
                                "shop_a".to_owned().into(),
                            )),
                        ]),
                        ..Default::default()
                    },
                )),
                true,
                true,
                false,
            ),
            (
                "nested_with_per_element_text_index",
                Filter::new_must(Condition::new_nested(
                    JsonPath::new("offers"),
                    Filter::new_must(Condition::Field(FieldCondition::new_match(
                        JsonPath::new("desc"),
                        Match::new_text("apples"),
                    ))),
                )),
                true,
                true,
                false,
            ),
            // Nested with cold flattened full-text index (payload fallback calls special_check_condition)
            (
                "nested_with_cold_flattened_text_index",
                Filter::new_must(Condition::new_nested(
                    JsonPath::new("flat_items"),
                    Filter::new_must(Condition::Field(FieldCondition::new_match(
                        JsonPath::new("desc"),
                        Match::new_text("sourdough"),
                    ))),
                )),
                true,
                true,
                true,
            ),
        ];

        // Prove ZERO HW I/O during `lazy_checks_hit_disk` across all cases on the fully cold segment.
        let acc = AmbientContext::new();
        acc.measure(|| {
            cold_both.payload_index.borrow().with_view(|view| {
                for (name, filter, expected, _, _) in &cases {
                    assert_eq!(
                        view.lazy_checks_hit_disk(filter),
                        *expected,
                        "cold_both mismatch for `{name}`"
                    );
                }
            });
        });
        assert_eq!(
            acc.hw_data()[HwMetric::PayloadIndexIoRead],
            0,
            "lazy_checks_hit_disk must perform zero payload-index IO"
        );
        assert_eq!(
            acc.hw_data()[HwMetric::PayloadIoRead],
            0,
            "lazy_checks_hit_disk must perform zero payload IO"
        );

        // Contrast: building `filter_context_for_serial_checks` on a cold match filter DOES read index IO.
        let serial_acc = AmbientContext::new();
        serial_acc.measure(|| {
            cold_both.payload_index.borrow().with_view(|view| {
                let ctx = view.filter_context_for_serial_checks(&cases[0].1).unwrap();
                assert!(!ctx.reads_cold_index());
            });
        });
        assert!(
            serial_acc.hw_data()[HwMetric::PayloadIndexIoRead] > 0,
            "filter_context_for_serial_checks should materialize postings from disk"
        );

        // Check hot-payload/cold-index and cold-payload/hot-index segments.
        hot_payload_cold_idx
            .payload_index
            .borrow()
            .with_view(|view| {
                for (name, filter, _, expected, _) in &cases {
                    assert_eq!(
                        view.lazy_checks_hit_disk(filter),
                        *expected,
                        "hot_payload_cold_idx mismatch for `{name}`"
                    );
                }
            });
        cold_payload_hot_idx
            .payload_index
            .borrow()
            .with_view(|view| {
                for (name, filter, _, _, expected) in &cases {
                    assert_eq!(
                        view.lazy_checks_hit_disk(filter),
                        *expected,
                        "cold_payload_hot_idx mismatch for `{name}`"
                    );
                }
            });

        // Congruence check: every real field index's metadata predicates must match its actual
        // `filter_into`, `condition_checker`, `element_condition_checker`, and `special_check_condition`.
        let probe_conditions = vec![
            FieldCondition::new_match(JsonPath::new("k"), 1.into()),
            FieldCondition::new_match(JsonPath::new("k"), "alpha_1".to_owned().into()),
            FieldCondition::new_match(JsonPath::new("k"), sample_uuid.to_owned().into()),
            FieldCondition::new_match(JsonPath::new("k"), true.into()),
            FieldCondition::new_match(
                JsonPath::new("k"),
                Match::new_any(AnyVariants::Integers([1, 2].into_iter().collect())),
            ),
            FieldCondition::new_match(
                JsonPath::new("k"),
                Match::new_any(AnyVariants::Strings(Default::default())),
            ),
            FieldCondition::new_match(
                JsonPath::new("k"),
                Match::new_any(AnyVariants::Integers(Default::default())),
            ),
            FieldCondition::new_match(
                JsonPath::new("k"),
                Match::new_except(AnyVariants::Strings(
                    ["alpha_1".into()].into_iter().collect(),
                )),
            ),
            FieldCondition::new_match(JsonPath::new("k"), Match::new_prefix("alpha_")),
            FieldCondition::new_match(JsonPath::new("k"), Match::new_substring("pha")),
            FieldCondition::new_match(JsonPath::new("k"), Match::new_text("brown")),
            FieldCondition::new_match(
                JsonPath::new("k"),
                Match::Phrase(MatchPhrase {
                    phrase: "brown fox".to_owned(),
                }),
            ),
            FieldCondition::new_match(
                JsonPath::new("k"),
                Match::TextAny(MatchTextAny {
                    text_any: "brown".to_owned(),
                }),
            ),
            FieldCondition::new_range(
                JsonPath::new("k"),
                Range {
                    gte: Some(OrderedFloat(1.0)),
                    ..Default::default()
                },
            ),
            FieldCondition::new_datetime_range(
                JsonPath::new("k"),
                Range {
                    lt: None,
                    gt: None,
                    gte: Some(dt),
                    lte: None,
                },
            ),
            FieldCondition::new_geo_radius(
                JsonPath::new("k"),
                GeoRadius {
                    center: GeoPoint::new_unchecked(13.4, 52.5),
                    radius: OrderedFloat(50_000.0),
                },
            ),
            FieldCondition::new_values_count(
                JsonPath::new("k"),
                ValuesCount {
                    lt: None,
                    gt: None,
                    gte: Some(1),
                    lte: None,
                },
            ),
            FieldCondition::new_is_empty(JsonPath::new("k"), true),
            FieldCondition::new_is_null(JsonPath::new("k"), true),
        ];

        let offers_array = JsonPath::new("offers[]");
        let sample_val = json!("quick brown fox");
        cold_both.payload_index.borrow().with_view(|view| {
            for (key, indexes) in view.field_indexes {
                for index in indexes {
                    for cond in &probe_conditions {
                        let mut sink = Vec::new();
                        let actual_filter = index.filter_into(cond, &mut sink).unwrap();
                        assert_eq!(
                            index.supports_posting_filter(cond),
                            actual_filter,
                            "supports_posting_filter mismatch on key `{key}` index `{index:?}` for `{cond:?}`"
                        );

                        let actual_checker = index.condition_checker(cond).unwrap().is_some();
                        assert_eq!(
                            index.supports_condition_checker(cond),
                            actual_checker,
                            "supports_condition_checker mismatch on key `{key}` index `{index:?}` for `{cond:?}`"
                        );

                        let actual_elem = index
                            .element_condition_checker(&offers_array, cond)
                            .unwrap()
                            .is_some();
                        assert_eq!(
                            index.supports_element_condition(&offers_array, cond),
                            actual_elem,
                            "supports_element_condition mismatch on key `{key}` index `{index:?}` for `{cond:?}`"
                        );

                        let actual_special = index
                            .special_check_condition(cond, &sample_val)
                            .unwrap()
                            .is_some();
                        assert_eq!(
                            index.supports_special_check(cond),
                            actual_special,
                            "supports_special_check mismatch on key `{key}` index `{index:?}` for `{cond:?}`"
                        );
                    }
                }
            }
        });

        // Reopen the cold segment as `ReadOnlySegment<MmapFile>` and verify `ReadOnlyFieldIndex`
        // agrees on every case with zero HW I/O and passes the exact same congruence matrix.
        let ro_segment = ReadOnlySegment::<MmapFile>::open(
            &MmapFs,
            &cold_both.segment_path,
            cold_both.segment_uuid(),
            None,
            None,
        )
        .unwrap();
        let ro_acc = AmbientContext::new();
        ro_acc.measure(|| {
            ro_segment.with_view(|seg_view| {
                for (name, filter, expected, _, _) in &cases {
                    assert_eq!(
                        seg_view.payload_index.lazy_checks_hit_disk(filter),
                        *expected,
                        "read_only cold_both mismatch for `{name}`"
                    );
                }
            });
        });
        assert_eq!(
            ro_acc.hw_data()[HwMetric::PayloadIndexIoRead],
            0,
            "read-only lazy_checks_hit_disk must perform zero payload-index IO"
        );
        assert_eq!(
            ro_acc.hw_data()[HwMetric::PayloadIoRead],
            0,
            "read-only lazy_checks_hit_disk must perform zero payload IO"
        );
        ro_segment.with_view(|seg_view| {
            for (key, indexes) in seg_view.payload_index.field_indexes {
                for index in indexes {
                    for cond in &probe_conditions {
                        let mut sink = Vec::new();
                        let actual_filter = index.filter_into(cond, &mut sink).unwrap();
                        assert_eq!(
                            index.supports_posting_filter(cond),
                            actual_filter,
                            "read-only supports_posting_filter mismatch on key `{key}` index `{index:?}` for `{cond:?}`"
                        );
                        let actual_checker = index.condition_checker(cond).unwrap().is_some();
                        assert_eq!(
                            index.supports_condition_checker(cond),
                            actual_checker,
                            "read-only supports_condition_checker mismatch on key `{key}` index `{index:?}` for `{cond:?}`"
                        );
                        let actual_elem = index
                            .element_condition_checker(&offers_array, cond)
                            .unwrap()
                            .is_some();
                        assert_eq!(
                            index.supports_element_condition(&offers_array, cond),
                            actual_elem,
                            "read-only supports_element_condition mismatch on key `{key}` index `{index:?}` for `{cond:?}`"
                        );
                        let actual_special = index
                            .special_check_condition(cond, &sample_val)
                            .unwrap()
                            .is_some();
                        assert_eq!(
                            index.supports_special_check(cond),
                            actual_special,
                            "read-only supports_special_check mismatch on key `{key}` index `{index:?}` for `{cond:?}`"
                        );
                    }
                }
            }
        });
    }

    #[test]
    fn plain_payload_index_lazy_checks_hit_disk() {
        let _scope = ambient::test_guard();
        let hot_dir = Builder::new().prefix("plain_hot").tempdir().unwrap();
        let cold_dir = Builder::new().prefix("plain_cold").tempdir().unwrap();
        let storage_dir = Builder::new().prefix("plain_cold_store").tempdir().unwrap();

        let id_tracker = Arc::new(AtomicRefCell::new(IdTrackerEnum::InMemoryIdTracker(
            InMemoryIdTracker::new(),
        )));

        let hot_storage = Arc::new(AtomicRefCell::new(PayloadStorageEnum::InMemory(
            InMemoryPayloadStorage::default(),
        )));
        let hot_checker = Arc::new(SimpleConditionChecker::new(
            hot_storage,
            id_tracker.clone(),
            HashMap::new(),
        ));
        let hot_index =
            PlainPayloadIndex::open(hot_checker, id_tracker.clone(), hot_dir.path()).unwrap();

        let cold_storage = Arc::new(AtomicRefCell::new(PayloadStorageEnum::Mmap(
            PayloadStorageImpl::open_or_create(storage_dir.path().to_path_buf(), false).unwrap(),
        )));
        let cold_checker = Arc::new(SimpleConditionChecker::new(
            cold_storage,
            id_tracker.clone(),
            HashMap::new(),
        ));
        let cold_index =
            PlainPayloadIndex::open(cold_checker, id_tracker, cold_dir.path()).unwrap();

        let field_filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new("k"),
            1.into(),
        )));
        let nested_filter = Filter::new_must(Condition::new_nested(
            JsonPath::new("items"),
            Filter::new_must(Condition::Field(FieldCondition::new_match(
                JsonPath::new("k"),
                1.into(),
            ))),
        ));
        let has_id_filter = Filter::new_must(Condition::HasId(HasIdCondition::from(
            AHashSet::from_iter([1u64.into()]),
        )));
        let wrapped_filter = Filter::new_must(Condition::Filter(Filter::new_must_not(
            Condition::IsNull(IsNullCondition {
                is_null: PayloadField {
                    key: JsonPath::new("k"),
                },
            }),
        )));

        assert!(!hot_index.lazy_checks_hit_disk(&field_filter));
        assert!(!hot_index.lazy_checks_hit_disk(&nested_filter));
        assert!(!hot_index.lazy_checks_hit_disk(&wrapped_filter));

        assert!(cold_index.lazy_checks_hit_disk(&field_filter));
        assert!(cold_index.lazy_checks_hit_disk(&nested_filter));
        assert!(cold_index.lazy_checks_hit_disk(&wrapped_filter));
        assert!(!cold_index.lazy_checks_hit_disk(&has_id_filter));
        assert!(!cold_index.lazy_checks_hit_disk(&Filter::default()));
    }
}
