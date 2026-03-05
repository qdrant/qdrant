//! Flat and nested re-exports of `segment` and `shard` APIs.

// =============================================================================
// Flat re-exports (`pub use ::…`).
//
// These re-exports are good candidates for `qdrant-edge` Rust public API.
//
// Most of these re-exports are 1-to-1 matches with their python counterparts
// from `qdrant_edge` python module. IOW, these names can be found in this
// command output:
//     python -c 'import qdrant_edge; print("\n".join(dir(qdrant_edge)))'

pub use segment::data_types::facets::{FacetHit, FacetResponse};
pub use segment::data_types::index::{
    BoolIndexParams, DatetimeIndexParams, FloatIndexParams, GeoIndexParams, IntegerIndexParams,
    KeywordIndexParams, Language, SnowballLanguage, SnowballParams, StopwordsSet, TextIndexParams,
    TokenizerType, UuidIndexParams,
};
pub use segment::data_types::modifier::Modifier;
pub use segment::data_types::order_by::{Direction, OrderBy};
pub use segment::index::query_optimization::rescore_formula::parsed_formula::DecayKind;
pub use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
pub use segment::types::{
    AcornSearchParams, BinaryQuantizationConfig, BinaryQuantizationEncoding,
    BinaryQuantizationQueryEncoding, CompressionRatio, Distance, FieldCondition, Filter,
    GeoBoundingBox, GeoPoint, GeoPolygon, GeoRadius, HasIdCondition, HasVectorCondition,
    IsEmptyCondition, IsNullCondition, MatchAny, MatchExcept, MatchPhrase, MatchText, MatchTextAny,
    MatchValue, MinShould, MultiVectorComparator, MultiVectorConfig, NestedCondition,
    PayloadSchemaType, PayloadSelector, PayloadStorageType, ProductQuantizationConfig,
    QuantizationSearchParams, ScalarQuantizationConfig, ScalarType, ScoredPoint, SearchParams,
    SparseVectorDataConfig, SparseVectorStorageType, ValuesCount, VectorDataConfig,
    VectorStorageDatatype, VectorStorageType,
};
pub use segment::vector_storage::query::{ContextPair, ContextQuery, FeedbackItem, Query};
pub use shard::count::CountRequestInternal as CountRequest;
pub use shard::facet::FacetRequestInternal as FacetRequest;
pub use shard::operations::point_ops::UpdateMode;
pub use shard::query::formula::{ExpressionInternal as Expression, FormulaInternal as Formula};
pub use shard::query::{FusionInternal as Fusion, MmrInternal as Mmr, SampleInternal as Sample};
pub use shard::retrieve::record_internal::RecordInternal as Record;
pub use shard::scroll::ScrollRequestInternal as ScrollRequest;
pub use sparse::common::sparse_vector::SparseVector;

// End of flat re-exports.

// =============================================================================
// Nested re-exports (`pub mod … { pub use ::…; }`).
//
// These are WIP re-exports that match the structure of `segment` and `shard`
// modules. These are temporary, we will remove them later.
//
// TODO: We need to review each of these and decide:
// - Which of these are good enough for public qdrant_edge API.
//   These should be promoted to flat re-exports or replaced with some wrapper
//   types.
// - Which of these are helpers/internal APIs and should not be exposed at all.

/// Re-exports of internal APIs of Qdrant.
/// We will remove these re-exports later in favor of more curated public API.
pub mod internal {
    pub mod shard {
        pub mod files {
            pub use shard::files::{clear_data, move_data};
        }
        pub mod operations {
            pub use shard::operations::{
                CollectionUpdateOperations, CreateIndex, FieldIndexOperations,
            };
            pub mod payload_ops {
                pub use shard::operations::payload_ops::*;
            }
            pub mod point_ops {
                pub use shard::operations::point_ops::{
                    ConditionalInsertOperationInternal, PointIdsList,
                    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
                    VectorPersisted, VectorStructPersisted,
                };
            }
            pub mod vector_ops {
                pub use shard::operations::vector_ops::{
                    PointVectorsPersisted, UpdateVectorsOp, VectorOperations,
                };
            }
        }
        pub mod query {
            pub use shard::query::{ScoringQuery, ShardPrefetch, ShardQueryRequest};
            pub mod query_enum {
                pub use shard::query::query_enum::QueryEnum;
            }
        }
        pub mod search {
            pub use shard::search::CoreSearchRequest;
        }

        pub mod snapshots {
            pub mod snapshot_manifest {
                pub use shard::snapshots::snapshot_manifest::SnapshotManifest;
            }
        }
    }

    pub mod segment {
        pub mod common {
            pub mod operation_error {
                pub use segment::common::operation_error::{OperationError, OperationResult};
            }
        }

        pub mod data_types {
            pub mod facets {
                pub use segment::data_types::facets::{FacetValue, FacetValueHit};
            }
            pub mod index {
                pub use segment::data_types::index::{StemmingAlgorithm, StopwordsInterface};
            }
            pub mod order_by {
                pub use segment::data_types::order_by::{OrderByInterface, OrderValue, StartFrom};
            }
            pub mod vectors {
                pub use segment::data_types::vectors::{
                    DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, NamedQuery,
                    TypedMultiDenseVector, VectorInternal, VectorStructInternal,
                };
            }
        }


        pub mod index {
            pub mod query_optimization {
                pub mod rescore_formula {
                    pub mod parsed_formula {
                        pub use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
                    }
                }
            }
        }

        pub mod json_path {
            pub use segment::json_path::JsonPath;
        }


        pub mod types {
            pub use segment::types::{
                AnyVariants, BinaryQuantization, Condition, DateTimePayloadType, DateTimeWrapper,
                Distance, ExtendedPointId, FieldCondition, Filter, FloatPayloadType, GeoLineString,
                GeoPolygonShadow, HnswConfig, Indexes, IntPayloadType, Match, Nested, Payload,
                PayloadField, PayloadFieldSchema, PayloadIndexInfo, PayloadSchemaParams,
                PayloadSelectorExclude, PayloadSelectorInclude, PayloadStorageType, PointIdType,
                ProductQuantization, QuantizationConfig, Range, RangeInterface, ScalarQuantization,
                SegmentConfig, ValueVariants, VectorDataConfig, VectorNameBuf, VectorStorageType,
                WithPayloadInterface, WithVector,
            };
        }

        pub mod vector_storage {
            pub mod query {
                pub use segment::vector_storage::query::{
                    DiscoverQuery, NaiveFeedbackCoefficients, NaiveFeedbackQuery, RecoQuery,
                };
            }
        }
    }
}

// End of nested re-exports.
