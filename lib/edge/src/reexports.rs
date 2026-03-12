mod reexports_from_qdrant_crates {
    pub use segment::common::operation_error::{OperationError, OperationResult};
    pub use segment::data_types::facets::{FacetHit, FacetResponse, FacetValue, FacetValueHit};
    pub use segment::data_types::index::{
        BoolIndexParams, DatetimeIndexParams, FloatIndexParams, GeoIndexParams, IntegerIndexParams,
        KeywordIndexParams, Language, SnowballLanguage, SnowballParams, StopwordsSet,
        TextIndexParams, TokenizerType, UuidIndexParams,
    };
    pub use segment::data_types::modifier::Modifier;
    pub use segment::data_types::order_by::{
        Direction, OrderBy, OrderByInterface, OrderValue, StartFrom,
    };
    pub use segment::data_types::vectors::{
        DEFAULT_VECTOR_NAME, NamedQuery, TypedMultiDenseVector,
    };
    pub use segment::index::query_optimization::rescore_formula::parsed_formula::DecayKind;
    pub use segment::json_path::JsonPath;
    pub use segment::types::{
        AcornSearchParams, AnyVariants, BinaryQuantizationConfig, BinaryQuantizationEncoding,
        BinaryQuantizationQueryEncoding, CompressionRatio, Condition, DateTimeWrapper, Distance,
        ExtendedPointId as PointId, FieldCondition, Filter, GeoBoundingBox, GeoPoint, GeoPolygon,
        GeoRadius, HasIdCondition, HasVectorCondition, HnswConfig as HnswIndexConfig,
        IsEmptyCondition, IsNullCondition, Match, MatchAny, MatchExcept, MatchPhrase, MatchText,
        MatchTextAny, MatchValue, MinShould, MultiVectorComparator, MultiVectorConfig, Nested,
        NestedCondition, Payload, PayloadFieldSchema, PayloadIndexInfo, PayloadSchemaParams,
        PayloadSchemaType, PayloadSelector, PayloadSelectorExclude, PayloadSelectorInclude,
        ProductQuantizationConfig, QuantizationConfig, QuantizationSearchParams, Range,
        RangeInterface, ScalarQuantizationConfig, ScalarType, ScoredPoint, SearchParams,
        ValueVariants, ValuesCount, VectorStorageDatatype, WithPayloadInterface, WithVector,
    };
    pub use segment::vector_storage::query::{
        ContextPair, ContextQuery, DiscoverQuery, FeedbackItem,
        NaiveFeedbackCoefficients as NaiveFeedbackStrategy,
        NaiveFeedbackQuery as FeedbackNaiveQuery, RecoQuery as RecommendQuery,
    };
    pub use shard::count::CountRequestInternal as CountRequest;
    pub use shard::facet::FacetRequestInternal as FacetRequest;
    pub use shard::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
    pub use shard::operations::point_ops::{
        ConditionalInsertOperationInternal as ConditionalInsertOperation, PointIdsList,
        PointInsertOperationsInternal as PointInsertOperations, PointOperations,
        PointSyncOperation, UpdateMode,
    };
    pub use shard::operations::vector_ops::{UpdateVectorsOp, VectorOperations};
    pub use shard::operations::{
        CollectionUpdateOperations as UpdateOperation, CreateIndex, FieldIndexOperations,
    };
    pub use shard::query::formula::{ExpressionInternal as Expression, FormulaInternal as Formula};
    pub use shard::query::query_enum::QueryEnum;
    pub use shard::query::{
        FusionInternal as Fusion, MmrInternal as Mmr, SampleInternal as Sample, ScoringQuery,
        ShardPrefetch as Prefetch, ShardQueryRequest as QueryRequest,
    };
    pub use shard::retrieve::record_internal::RecordInternal as Record;
    pub use shard::scroll::ScrollRequestInternal as ScrollRequest;
    pub use shard::search::CoreSearchRequest as SearchRequest;
    pub use sparse::common::sparse_vector::SparseVector;
}
pub use reexports_from_qdrant_crates::*;

/// TODO: We need to do something these duplicates of Vector/Point structures
mod todo_to_clean_up {
    pub use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
    pub use shard::operations::point_ops::{
        PointStructPersisted, VectorPersisted, VectorStructPersisted,
    };
    pub use shard::operations::vector_ops::PointVectorsPersisted;
}
pub use todo_to_clean_up::*;

pub mod internal {
    pub use shard::files::{clear_data, move_data};
    pub use shard::snapshots::snapshot_manifest::SnapshotManifest;
}

/// Re-export from external crates used by Qdrant.
pub mod external {
    pub use ordered_float;
    pub use serde_json;
    pub use uuid;
}
