use std::collections::{HashMap, HashSet};
use std::str::FromStr as _;
use std::time::Instant;

use chrono::{NaiveDateTime, Timelike};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use itertools::Itertools;
use segment::common::operation_error::OperationError;
use segment::data_types::index::{
    BoolIndexType, DatetimeIndexType, FloatIndexType, GeoIndexType, IntegerIndexType,
    KeywordIndexType, TextIndexType, UuidIndexType,
};
use segment::data_types::{facets as segment_facets, vectors as segment_vectors};
use segment::types::{default_quantization_ignore_value, DateTimePayloadType, FloatPayloadType};
use segment::vector_storage::query as segment_query;
use sparse::common::sparse_vector::validate_sparse_vector_impl;
use tonic::Status;
use uuid::Uuid;

use super::qdrant::raw_query::RawContextPair;
use super::qdrant::{
    raw_query, start_from, BinaryQuantization, BoolIndexParams, CompressionRatio,
    DatetimeIndexParams, DatetimeRange, Direction, FacetHit, FacetHitInternal, FacetValue,
    FacetValueInternal, FieldType, FloatIndexParams, GeoIndexParams, GeoLineString, GroupId,
    HardwareUsage, HasVectorCondition, KeywordIndexParams, LookupLocation, MultiVectorComparator,
    MultiVectorConfig, OrderBy, OrderValue, Range, RawVector, RecommendStrategy, RetrievedPoint,
    SearchMatrixPair, SearchPointGroups, SearchPoints, ShardKeySelector, SparseIndices, StartFrom,
    UuidIndexParams, VectorsOutput, WithLookup,
};
use crate::conversions::json;
use crate::grpc::qdrant::condition::ConditionOneOf;
use crate::grpc::qdrant::payload_index_params::IndexParams;
use crate::grpc::qdrant::point_id::PointIdOptions;
use crate::grpc::qdrant::r#match::MatchValue;
use crate::grpc::qdrant::with_payload_selector::SelectorOptions;
use crate::grpc::qdrant::{
    shard_key, with_vectors_selector, CollectionDescription, CollectionOperationResponse,
    Condition, Distance, FieldCondition, Filter, GeoBoundingBox, GeoPoint, GeoPolygon, GeoRadius,
    HasIdCondition, HealthCheckReply, HnswConfigDiff, IntegerIndexParams, IsEmptyCondition,
    IsNullCondition, ListCollectionsResponse, Match, MinShould, NamedVectors, NestedCondition,
    PayloadExcludeSelector, PayloadIncludeSelector, PayloadIndexParams, PayloadSchemaInfo,
    PayloadSchemaType, PointId, PointStruct, PointsOperationResponse,
    PointsOperationResponseInternal, ProductQuantization, QuantizationConfig,
    QuantizationSearchParams, QuantizationType, RepeatedIntegers, RepeatedStrings,
    ScalarQuantization, ScoredPoint, SearchParams, ShardKey, StrictModeConfig, TextIndexParams,
    TokenizerType, UpdateResult, UpdateResultInternal, ValuesCount, VectorsSelector,
    WithPayloadSelector, WithVectorsSelector,
};
use crate::rest::models::{CollectionsResponse, VersionInfo};
use crate::rest::schema as rest;

pub fn convert_shard_key_to_grpc(value: segment::types::ShardKey) -> ShardKey {
    match value {
        segment::types::ShardKey::Keyword(keyword) => ShardKey {
            key: Some(shard_key::Key::Keyword(keyword)),
        },
        segment::types::ShardKey::Number(number) => ShardKey {
            key: Some(shard_key::Key::Number(number)),
        },
    }
}

pub fn convert_shard_key_from_grpc(value: ShardKey) -> Option<segment::types::ShardKey> {
    match value.key {
        None => None,
        Some(key) => match key {
            shard_key::Key::Keyword(keyword) => Some(segment::types::ShardKey::Keyword(keyword)),
            shard_key::Key::Number(number) => Some(segment::types::ShardKey::Number(number)),
        },
    }
}

pub fn convert_shard_key_from_grpc_opt(
    value: Option<ShardKey>,
) -> Option<segment::types::ShardKey> {
    match value {
        None => None,
        Some(key) => match key.key {
            None => None,
            Some(key) => match key {
                shard_key::Key::Keyword(keyword) => {
                    Some(segment::types::ShardKey::Keyword(keyword))
                }
                shard_key::Key::Number(number) => Some(segment::types::ShardKey::Number(number)),
            },
        },
    }
}
impl From<ShardKeySelector> for rest::ShardKeySelector {
    fn from(value: ShardKeySelector) -> Self {
        let shard_keys: Vec<_> = value
            .shard_keys
            .into_iter()
            .filter_map(convert_shard_key_from_grpc)
            .collect();

        if shard_keys.len() == 1 {
            rest::ShardKeySelector::ShardKey(shard_keys.into_iter().next().unwrap())
        } else {
            rest::ShardKeySelector::ShardKeys(shard_keys)
        }
    }
}

impl From<VersionInfo> for HealthCheckReply {
    fn from(info: VersionInfo) -> Self {
        HealthCheckReply {
            title: info.title,
            version: info.version,
            commit: info.commit,
        }
    }
}

impl From<(Instant, CollectionsResponse)> for ListCollectionsResponse {
    fn from(value: (Instant, CollectionsResponse)) -> Self {
        let (timing, response) = value;
        let collections = response
            .collections
            .into_iter()
            .map(|desc| CollectionDescription { name: desc.name })
            .collect::<Vec<_>>();
        Self {
            collections,
            time: timing.elapsed().as_secs_f64(),
        }
    }
}

impl From<segment::data_types::index::TokenizerType> for TokenizerType {
    fn from(tokenizer_type: segment::data_types::index::TokenizerType) -> Self {
        match tokenizer_type {
            segment::data_types::index::TokenizerType::Prefix => TokenizerType::Prefix,
            segment::data_types::index::TokenizerType::Whitespace => TokenizerType::Whitespace,
            segment::data_types::index::TokenizerType::Multilingual => TokenizerType::Multilingual,
            segment::data_types::index::TokenizerType::Word => TokenizerType::Word,
        }
    }
}

impl From<segment::data_types::index::KeywordIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::KeywordIndexParams) -> Self {
        PayloadIndexParams {
            index_params: Some(IndexParams::KeywordIndexParams(KeywordIndexParams {
                is_tenant: params.is_tenant,
                on_disk: params.on_disk,
            })),
        }
    }
}

impl From<segment::data_types::index::IntegerIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::IntegerIndexParams) -> Self {
        PayloadIndexParams {
            index_params: Some(IndexParams::IntegerIndexParams(IntegerIndexParams {
                lookup: params.lookup,
                range: params.range,
                on_disk: params.on_disk,
                is_principal: params.is_principal,
            })),
        }
    }
}

impl From<segment::data_types::index::FloatIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::FloatIndexParams) -> Self {
        PayloadIndexParams {
            index_params: Some(IndexParams::FloatIndexParams(FloatIndexParams {
                on_disk: params.on_disk,
                is_principal: params.is_principal,
            })),
        }
    }
}

impl From<segment::data_types::index::GeoIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::GeoIndexParams) -> Self {
        PayloadIndexParams {
            index_params: Some(IndexParams::GeoIndexParams(GeoIndexParams {
                on_disk: params.on_disk,
            })),
        }
    }
}

impl From<segment::data_types::index::TextIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::TextIndexParams) -> Self {
        let tokenizer = TokenizerType::from(params.tokenizer);
        PayloadIndexParams {
            index_params: Some(IndexParams::TextIndexParams(TextIndexParams {
                tokenizer: tokenizer as i32,
                lowercase: params.lowercase,
                min_token_len: params.min_token_len.map(|x| x as u64),
                max_token_len: params.max_token_len.map(|x| x as u64),
                on_disk: params.on_disk,
            })),
        }
    }
}

impl From<segment::data_types::index::BoolIndexParams> for PayloadIndexParams {
    fn from(_params: segment::data_types::index::BoolIndexParams) -> Self {
        PayloadIndexParams {
            index_params: Some(IndexParams::BoolIndexParams(BoolIndexParams {})),
        }
    }
}

impl From<segment::data_types::index::UuidIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::UuidIndexParams) -> Self {
        PayloadIndexParams {
            index_params: Some(IndexParams::UuidIndexParams(UuidIndexParams {
                is_tenant: params.is_tenant,
                on_disk: params.on_disk,
            })),
        }
    }
}

impl From<segment::data_types::index::DatetimeIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::DatetimeIndexParams) -> Self {
        PayloadIndexParams {
            index_params: Some(IndexParams::DatetimeIndexParams(DatetimeIndexParams {
                on_disk: params.on_disk,
                is_principal: params.is_principal,
            })),
        }
    }
}

impl From<segment::types::PayloadIndexInfo> for PayloadSchemaInfo {
    fn from(schema: segment::types::PayloadIndexInfo) -> Self {
        PayloadSchemaInfo {
            data_type: PayloadSchemaType::from(schema.data_type) as i32,
            params: schema.params.map(|p| p.into()),
            points: Some(schema.points as u64),
        }
    }
}

impl From<segment::types::PayloadSchemaType> for PayloadSchemaType {
    fn from(schema_type: segment::types::PayloadSchemaType) -> Self {
        match schema_type {
            segment::types::PayloadSchemaType::Keyword => PayloadSchemaType::Keyword,
            segment::types::PayloadSchemaType::Integer => PayloadSchemaType::Integer,
            segment::types::PayloadSchemaType::Float => PayloadSchemaType::Float,
            segment::types::PayloadSchemaType::Geo => PayloadSchemaType::Geo,
            segment::types::PayloadSchemaType::Text => PayloadSchemaType::Text,
            segment::types::PayloadSchemaType::Bool => PayloadSchemaType::Bool,
            segment::types::PayloadSchemaType::Datetime => PayloadSchemaType::Datetime,
            segment::types::PayloadSchemaType::Uuid => PayloadSchemaType::Uuid,
        }
    }
}

impl From<segment::types::PayloadSchemaType> for FieldType {
    fn from(schema_type: segment::types::PayloadSchemaType) -> Self {
        match schema_type {
            segment::types::PayloadSchemaType::Keyword => FieldType::Keyword,
            segment::types::PayloadSchemaType::Integer => FieldType::Integer,
            segment::types::PayloadSchemaType::Float => FieldType::Float,
            segment::types::PayloadSchemaType::Geo => FieldType::Geo,
            segment::types::PayloadSchemaType::Text => FieldType::Text,
            segment::types::PayloadSchemaType::Bool => FieldType::Bool,
            segment::types::PayloadSchemaType::Datetime => FieldType::Datetime,
            segment::types::PayloadSchemaType::Uuid => FieldType::Uuid,
        }
    }
}

impl TryFrom<TokenizerType> for segment::data_types::index::TokenizerType {
    type Error = Status;
    fn try_from(tokenizer_type: TokenizerType) -> Result<Self, Self::Error> {
        match tokenizer_type {
            TokenizerType::Unknown => Err(Status::invalid_argument("unknown tokenizer type")),
            TokenizerType::Prefix => Ok(segment::data_types::index::TokenizerType::Prefix),
            TokenizerType::Multilingual => {
                Ok(segment::data_types::index::TokenizerType::Multilingual)
            }
            TokenizerType::Whitespace => Ok(segment::data_types::index::TokenizerType::Whitespace),
            TokenizerType::Word => Ok(segment::data_types::index::TokenizerType::Word),
        }
    }
}

impl From<segment::types::PayloadSchemaParams> for PayloadIndexParams {
    fn from(params: segment::types::PayloadSchemaParams) -> Self {
        match params {
            segment::types::PayloadSchemaParams::Keyword(p) => p.into(),
            segment::types::PayloadSchemaParams::Integer(p) => p.into(),
            segment::types::PayloadSchemaParams::Float(p) => p.into(),
            segment::types::PayloadSchemaParams::Geo(p) => p.into(),
            segment::types::PayloadSchemaParams::Text(p) => p.into(),
            segment::types::PayloadSchemaParams::Bool(p) => p.into(),
            segment::types::PayloadSchemaParams::Datetime(p) => p.into(),
            segment::types::PayloadSchemaParams::Uuid(p) => p.into(),
        }
    }
}

impl TryFrom<KeywordIndexParams> for segment::data_types::index::KeywordIndexParams {
    type Error = Status;
    fn try_from(params: KeywordIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::index::KeywordIndexParams {
            r#type: KeywordIndexType::Keyword,
            is_tenant: params.is_tenant,
            on_disk: params.on_disk,
        })
    }
}

impl TryFrom<IntegerIndexParams> for segment::data_types::index::IntegerIndexParams {
    type Error = Status;
    fn try_from(params: IntegerIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::index::IntegerIndexParams {
            r#type: IntegerIndexType::Integer,
            lookup: params.lookup,
            range: params.range,
            is_principal: params.is_principal,
            on_disk: params.on_disk,
        })
    }
}

impl TryFrom<FloatIndexParams> for segment::data_types::index::FloatIndexParams {
    type Error = Status;
    fn try_from(params: FloatIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::index::FloatIndexParams {
            r#type: FloatIndexType::Float,
            on_disk: params.on_disk,
            is_principal: params.is_principal,
        })
    }
}

impl TryFrom<GeoIndexParams> for segment::data_types::index::GeoIndexParams {
    type Error = Status;
    fn try_from(params: GeoIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::index::GeoIndexParams {
            r#type: GeoIndexType::Geo,
            on_disk: params.on_disk,
        })
    }
}

impl TryFrom<TextIndexParams> for segment::data_types::index::TextIndexParams {
    type Error = Status;
    fn try_from(params: TextIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::index::TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::try_from(params.tokenizer)
                .map(|x| x.try_into())
                .unwrap_or_else(|_| Err(Status::invalid_argument("unknown tokenizer type")))?,
            lowercase: params.lowercase,
            min_token_len: params.min_token_len.map(|x| x as usize),
            max_token_len: params.max_token_len.map(|x| x as usize),
            on_disk: params.on_disk,
        })
    }
}

impl TryFrom<BoolIndexParams> for segment::data_types::index::BoolIndexParams {
    type Error = Status;
    fn try_from(_params: BoolIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::index::BoolIndexParams {
            r#type: BoolIndexType::Bool,
        })
    }
}

impl TryFrom<DatetimeIndexParams> for segment::data_types::index::DatetimeIndexParams {
    type Error = Status;
    fn try_from(params: DatetimeIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::index::DatetimeIndexParams {
            r#type: DatetimeIndexType::Datetime,
            on_disk: params.on_disk,
            is_principal: params.is_principal,
        })
    }
}

impl TryFrom<UuidIndexParams> for segment::data_types::index::UuidIndexParams {
    type Error = Status;
    fn try_from(params: UuidIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::index::UuidIndexParams {
            r#type: UuidIndexType::Uuid,
            is_tenant: params.is_tenant,
            on_disk: params.on_disk,
        })
    }
}

impl TryFrom<IndexParams> for segment::types::PayloadSchemaParams {
    type Error = Status;

    fn try_from(value: IndexParams) -> Result<Self, Self::Error> {
        Ok(match value {
            IndexParams::KeywordIndexParams(p) => {
                segment::types::PayloadSchemaParams::Keyword(p.try_into()?)
            }
            IndexParams::IntegerIndexParams(p) => {
                segment::types::PayloadSchemaParams::Integer(p.try_into()?)
            }
            IndexParams::FloatIndexParams(p) => {
                segment::types::PayloadSchemaParams::Float(p.try_into()?)
            }
            IndexParams::GeoIndexParams(p) => {
                segment::types::PayloadSchemaParams::Geo(p.try_into()?)
            }
            IndexParams::TextIndexParams(p) => {
                segment::types::PayloadSchemaParams::Text(p.try_into()?)
            }
            IndexParams::BoolIndexParams(p) => {
                segment::types::PayloadSchemaParams::Bool(p.try_into()?)
            }
            IndexParams::DatetimeIndexParams(p) => {
                segment::types::PayloadSchemaParams::Datetime(p.try_into()?)
            }
            IndexParams::UuidIndexParams(p) => {
                segment::types::PayloadSchemaParams::Uuid(p.try_into()?)
            }
        })
    }
}

impl TryFrom<PayloadSchemaInfo> for segment::types::PayloadIndexInfo {
    type Error = Status;

    fn try_from(schema: PayloadSchemaInfo) -> Result<Self, Self::Error> {
        let data_type = match PayloadSchemaType::try_from(schema.data_type) {
            Err(_) => {
                return Err(Status::invalid_argument(
                    "Malformed payload schema".to_string(),
                ));
            }
            Ok(data_type) => match data_type {
                PayloadSchemaType::Keyword => segment::types::PayloadSchemaType::Keyword,
                PayloadSchemaType::Integer => segment::types::PayloadSchemaType::Integer,
                PayloadSchemaType::Float => segment::types::PayloadSchemaType::Float,
                PayloadSchemaType::Geo => segment::types::PayloadSchemaType::Geo,
                PayloadSchemaType::Text => segment::types::PayloadSchemaType::Text,
                PayloadSchemaType::Bool => segment::types::PayloadSchemaType::Bool,
                PayloadSchemaType::Datetime => segment::types::PayloadSchemaType::Datetime,
                PayloadSchemaType::UnknownType => {
                    return Err(Status::invalid_argument(
                        "Malformed payload schema".to_string(),
                    ));
                }
                PayloadSchemaType::Uuid => segment::types::PayloadSchemaType::Uuid,
            },
        };
        let params = match schema.params {
            None => None,
            Some(PayloadIndexParams { index_params: None }) => None,
            Some(PayloadIndexParams {
                index_params: Some(index_params),
            }) => Some(index_params.try_into()?),
        };

        Ok(segment::types::PayloadIndexInfo {
            data_type,
            params,
            points: schema.points.unwrap_or(0) as usize,
        })
    }
}

impl From<(Instant, bool)> for CollectionOperationResponse {
    fn from(value: (Instant, bool)) -> Self {
        let (timing, result) = value;
        CollectionOperationResponse {
            result,
            time: timing.elapsed().as_secs_f64(),
        }
    }
}

impl From<segment::types::GeoPoint> for GeoPoint {
    fn from(geo: segment::types::GeoPoint) -> Self {
        Self {
            lon: geo.lon,
            lat: geo.lat,
        }
    }
}

impl TryFrom<WithPayloadSelector> for segment::types::WithPayloadInterface {
    type Error = Status;

    fn try_from(value: WithPayloadSelector) -> Result<Self, Self::Error> {
        match value.selector_options {
            Some(options) => Ok(match options {
                SelectorOptions::Enable(flag) => segment::types::WithPayloadInterface::Bool(flag),
                SelectorOptions::Exclude(s) => segment::types::PayloadSelectorExclude::new(
                    s.fields
                        .iter()
                        .map(|i| json::json_path_from_proto(i))
                        .collect::<Result<_, _>>()?,
                )
                .into(),
                SelectorOptions::Include(s) => segment::types::PayloadSelectorInclude::new(
                    s.fields
                        .iter()
                        .map(|i| json::json_path_from_proto(i))
                        .collect::<Result<_, _>>()?,
                )
                .into(),
            }),
            _ => Err(Status::invalid_argument("No PayloadSelector".to_string())),
        }
    }
}

impl From<segment::types::WithPayloadInterface> for WithPayloadSelector {
    fn from(value: segment::types::WithPayloadInterface) -> Self {
        let selector_options = match value {
            segment::types::WithPayloadInterface::Bool(flag) => SelectorOptions::Enable(flag),
            segment::types::WithPayloadInterface::Fields(fields) => {
                SelectorOptions::Include(PayloadIncludeSelector {
                    fields: fields.iter().map(|f| f.to_string()).collect(),
                })
            }
            segment::types::WithPayloadInterface::Selector(selector) => match selector {
                segment::types::PayloadSelector::Include(s) => {
                    SelectorOptions::Include(PayloadIncludeSelector {
                        fields: s.include.iter().map(|f| f.to_string()).collect(),
                    })
                }
                segment::types::PayloadSelector::Exclude(s) => {
                    SelectorOptions::Exclude(PayloadExcludeSelector {
                        fields: s.exclude.iter().map(|f| f.to_string()).collect(),
                    })
                }
            },
        };
        WithPayloadSelector {
            selector_options: Some(selector_options),
        }
    }
}

impl From<QuantizationSearchParams> for segment::types::QuantizationSearchParams {
    fn from(params: QuantizationSearchParams) -> Self {
        Self {
            ignore: params.ignore.unwrap_or(default_quantization_ignore_value()),
            rescore: params.rescore,
            oversampling: params.oversampling,
        }
    }
}

impl From<segment::types::QuantizationSearchParams> for QuantizationSearchParams {
    fn from(params: segment::types::QuantizationSearchParams) -> Self {
        Self {
            ignore: Some(params.ignore),
            rescore: params.rescore,
            oversampling: params.oversampling,
        }
    }
}

impl From<SearchParams> for segment::types::SearchParams {
    fn from(params: SearchParams) -> Self {
        Self {
            hnsw_ef: params.hnsw_ef.map(|x| x as usize),
            exact: params.exact.unwrap_or(false),
            quantization: params.quantization.map(|q| q.into()),
            indexed_only: params.indexed_only.unwrap_or(false),
        }
    }
}

impl From<segment::types::SearchParams> for SearchParams {
    fn from(params: segment::types::SearchParams) -> Self {
        Self {
            hnsw_ef: params.hnsw_ef.map(|x| x as u64),
            exact: Some(params.exact),
            quantization: params.quantization.map(|q| q.into()),
            indexed_only: Some(params.indexed_only),
        }
    }
}

impl From<segment::types::PointIdType> for PointId {
    fn from(point_id: segment::types::PointIdType) -> Self {
        PointId {
            point_id_options: Some(match point_id {
                segment::types::PointIdType::NumId(num) => PointIdOptions::Num(num),
                segment::types::PointIdType::Uuid(uuid) => PointIdOptions::Uuid(uuid.to_string()),
            }),
        }
    }
}

impl TryFrom<PointStruct> for rest::PointStruct {
    type Error = Status;

    fn try_from(value: PointStruct) -> Result<Self, Self::Error> {
        let PointStruct {
            id,
            vectors,
            payload,
        } = value;

        // empty payload means None in PointStruct
        let converted_payload = if payload.is_empty() {
            None
        } else {
            Some(json::proto_to_payloads(payload)?)
        };

        let vector_struct = match vectors {
            None => return Err(Status::invalid_argument("Expected some vectors")),
            Some(vectors) => rest::VectorStruct::try_from(vectors)?,
        };

        Ok(Self {
            id: id
                .ok_or_else(|| Status::invalid_argument("Empty ID is not allowed"))?
                .try_into()?,
            vector: vector_struct,
            payload: converted_payload,
        })
    }
}

impl TryFrom<rest::Record> for RetrievedPoint {
    type Error = OperationError;
    fn try_from(record: rest::Record) -> Result<Self, Self::Error> {
        let retrieved_point = Self {
            id: Some(PointId::from(record.id)),
            payload: record
                .payload
                .map(json::payload_to_proto)
                .unwrap_or_default(),
            vectors: record.vector.map(VectorsOutput::try_from).transpose()?,
            shard_key: record.shard_key.map(convert_shard_key_to_grpc),
            order_value: record.order_value.map(From::from),
        };
        Ok(retrieved_point)
    }
}

impl From<segment::data_types::order_by::OrderValue> for OrderValue {
    fn from(value: segment::data_types::order_by::OrderValue) -> Self {
        use segment::data_types::order_by as segment;

        use crate::grpc::qdrant::order_value::Variant;

        let variant = match value {
            segment::OrderValue::Float(value) => Variant::Float(value),
            segment::OrderValue::Int(value) => Variant::Int(value),
        };

        Self {
            variant: Some(variant),
        }
    }
}

impl TryFrom<OrderValue> for segment::data_types::order_by::OrderValue {
    type Error = Status;

    fn try_from(value: OrderValue) -> Result<Self, Self::Error> {
        use segment::data_types::order_by as segment;

        use crate::grpc::qdrant::order_value::Variant;

        let variant = value.variant.ok_or_else(|| {
            Status::invalid_argument("OrderedValue should have a variant".to_string())
        })?;

        let value = match variant {
            Variant::Float(value) => segment::OrderValue::Float(value),
            Variant::Int(value) => segment::OrderValue::Int(value),
        };

        Ok(value)
    }
}

impl From<segment::types::ScoredPoint> for ScoredPoint {
    fn from(point: segment::types::ScoredPoint) -> Self {
        Self {
            id: Some(PointId::from(point.id)),
            payload: point
                .payload
                .map(json::payload_to_proto)
                .unwrap_or_default(),
            score: point.score,
            version: point.version,
            vectors: point.vector.map(VectorsOutput::from),
            shard_key: point.shard_key.map(convert_shard_key_to_grpc),
            order_value: point.order_value.map(OrderValue::from),
        }
    }
}

impl TryFrom<crate::rest::ScoredPoint> for ScoredPoint {
    type Error = OperationError;
    fn try_from(point: crate::rest::ScoredPoint) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Some(PointId::from(point.id)),
            payload: point
                .payload
                .map(json::payload_to_proto)
                .unwrap_or_default(),
            score: point.score,
            version: point.version,
            vectors: point.vector.map(VectorsOutput::try_from).transpose()?,
            shard_key: point.shard_key.map(convert_shard_key_to_grpc),
            order_value: point.order_value.map(OrderValue::from),
        })
    }
}

impl From<segment::data_types::groups::GroupId> for GroupId {
    fn from(key: segment::data_types::groups::GroupId) -> Self {
        match key {
            segment::data_types::groups::GroupId::String(str) => Self {
                kind: Some(crate::grpc::qdrant::group_id::Kind::StringValue(str)),
            },
            segment::data_types::groups::GroupId::NumberU64(n) => Self {
                kind: Some(crate::grpc::qdrant::group_id::Kind::UnsignedValue(n)),
            },
            segment::data_types::groups::GroupId::NumberI64(n) => Self {
                kind: Some(crate::grpc::qdrant::group_id::Kind::IntegerValue(n)),
            },
        }
    }
}

impl TryFrom<NamedVectors> for HashMap<String, segment_vectors::VectorInternal> {
    type Error = Status;

    fn try_from(vectors: NamedVectors) -> Result<Self, Self::Error> {
        vectors
            .vectors
            .into_iter()
            .map(
                |(name, vector)| match segment_vectors::VectorInternal::try_from(vector) {
                    Ok(vector) => Ok((name, vector)),
                    Err(err) => Err(err),
                },
            )
            .collect::<Result<_, _>>()
    }
}

impl From<segment::types::WithVector> for WithVectorsSelector {
    fn from(with_vectors: segment::types::WithVector) -> Self {
        let selector_options = match with_vectors {
            segment::types::WithVector::Bool(enabled) => {
                with_vectors_selector::SelectorOptions::Enable(enabled)
            }
            segment::types::WithVector::Selector(include) => {
                with_vectors_selector::SelectorOptions::Include(VectorsSelector { names: include })
            }
        };
        Self {
            selector_options: Some(selector_options),
        }
    }
}

impl From<WithVectorsSelector> for segment::types::WithVector {
    fn from(with_vectors_selector: WithVectorsSelector) -> Self {
        match with_vectors_selector.selector_options {
            None => Self::default(),
            Some(with_vectors_selector::SelectorOptions::Enable(enabled)) => Self::Bool(enabled),
            Some(with_vectors_selector::SelectorOptions::Include(include)) => {
                Self::Selector(include.names)
            }
        }
    }
}

impl TryFrom<PointId> for segment::types::PointIdType {
    type Error = Status;

    fn try_from(value: PointId) -> Result<Self, Self::Error> {
        match value.point_id_options {
            Some(PointIdOptions::Num(num_id)) => Ok(segment::types::PointIdType::NumId(num_id)),
            Some(PointIdOptions::Uuid(uui_str)) => Uuid::parse_str(&uui_str)
                .map(segment::types::PointIdType::Uuid)
                .map_err(|_err| {
                    Status::invalid_argument(format!("Unable to parse UUID: {uui_str}"))
                }),
            _ => Err(Status::invalid_argument(
                "No ID options provided".to_string(),
            )),
        }
    }
}

impl From<segment::types::ScalarQuantization> for ScalarQuantization {
    fn from(value: segment::types::ScalarQuantization) -> Self {
        let config = value.scalar;
        ScalarQuantization {
            r#type: match config.r#type {
                segment::types::ScalarType::Int8 => {
                    crate::grpc::qdrant::QuantizationType::Int8 as i32
                }
            },
            quantile: config.quantile,
            always_ram: config.always_ram,
        }
    }
}

impl TryFrom<ScalarQuantization> for segment::types::ScalarQuantization {
    type Error = Status;

    fn try_from(value: ScalarQuantization) -> Result<Self, Self::Error> {
        Ok(segment::types::ScalarQuantization {
            scalar: segment::types::ScalarQuantizationConfig {
                r#type: match QuantizationType::try_from(value.r#type).ok() {
                    Some(QuantizationType::Int8) => segment::types::ScalarType::Int8,
                    Some(QuantizationType::UnknownQuantization) | None => {
                        return Err(Status::invalid_argument("Unknown quantization type"));
                    }
                },
                quantile: value.quantile,
                always_ram: value.always_ram,
            },
        })
    }
}

impl From<segment::types::ProductQuantization> for ProductQuantization {
    fn from(value: segment::types::ProductQuantization) -> Self {
        let config = value.product;
        ProductQuantization {
            compression: match config.compression {
                segment::types::CompressionRatio::X4 => CompressionRatio::X4 as i32,
                segment::types::CompressionRatio::X8 => CompressionRatio::X8 as i32,
                segment::types::CompressionRatio::X16 => CompressionRatio::X16 as i32,
                segment::types::CompressionRatio::X32 => CompressionRatio::X32 as i32,
                segment::types::CompressionRatio::X64 => CompressionRatio::X64 as i32,
            },
            always_ram: config.always_ram,
        }
    }
}

impl TryFrom<ProductQuantization> for segment::types::ProductQuantization {
    type Error = Status;

    fn try_from(value: ProductQuantization) -> Result<Self, Self::Error> {
        Ok(segment::types::ProductQuantization {
            product: segment::types::ProductQuantizationConfig {
                compression: match CompressionRatio::try_from(value.compression) {
                    Err(_) => {
                        return Err(Status::invalid_argument(
                            "Unknown compression ratio".to_string(),
                        ));
                    }
                    Ok(CompressionRatio::X4) => segment::types::CompressionRatio::X4,
                    Ok(CompressionRatio::X8) => segment::types::CompressionRatio::X8,
                    Ok(CompressionRatio::X16) => segment::types::CompressionRatio::X16,
                    Ok(CompressionRatio::X32) => segment::types::CompressionRatio::X32,
                    Ok(CompressionRatio::X64) => segment::types::CompressionRatio::X64,
                },
                always_ram: value.always_ram,
            },
        })
    }
}

impl From<segment::types::BinaryQuantization> for BinaryQuantization {
    fn from(value: segment::types::BinaryQuantization) -> Self {
        let config = value.binary;
        BinaryQuantization {
            always_ram: config.always_ram,
        }
    }
}

impl TryFrom<BinaryQuantization> for segment::types::BinaryQuantization {
    type Error = Status;

    fn try_from(value: BinaryQuantization) -> Result<Self, Self::Error> {
        Ok(segment::types::BinaryQuantization {
            binary: segment::types::BinaryQuantizationConfig {
                always_ram: value.always_ram,
            },
        })
    }
}

impl From<segment::types::QuantizationConfig> for QuantizationConfig {
    fn from(value: segment::types::QuantizationConfig) -> Self {
        match value {
            segment::types::QuantizationConfig::Scalar(scalar) => Self {
                quantization: Some(super::qdrant::quantization_config::Quantization::Scalar(
                    scalar.into(),
                )),
            },
            segment::types::QuantizationConfig::Product(product) => Self {
                quantization: Some(super::qdrant::quantization_config::Quantization::Product(
                    product.into(),
                )),
            },
            segment::types::QuantizationConfig::Binary(binary) => Self {
                quantization: Some(super::qdrant::quantization_config::Quantization::Binary(
                    binary.into(),
                )),
            },
        }
    }
}

impl TryFrom<QuantizationConfig> for segment::types::QuantizationConfig {
    type Error = Status;

    fn try_from(value: QuantizationConfig) -> Result<Self, Self::Error> {
        let value = value
            .quantization
            .ok_or_else(|| Status::invalid_argument("Unable to convert quantization config"))?;
        match value {
            super::qdrant::quantization_config::Quantization::Scalar(config) => Ok(
                segment::types::QuantizationConfig::Scalar(config.try_into()?),
            ),
            super::qdrant::quantization_config::Quantization::Product(config) => Ok(
                segment::types::QuantizationConfig::Product(config.try_into()?),
            ),
            super::qdrant::quantization_config::Quantization::Binary(config) => Ok(
                segment::types::QuantizationConfig::Binary(config.try_into()?),
            ),
        }
    }
}

impl From<segment::types::MultiVectorConfig> for MultiVectorConfig {
    fn from(value: segment::types::MultiVectorConfig) -> Self {
        Self {
            comparator: MultiVectorComparator::from(value.comparator) as i32,
        }
    }
}

impl From<segment::types::MultiVectorComparator> for MultiVectorComparator {
    fn from(value: segment::types::MultiVectorComparator) -> Self {
        match value {
            segment::types::MultiVectorComparator::MaxSim => MultiVectorComparator::MaxSim,
        }
    }
}

impl TryFrom<MultiVectorConfig> for segment::types::MultiVectorConfig {
    type Error = Status;

    fn try_from(value: MultiVectorConfig) -> Result<Self, Self::Error> {
        let comparator = MultiVectorComparator::try_from(value.comparator)
            .map_err(|_| Status::invalid_argument("Unknown multi vector comparator"))?;
        Ok(segment::types::MultiVectorConfig {
            comparator: segment::types::MultiVectorComparator::from(comparator),
        })
    }
}

impl From<MultiVectorComparator> for segment::types::MultiVectorComparator {
    fn from(value: MultiVectorComparator) -> Self {
        match value {
            MultiVectorComparator::MaxSim => segment::types::MultiVectorComparator::MaxSim,
        }
    }
}

fn conditions_helper_from_grpc(
    conditions: Vec<Condition>,
) -> Result<Option<Vec<segment::types::Condition>>, tonic::Status> {
    if conditions.is_empty() {
        Ok(None)
    } else {
        let vec = conditions
            .into_iter()
            .map(|c| c.try_into())
            .collect::<Result<_, _>>()?;
        Ok(Some(vec))
    }
}

fn conditions_helper_to_grpc(conditions: Option<Vec<segment::types::Condition>>) -> Vec<Condition> {
    match conditions {
        None => vec![],
        Some(conditions) => {
            if conditions.is_empty() {
                vec![]
            } else {
                conditions
                    .into_iter()
                    .map(Condition::from)
                    .filter(|c| c.condition_one_of.is_some()) // Filter out empty conditions
                    .collect()
            }
        }
    }
}

impl TryFrom<Filter> for segment::types::Filter {
    type Error = Status;

    fn try_from(value: Filter) -> Result<Self, Self::Error> {
        Ok(Self {
            should: conditions_helper_from_grpc(value.should)?,
            min_should: {
                match value.min_should {
                    Some(MinShould {
                        conditions,
                        min_count,
                    }) => Some(segment::types::MinShould {
                        conditions: conditions_helper_from_grpc(conditions)
                            .map(|conds| conds.unwrap_or_default())?,
                        min_count: min_count as usize,
                    }),
                    None => None,
                }
            },
            must: conditions_helper_from_grpc(value.must)?,
            must_not: conditions_helper_from_grpc(value.must_not)?,
        })
    }
}

impl From<segment::types::Filter> for Filter {
    fn from(value: segment::types::Filter) -> Self {
        Self {
            should: conditions_helper_to_grpc(value.should),
            min_should: {
                if let Some(segment::types::MinShould {
                    conditions,
                    min_count,
                }) = value.min_should
                {
                    Some(MinShould {
                        conditions: conditions_helper_to_grpc(Some(conditions)),
                        min_count: min_count as u64,
                    })
                } else {
                    None
                }
            },
            must: conditions_helper_to_grpc(value.must),
            must_not: conditions_helper_to_grpc(value.must_not),
        }
    }
}

impl TryFrom<Condition> for segment::types::Condition {
    type Error = Status;

    fn try_from(value: Condition) -> Result<Self, Self::Error> {
        if let Some(condition) = value.condition_one_of {
            return match condition {
                ConditionOneOf::Field(field) => {
                    Ok(segment::types::Condition::Field(field.try_into()?))
                }
                ConditionOneOf::HasId(has_id) => {
                    Ok(segment::types::Condition::HasId(has_id.try_into()?))
                }
                ConditionOneOf::Filter(filter) => {
                    Ok(segment::types::Condition::Filter(filter.try_into()?))
                }
                ConditionOneOf::IsEmpty(is_empty) => {
                    Ok(segment::types::Condition::IsEmpty(is_empty.try_into()?))
                }
                ConditionOneOf::IsNull(is_null) => {
                    Ok(segment::types::Condition::IsNull(is_null.try_into()?))
                }
                ConditionOneOf::Nested(nested) => Ok(segment::types::Condition::Nested(
                    segment::types::NestedCondition::new(nested.try_into()?),
                )),
                ConditionOneOf::HasVector(has_vector) => Ok(segment::types::Condition::HasVector(
                    segment::types::HasVectorCondition {
                        has_vector: has_vector.has_vector,
                    },
                )),
            };
        }
        Err(Status::invalid_argument("Malformed Condition type"))
    }
}

impl From<segment::types::Condition> for Condition {
    fn from(value: segment::types::Condition) -> Self {
        let condition_one_of = match value {
            segment::types::Condition::Field(field) => {
                Some(ConditionOneOf::Field(FieldCondition::from(field)))
            }
            segment::types::Condition::IsEmpty(is_empty) => {
                Some(ConditionOneOf::IsEmpty(IsEmptyCondition::from(is_empty)))
            }
            segment::types::Condition::IsNull(is_null) => {
                Some(ConditionOneOf::IsNull(IsNullCondition::from(is_null)))
            }
            segment::types::Condition::HasId(has_id) => {
                Some(ConditionOneOf::HasId(HasIdCondition::from(has_id)))
            }
            segment::types::Condition::Filter(filter) => {
                Some(ConditionOneOf::Filter(Filter::from(filter)))
            }
            segment::types::Condition::Nested(nested) => {
                Some(ConditionOneOf::Nested(NestedCondition::from(nested.nested)))
            }
            // This type of condition should be only applied locally
            // and never be sent to the other peers
            segment::types::Condition::CustomIdChecker(_) => None,
            segment::types::Condition::HasVector(has_vector) => {
                Some(ConditionOneOf::HasVector(HasVectorCondition {
                    has_vector: has_vector.has_vector,
                }))
            }
        };

        Self { condition_one_of }
    }
}

impl TryFrom<NestedCondition> for segment::types::Nested {
    type Error = Status;

    fn try_from(value: NestedCondition) -> Result<Self, Self::Error> {
        match value.filter {
            None => Err(Status::invalid_argument(
                "Nested condition must have a filter",
            )),
            Some(filter) => Ok(Self {
                key: json::json_path_from_proto(&value.key)?,
                filter: filter.try_into()?,
            }),
        }
    }
}

impl From<segment::types::Nested> for NestedCondition {
    fn from(value: segment::types::Nested) -> Self {
        Self {
            key: value.key.to_string(),
            filter: Some(value.filter.into()),
        }
    }
}

impl TryFrom<IsEmptyCondition> for segment::types::IsEmptyCondition {
    type Error = Status;

    fn try_from(value: IsEmptyCondition) -> Result<Self, Status> {
        Ok(segment::types::IsEmptyCondition {
            is_empty: segment::types::PayloadField {
                key: json::json_path_from_proto(&value.key)?,
            },
        })
    }
}

impl From<segment::types::IsEmptyCondition> for IsEmptyCondition {
    fn from(value: segment::types::IsEmptyCondition) -> Self {
        Self {
            key: value.is_empty.key.to_string(),
        }
    }
}

impl TryFrom<IsNullCondition> for segment::types::IsNullCondition {
    type Error = Status;

    fn try_from(value: IsNullCondition) -> Result<Self, Status> {
        Ok(segment::types::IsNullCondition {
            is_null: segment::types::PayloadField {
                key: json::json_path_from_proto(&value.key)?,
            },
        })
    }
}

impl From<segment::types::IsNullCondition> for IsNullCondition {
    fn from(value: segment::types::IsNullCondition) -> Self {
        Self {
            key: value.is_null.key.to_string(),
        }
    }
}

impl TryFrom<HasIdCondition> for segment::types::HasIdCondition {
    type Error = Status;

    fn try_from(value: HasIdCondition) -> Result<Self, Self::Error> {
        let set: HashSet<segment::types::PointIdType> = value
            .has_id
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?;
        Ok(Self { has_id: set })
    }
}

impl From<segment::types::HasIdCondition> for HasIdCondition {
    fn from(value: segment::types::HasIdCondition) -> Self {
        let set: Vec<PointId> = value.has_id.into_iter().map(|p| p.into()).collect();
        Self { has_id: set }
    }
}

impl TryFrom<FieldCondition> for segment::types::FieldCondition {
    type Error = Status;

    fn try_from(value: FieldCondition) -> Result<Self, Self::Error> {
        let FieldCondition {
            key,
            r#match,
            range,
            geo_bounding_box,
            geo_radius,
            values_count,
            geo_polygon,
            datetime_range,
        } = value;

        let geo_bounding_box =
            geo_bounding_box.map_or_else(|| Ok(None), |g| g.try_into().map(Some))?;
        let geo_radius = geo_radius.map_or_else(|| Ok(None), |g| g.try_into().map(Some))?;
        let geo_polygon = geo_polygon.map_or_else(|| Ok(None), |g| g.try_into().map(Some))?;

        let mut range = range.map(Into::into);
        if range.is_none() {
            range = datetime_range
                .map(segment::types::RangeInterface::try_from)
                .transpose()?;
        }

        Ok(Self {
            key: json::json_path_from_proto(&key)?,
            r#match: r#match.map_or_else(|| Ok(None), |m| m.try_into().map(Some))?,
            range,
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count: values_count.map(Into::into),
        })
    }
}

impl From<segment::types::FieldCondition> for FieldCondition {
    fn from(value: segment::types::FieldCondition) -> Self {
        let segment::types::FieldCondition {
            key,
            r#match,
            range,
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count,
        } = value;

        let (range, datetime_range) = match range {
            Some(segment::types::RangeInterface::Float(range)) => (Some(range.into()), None),
            Some(segment::types::RangeInterface::DateTime(range)) => (None, Some(range.into())),
            None => (None, None),
        };

        Self {
            key: key.to_string(),
            r#match: r#match.map(Into::into),
            range,
            geo_bounding_box: geo_bounding_box.map(Into::into),
            geo_radius: geo_radius.map(Into::into),
            geo_polygon: geo_polygon.map(Into::into),
            values_count: values_count.map(Into::into),
            datetime_range,
        }
    }
}

impl TryFrom<GeoBoundingBox> for segment::types::GeoBoundingBox {
    type Error = Status;

    fn try_from(value: GeoBoundingBox) -> Result<Self, Self::Error> {
        match value {
            GeoBoundingBox {
                top_left: Some(t),
                bottom_right: Some(b),
            } => Ok(Self {
                top_left: t.into(),
                bottom_right: b.into(),
            }),
            _ => Err(Status::invalid_argument("Malformed GeoBoundingBox type")),
        }
    }
}

impl From<segment::types::GeoBoundingBox> for GeoBoundingBox {
    fn from(value: segment::types::GeoBoundingBox) -> Self {
        Self {
            top_left: Some(value.top_left.into()),
            bottom_right: Some(value.bottom_right.into()),
        }
    }
}

impl TryFrom<GeoRadius> for segment::types::GeoRadius {
    type Error = Status;

    fn try_from(value: GeoRadius) -> Result<Self, Self::Error> {
        match value {
            GeoRadius {
                center: Some(c),
                radius,
            } => Ok(Self {
                center: c.into(),
                radius: radius.into(),
            }),
            _ => Err(Status::invalid_argument("Malformed GeoRadius type")),
        }
    }
}

impl From<segment::types::GeoRadius> for GeoRadius {
    fn from(value: segment::types::GeoRadius) -> Self {
        Self {
            center: Some(value.center.into()),
            radius: value.radius as f32, // TODO lossy ok?
        }
    }
}

impl TryFrom<GeoPolygon> for segment::types::GeoPolygon {
    type Error = Status;

    fn try_from(value: GeoPolygon) -> Result<Self, Self::Error> {
        match value {
            GeoPolygon {
                exterior: Some(e),
                interiors,
            } => Ok(Self {
                exterior: e.into(),
                interiors: Some(interiors.into_iter().map(Into::into).collect()),
            }),
            _ => Err(Status::invalid_argument(
                "Malformed GeoPolygon type - field `exterior` is required",
            )),
        }
    }
}

impl From<segment::types::GeoPolygon> for GeoPolygon {
    fn from(value: segment::types::GeoPolygon) -> Self {
        Self {
            exterior: Some(value.exterior.into()),
            interiors: value
                .interiors
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<GeoPoint> for segment::types::GeoPoint {
    fn from(value: GeoPoint) -> Self {
        Self {
            lon: value.lon,
            lat: value.lat,
        }
    }
}

impl From<GeoLineString> for segment::types::GeoLineString {
    fn from(value: GeoLineString) -> Self {
        Self {
            points: value.points.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<segment::types::GeoLineString> for GeoLineString {
    fn from(value: segment::types::GeoLineString) -> Self {
        Self {
            points: value.points.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Range> for segment::types::Range<FloatPayloadType> {
    fn from(value: Range) -> Self {
        Self {
            lt: value.lt,
            gt: value.gt,
            gte: value.gte,
            lte: value.lte,
        }
    }
}

impl From<segment::types::Range<FloatPayloadType>> for Range {
    fn from(value: segment::types::Range<FloatPayloadType>) -> Self {
        Self {
            lt: value.lt,
            gt: value.gt,
            gte: value.gte,
            lte: value.lte,
        }
    }
}

impl From<Range> for segment::types::RangeInterface {
    fn from(value: Range) -> Self {
        Self::Float(value.into())
    }
}

impl TryFrom<DatetimeRange> for segment::types::RangeInterface {
    type Error = Status;

    fn try_from(value: DatetimeRange) -> Result<Self, Self::Error> {
        Ok(Self::DateTime(segment::types::Range {
            lt: value.lt.map(try_date_time_from_proto).transpose()?,
            gt: value.gt.map(try_date_time_from_proto).transpose()?,
            gte: value.gte.map(try_date_time_from_proto).transpose()?,
            lte: value.lte.map(try_date_time_from_proto).transpose()?,
        }))
    }
}

impl From<segment::types::Range<DateTimePayloadType>> for DatetimeRange {
    fn from(value: segment::types::Range<DateTimePayloadType>) -> Self {
        Self {
            lt: value.lt.map(date_time_to_proto),
            gt: value.gt.map(date_time_to_proto),
            gte: value.gte.map(date_time_to_proto),
            lte: value.lte.map(date_time_to_proto),
        }
    }
}

impl From<ValuesCount> for segment::types::ValuesCount {
    fn from(value: ValuesCount) -> Self {
        Self {
            lt: value.lt.map(|x| x as usize),
            gt: value.gt.map(|x| x as usize),
            gte: value.gte.map(|x| x as usize),
            lte: value.lte.map(|x| x as usize),
        }
    }
}

impl From<segment::types::ValuesCount> for ValuesCount {
    fn from(value: segment::types::ValuesCount) -> Self {
        Self {
            lt: value.lt.map(|x| x as u64),
            gt: value.gt.map(|x| x as u64),
            gte: value.gte.map(|x| x as u64),
            lte: value.lte.map(|x| x as u64),
        }
    }
}

impl TryFrom<Match> for segment::types::Match {
    type Error = Status;

    fn try_from(value: Match) -> Result<Self, Self::Error> {
        match value.match_value {
            Some(mv) => Ok(match mv {
                MatchValue::Keyword(kw) => kw.into(),
                MatchValue::Integer(int) => int.into(),
                MatchValue::Boolean(flag) => flag.into(),
                MatchValue::Text(text) => segment::types::Match::Text(text.into()),
                MatchValue::Keywords(kwds) => kwds.strings.into(),
                MatchValue::Integers(ints) => ints.integers.into(),
                MatchValue::ExceptIntegers(kwds) => {
                    segment::types::Match::Except(kwds.integers.into())
                }
                MatchValue::ExceptKeywords(ints) => {
                    segment::types::Match::Except(ints.strings.into())
                }
            }),
            _ => Err(Status::invalid_argument("Malformed Match condition")),
        }
    }
}

impl From<segment::types::Match> for Match {
    fn from(value: segment::types::Match) -> Self {
        let match_value = match value {
            segment::types::Match::Value(value) => match value.value {
                segment::types::ValueVariants::String(kw) => MatchValue::Keyword(kw),
                segment::types::ValueVariants::Integer(int) => MatchValue::Integer(int),
                segment::types::ValueVariants::Bool(flag) => MatchValue::Boolean(flag),
            },
            segment::types::Match::Text(segment::types::MatchText { text }) => {
                MatchValue::Text(text)
            }
            segment::types::Match::Any(any) => match any.any {
                segment::types::AnyVariants::Strings(strings) => {
                    let strings = strings.into_iter().collect();
                    MatchValue::Keywords(RepeatedStrings { strings })
                }
                segment::types::AnyVariants::Integers(integers) => {
                    let integers = integers.into_iter().collect();
                    MatchValue::Integers(RepeatedIntegers { integers })
                }
            },
            segment::types::Match::Except(except) => match except.except {
                segment::types::AnyVariants::Strings(strings) => {
                    let strings = strings.into_iter().collect();
                    MatchValue::ExceptKeywords(RepeatedStrings { strings })
                }
                segment::types::AnyVariants::Integers(integers) => {
                    let integers = integers.into_iter().collect();
                    MatchValue::ExceptIntegers(RepeatedIntegers { integers })
                }
            },
        };
        Self {
            match_value: Some(match_value),
        }
    }
}

impl From<Direction> for segment::data_types::order_by::Direction {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Asc => segment::data_types::order_by::Direction::Asc,
            Direction::Desc => segment::data_types::order_by::Direction::Desc,
        }
    }
}

impl From<segment::data_types::order_by::Direction> for Direction {
    fn from(value: segment::data_types::order_by::Direction) -> Self {
        match value {
            segment::data_types::order_by::Direction::Asc => Direction::Asc,
            segment::data_types::order_by::Direction::Desc => Direction::Desc,
        }
    }
}
impl TryFrom<OrderBy> for segment::data_types::order_by::OrderBy {
    type Error = Status;

    fn try_from(value: OrderBy) -> Result<Self, Self::Error> {
        use segment::data_types::order_by::StartFrom;

        use crate::conversions::json;
        use crate::grpc::qdrant::start_from::Value;

        let direction = value
            .direction
            .and_then(|x|
                // XXX: Invalid values silently converted to None
                Direction::try_from(x).ok())
            .map(segment::data_types::order_by::Direction::from);

        let start_from = value
            .start_from
            .and_then(|value| value.value)
            .map(|v| -> Result<StartFrom, Status> {
                match v {
                    Value::Integer(int) => Ok(StartFrom::Integer(int)),
                    Value::Float(float) => Ok(StartFrom::Float(float)),
                    Value::Timestamp(timestamp) => {
                        Ok(StartFrom::Datetime(try_date_time_from_proto(timestamp)?))
                    }
                    Value::Datetime(datetime_str) => Ok(StartFrom::Datetime(
                        segment::types::DateTimeWrapper::from_str(&datetime_str).map_err(|e| {
                            Status::invalid_argument(format!("Malformed datetime: {e}"))
                        })?,
                    )),
                }
            })
            .transpose()?;

        Ok(Self {
            key: json::json_path_from_proto(&value.key)?,
            direction,
            start_from,
        })
    }
}

impl From<segment::data_types::order_by::OrderBy> for OrderBy {
    fn from(value: segment::data_types::order_by::OrderBy) -> Self {
        Self {
            key: value.key.to_string(),
            direction: value.direction.map(|d| Direction::from(d) as i32),
            start_from: value.start_from.map(|start_from| start_from.into()),
        }
    }
}

impl From<segment::data_types::order_by::StartFrom> for StartFrom {
    fn from(value: segment::data_types::order_by::StartFrom) -> Self {
        Self {
            value: Some(match value {
                segment::data_types::order_by::StartFrom::Integer(int) => {
                    start_from::Value::Integer(int)
                }
                segment::data_types::order_by::StartFrom::Float(float) => {
                    start_from::Value::Float(float)
                }
                segment::data_types::order_by::StartFrom::Datetime(datetime) => {
                    start_from::Value::Timestamp(date_time_to_proto(datetime))
                }
            }),
        }
    }
}

impl From<HnswConfigDiff> for segment::types::HnswConfig {
    fn from(hnsw_config: HnswConfigDiff) -> Self {
        Self {
            m: hnsw_config.m.unwrap_or_default() as usize,
            ef_construct: hnsw_config.ef_construct.unwrap_or_default() as usize,
            full_scan_threshold: hnsw_config.full_scan_threshold.unwrap_or_default() as usize,
            max_indexing_threads: hnsw_config.max_indexing_threads.unwrap_or_default() as usize,
            on_disk: hnsw_config.on_disk,
            payload_m: hnsw_config.payload_m.map(|x| x as usize),
        }
    }
}

impl From<StrictModeConfig> for segment::types::StrictModeConfig {
    fn from(value: StrictModeConfig) -> Self {
        Self {
            enabled: value.enabled,
            max_query_limit: value.max_query_limit.map(|i| i as usize),
            max_timeout: value.max_timeout.map(|i| i as usize),
            unindexed_filtering_retrieve: value.unindexed_filtering_retrieve,
            unindexed_filtering_update: value.unindexed_filtering_update,
            search_max_hnsw_ef: value.search_max_hnsw_ef.map(|i| i as usize),
            search_allow_exact: value.search_allow_exact,
            search_max_oversampling: value.search_max_oversampling.map(f64::from),
            upsert_max_batchsize: value.upsert_max_batchsize.map(|i| i as usize),
            max_collection_vector_size_bytes: value
                .max_collection_vector_size_bytes
                .map(|i| i as usize),
            read_rate_limit_per_sec: value.read_rate_limit_per_sec.map(|i| i as usize),
            write_rate_limit_per_sec: value.write_rate_limit_per_sec.map(|i| i as usize),
        }
    }
}

impl From<segment::types::StrictModeConfig> for StrictModeConfig {
    fn from(value: segment::types::StrictModeConfig) -> Self {
        Self {
            enabled: value.enabled,
            max_query_limit: value.max_query_limit.map(|i| i as u32),
            max_timeout: value.max_timeout.map(|i| i as u32),
            unindexed_filtering_retrieve: value.unindexed_filtering_retrieve,
            unindexed_filtering_update: value.unindexed_filtering_update,
            search_max_hnsw_ef: value.search_max_hnsw_ef.map(|i| i as u32),
            search_allow_exact: value.search_allow_exact,
            search_max_oversampling: value.search_max_oversampling.map(|i| i as f32),
            upsert_max_batchsize: value.upsert_max_batchsize.map(|i| i as u64),
            max_collection_vector_size_bytes: value
                .max_collection_vector_size_bytes
                .map(|i| i as u64),
            read_rate_limit_per_sec: value.read_rate_limit_per_sec.map(|i| i as u32),
            write_rate_limit_per_sec: value.write_rate_limit_per_sec.map(|i| i as u32),
        }
    }
}

pub fn naive_date_time_to_proto(date_time: NaiveDateTime) -> prost_wkt_types::Timestamp {
    prost_wkt_types::Timestamp {
        seconds: date_time.and_utc().timestamp(), // number of non-leap seconds since the midnight on January 1, 1970.
        nanos: date_time.nanosecond() as i32,
    }
}

pub fn date_time_to_proto(date_time: DateTimePayloadType) -> prost_wkt_types::Timestamp {
    naive_date_time_to_proto(date_time.0.naive_utc())
}

pub fn try_date_time_from_proto(
    date_time: prost_wkt_types::Timestamp,
) -> Result<DateTimePayloadType, Status> {
    chrono::DateTime::from_timestamp(date_time.seconds, date_time.nanos.try_into().unwrap_or(0))
        .map(|date_time| date_time.into())
        .ok_or_else(|| Status::invalid_argument(format!("Unable to parse timestamp: {date_time}")))
}

impl TryFrom<Distance> for segment::types::Distance {
    type Error = Status;

    fn try_from(value: Distance) -> Result<Self, Self::Error> {
        Ok(match value {
            Distance::UnknownDistance => {
                return Err(Status::invalid_argument(
                    "Malformed distance parameter: UnknownDistance",
                ));
            }
            Distance::Cosine => segment::types::Distance::Cosine,
            Distance::Euclid => segment::types::Distance::Euclid,
            Distance::Dot => segment::types::Distance::Dot,
            Distance::Manhattan => segment::types::Distance::Manhattan,
        })
    }
}

pub fn from_grpc_dist(dist: i32) -> Result<segment::types::Distance, Status> {
    match Distance::try_from(dist) {
        Err(_) => Err(Status::invalid_argument(format!(
            "Malformed distance parameter, unexpected value: {dist}"
        ))),
        Ok(grpc_distance) => Ok(grpc_distance.try_into()?),
    }
}

pub fn into_named_vector_struct(
    vector_name: Option<String>,
    vector: segment_vectors::DenseVector,
    indices: Option<SparseIndices>,
) -> Result<segment_vectors::NamedVectorStruct, Status> {
    use segment_vectors::{NamedSparseVector, NamedVector, NamedVectorStruct};
    use sparse::common::sparse_vector::SparseVector;
    Ok(match indices {
        Some(indices) => NamedVectorStruct::Sparse(NamedSparseVector {
            name: vector_name
                .ok_or_else(|| Status::invalid_argument("Sparse vector must have a name"))?,
            vector: SparseVector::new(indices.data, vector).map_err(|e| {
                Status::invalid_argument(format!(
                    "Sparse indices does not match sparse vector conditions: {e}"
                ))
            })?,
        }),
        None => {
            if let Some(vector_name) = vector_name {
                NamedVectorStruct::Dense(NamedVector {
                    name: vector_name,
                    vector,
                })
            } else {
                NamedVectorStruct::Default(vector)
            }
        }
    })
}

impl From<PointsOperationResponseInternal> for PointsOperationResponse {
    fn from(resp: PointsOperationResponseInternal) -> Self {
        Self {
            result: resp.result.map(Into::into),
            time: resp.time,
        }
    }
}

// TODO: Make it explicit `from_operations_response` method instead of `impl From<PointsOperationResponse>`?
impl From<PointsOperationResponse> for PointsOperationResponseInternal {
    fn from(resp: PointsOperationResponse) -> Self {
        Self {
            result: resp.result.map(Into::into),
            time: resp.time,
        }
    }
}

impl From<UpdateResultInternal> for UpdateResult {
    fn from(res: UpdateResultInternal) -> Self {
        Self {
            operation_id: res.operation_id,
            status: res.status,
        }
    }
}

// TODO: Make it explicit `from_update_result` method instead of `impl From<UpdateResult>`?
impl From<UpdateResult> for UpdateResultInternal {
    fn from(res: UpdateResult) -> Self {
        Self {
            operation_id: res.operation_id,
            status: res.status,
            clock_tag: None,
        }
    }
}

impl From<RecommendStrategy> for crate::rest::RecommendStrategy {
    fn from(value: RecommendStrategy) -> Self {
        match value {
            RecommendStrategy::AverageVector => crate::rest::RecommendStrategy::AverageVector,
            RecommendStrategy::BestScore => crate::rest::RecommendStrategy::BestScore,
        }
    }
}

impl TryFrom<i32> for crate::rest::RecommendStrategy {
    type Error = Status;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let strategy = RecommendStrategy::try_from(value).map_err(|_| {
            Status::invalid_argument(format!("Unknown recommend strategy: {value}"))
        })?;
        Ok(strategy.into())
    }
}

impl From<segment_query::RecoQuery<segment_vectors::VectorInternal>> for raw_query::Recommend {
    fn from(value: segment_query::RecoQuery<segment_vectors::VectorInternal>) -> Self {
        Self {
            positives: value.positives.into_iter().map(RawVector::from).collect(),
            negatives: value.negatives.into_iter().map(RawVector::from).collect(),
        }
    }
}

impl TryFrom<raw_query::Recommend> for segment_query::RecoQuery<segment_vectors::VectorInternal> {
    type Error = Status;
    fn try_from(value: raw_query::Recommend) -> Result<Self, Self::Error> {
        Ok(Self {
            positives: value
                .positives
                .into_iter()
                .map(segment_vectors::VectorInternal::try_from)
                .try_collect()?,
            negatives: value
                .negatives
                .into_iter()
                .map(segment_vectors::VectorInternal::try_from)
                .try_collect()?,
        })
    }
}

impl From<segment_query::ContextPair<segment_vectors::VectorInternal>>
    for raw_query::RawContextPair
{
    fn from(value: segment_query::ContextPair<segment_vectors::VectorInternal>) -> Self {
        Self {
            positive: Some(RawVector::from(value.positive)),
            negative: Some(RawVector::from(value.negative)),
        }
    }
}

impl TryFrom<raw_query::RawContextPair>
    for segment_query::ContextPair<segment_vectors::VectorInternal>
{
    type Error = Status;
    fn try_from(value: raw_query::RawContextPair) -> Result<Self, Self::Error> {
        Ok(Self {
            positive: value
                .positive
                .map(segment_vectors::VectorInternal::try_from)
                .transpose()?
                .ok_or_else(|| {
                    Status::invalid_argument("No positive part of context pair provided")
                })?,
            negative: value
                .negative
                .map(segment_vectors::VectorInternal::try_from)
                .transpose()?
                .ok_or_else(|| {
                    Status::invalid_argument("No negative part of context pair provided")
                })?,
        })
    }
}

impl From<segment_query::ContextQuery<segment_vectors::VectorInternal>> for raw_query::Context {
    fn from(value: segment_query::ContextQuery<segment_vectors::VectorInternal>) -> Self {
        Self {
            context: value.pairs.into_iter().map(RawContextPair::from).collect(),
        }
    }
}

impl TryFrom<raw_query::Context> for segment_query::ContextQuery<segment_vectors::VectorInternal> {
    type Error = Status;
    fn try_from(value: raw_query::Context) -> Result<Self, Self::Error> {
        Ok(Self {
            pairs: value
                .context
                .into_iter()
                .map(segment_query::ContextPair::try_from)
                .try_collect()?,
        })
    }
}

impl From<segment_query::DiscoveryQuery<segment_vectors::VectorInternal>> for raw_query::Discovery {
    fn from(value: segment_query::DiscoveryQuery<segment_vectors::VectorInternal>) -> Self {
        Self {
            target: Some(RawVector::from(value.target)),
            context: value.pairs.into_iter().map(RawContextPair::from).collect(),
        }
    }
}

impl TryFrom<raw_query::Discovery>
    for segment_query::DiscoveryQuery<segment_vectors::VectorInternal>
{
    type Error = Status;
    fn try_from(value: raw_query::Discovery) -> Result<Self, Self::Error> {
        Ok(Self {
            target: value
                .target
                .map(segment_vectors::VectorInternal::try_from)
                .transpose()?
                .ok_or_else(|| Status::invalid_argument("No target provided"))?,
            pairs: value
                .context
                .into_iter()
                .map(segment_query::ContextPair::try_from)
                .try_collect()?,
        })
    }
}

impl TryFrom<SearchPoints> for rest::SearchRequestInternal {
    type Error = Status;

    fn try_from(value: SearchPoints) -> Result<Self, Self::Error> {
        let named_struct = crate::grpc::conversions::into_named_vector_struct(
            value.vector_name,
            value.vector,
            value.sparse_indices,
        )?;
        let vector = match named_struct {
            segment_vectors::NamedVectorStruct::Default(v) => rest::NamedVectorStruct::Default(v),
            segment_vectors::NamedVectorStruct::Dense(v) => rest::NamedVectorStruct::Dense(v),
            segment_vectors::NamedVectorStruct::Sparse(v) => rest::NamedVectorStruct::Sparse(v),
            segment_vectors::NamedVectorStruct::MultiDense(_) => {
                return Err(Status::invalid_argument(
                    "MultiDense vector is not supported in search request",
                ))
            }
        };
        Ok(Self {
            vector,
            filter: value.filter.map(|f| f.try_into()).transpose()?,
            params: value.params.map(|p| p.into()),
            limit: value.limit as usize,
            offset: value.offset.map(|x| x as usize),
            with_payload: value.with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: Some(
                value
                    .with_vectors
                    .map(|with_vectors| with_vectors.into())
                    .unwrap_or_default(),
            ),
            score_threshold: value.score_threshold,
        })
    }
}

impl TryFrom<SearchPointGroups> for rest::SearchGroupsRequestInternal {
    type Error = Status;

    fn try_from(value: SearchPointGroups) -> Result<Self, Self::Error> {
        let search_points = SearchPoints {
            vector: value.vector,
            filter: value.filter,
            params: value.params,
            with_payload: value.with_payload,
            with_vectors: value.with_vectors,
            score_threshold: value.score_threshold,
            vector_name: value.vector_name,
            limit: 0,
            offset: None,
            collection_name: String::new(),
            read_consistency: None,
            timeout: None,
            shard_key_selector: None,
            sparse_indices: value.sparse_indices,
        };

        if let Some(sparse_indices) = &search_points.sparse_indices {
            validate_sparse_vector_impl(&sparse_indices.data, &search_points.vector).map_err(
                |e| {
                    Status::invalid_argument(format!(
                        "Sparse indices does not match sparse vector conditions: {e}"
                    ))
                },
            )?;
        }

        let rest::SearchRequestInternal {
            vector,
            filter,
            params,
            limit: _,
            offset: _,
            with_payload,
            with_vector,
            score_threshold,
        } = search_points.try_into()?;

        Ok(Self {
            vector,
            filter,
            params,
            with_payload,
            with_vector,
            score_threshold,
            group_request: rest::BaseGroupRequest {
                group_by: json::json_path_from_proto(&value.group_by)?,
                limit: value.limit,
                group_size: value.group_size,
                with_lookup: value
                    .with_lookup
                    .map(rest::WithLookupInterface::try_from)
                    .transpose()?,
            },
        })
    }
}

impl TryFrom<WithLookup> for rest::WithLookupInterface {
    type Error = Status;

    fn try_from(value: WithLookup) -> Result<Self, Self::Error> {
        Ok(Self::WithLookup(value.try_into()?))
    }
}

impl TryFrom<WithLookup> for rest::WithLookup {
    type Error = Status;

    fn try_from(value: WithLookup) -> Result<Self, Self::Error> {
        let with_default_payload = || Some(segment::types::WithPayloadInterface::Bool(true));

        Ok(Self {
            collection_name: value.collection,
            with_payload: value
                .with_payload
                .map(|wp| wp.try_into())
                .transpose()?
                .or_else(with_default_payload),
            with_vectors: value.with_vectors.map(|wv| wv.into()),
        })
    }
}

impl From<LookupLocation> for rest::LookupLocation {
    fn from(value: LookupLocation) -> Self {
        Self {
            collection: value.collection_name,
            vector: value.vector_name,
            shard_key: value.shard_key_selector.map(rest::ShardKeySelector::from),
        }
    }
}

impl TryFrom<FacetHitInternal> for segment_facets::FacetValueHit {
    type Error = Status;

    fn try_from(hit: FacetHitInternal) -> Result<Self, Self::Error> {
        let value = hit
            .value
            .ok_or_else(|| Status::internal("expected FacetHit to have a value"))?;

        Ok(Self {
            value: segment_facets::FacetValue::try_from(value)?,
            count: hit.count as usize,
        })
    }
}

impl From<segment_facets::FacetValueHit> for FacetHitInternal {
    fn from(hit: segment_facets::FacetValueHit) -> Self {
        Self {
            value: Some(From::from(hit.value)),
            count: hit.count as u64,
        }
    }
}

impl From<segment_facets::FacetValueHit> for FacetHit {
    fn from(hit: segment_facets::FacetValueHit) -> Self {
        Self {
            value: Some(hit.value.into()),
            count: hit.count as u64,
        }
    }
}

impl TryFrom<FacetValueInternal> for segment_facets::FacetValue {
    type Error = Status;

    fn try_from(value: FacetValueInternal) -> Result<Self, Self::Error> {
        use super::qdrant::facet_value_internal::Variant;

        let variant = value
            .variant
            .ok_or_else(|| Status::internal("expected FacetValueInternal to have a value"))?;

        Ok(match variant {
            Variant::KeywordValue(value) => segment_facets::FacetValue::Keyword(value),
            Variant::IntegerValue(value) => segment_facets::FacetValue::Int(value),
            Variant::UuidValue(value) => {
                let uuid_bytes: [u8; 16] = value.try_into().map_err(|_| {
                    Status::invalid_argument("Unable to parse UUID: expected 16 bytes")
                })?;
                segment_facets::FacetValue::Uuid(Uuid::from_bytes(uuid_bytes).as_u128())
            }
            Variant::BoolValue(value) => segment_facets::FacetValue::Bool(value),
        })
    }
}

impl From<segment_facets::FacetValue> for FacetValueInternal {
    fn from(value: segment_facets::FacetValue) -> Self {
        use super::qdrant::facet_value_internal::Variant;

        Self {
            variant: Some(match value {
                segment_facets::FacetValue::Keyword(value) => Variant::KeywordValue(value),
                segment_facets::FacetValue::Int(value) => Variant::IntegerValue(value),
                segment_facets::FacetValue::Uuid(value) => {
                    let uuid = Uuid::from_u128(value);
                    Variant::UuidValue(uuid.as_bytes().to_vec())
                }
                segment_facets::FacetValue::Bool(value) => Variant::BoolValue(value),
            }),
        }
    }
}

impl From<segment_facets::FacetValue> for FacetValue {
    fn from(value: segment_facets::FacetValue) -> Self {
        use super::qdrant::facet_value::Variant;

        Self {
            variant: Some(match value {
                segment_facets::FacetValue::Keyword(value) => Variant::StringValue(value),
                segment_facets::FacetValue::Int(value) => Variant::IntegerValue(value),
                segment_facets::FacetValue::Uuid(value) => {
                    Variant::StringValue(Uuid::from_u128(value).to_string())
                }
                segment_facets::FacetValue::Bool(value) => Variant::BoolValue(value),
            }),
        }
    }
}

impl From<rest::SearchMatrixPair> for SearchMatrixPair {
    fn from(pair: rest::SearchMatrixPair) -> Self {
        Self {
            a: Some(pair.a.into()),
            b: Some(pair.b.into()),
            score: pair.score,
        }
    }
}

impl From<HwMeasurementAcc> for HardwareUsage {
    fn from(value: HwMeasurementAcc) -> Self {
        Self {
            cpu: value.get_cpu() as u64,
        }
    }
}

impl From<HardwareUsage> for HardwareCounterCell {
    fn from(value: HardwareUsage) -> Self {
        let HardwareUsage { cpu } = value;
        HardwareCounterCell::new_with(cpu as usize)
    }
}
