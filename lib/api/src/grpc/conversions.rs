use std::collections::{HashMap, HashSet};
use std::time::Instant;

use chrono::{NaiveDateTime, Timelike};
use segment::data_types::integer_index::IntegerIndexType;
use segment::data_types::text_index::TextIndexType;
use segment::data_types::vectors::VectorElementType;
use segment::types::default_quantization_ignore_value;
use tonic::Status;
use uuid::Uuid;

use super::qdrant::{BinaryQuantization, CompressionRatio, GeoLineString, GroupId, SparseIndices};
use crate::grpc::models::{CollectionsResponse, VersionInfo};
use crate::grpc::qdrant::condition::ConditionOneOf;
use crate::grpc::qdrant::payload_index_params::IndexParams;
use crate::grpc::qdrant::point_id::PointIdOptions;
use crate::grpc::qdrant::r#match::MatchValue;
use crate::grpc::qdrant::value::Kind;
use crate::grpc::qdrant::vectors::VectorsOptions;
use crate::grpc::qdrant::with_payload_selector::SelectorOptions;
use crate::grpc::qdrant::{
    shard_key, with_vectors_selector, CollectionDescription, CollectionOperationResponse,
    Condition, Distance, FieldCondition, Filter, GeoBoundingBox, GeoPoint, GeoPolygon, GeoRadius,
    HasIdCondition, HealthCheckReply, HnswConfigDiff, IntegerParams, IsEmptyCondition,
    IsNullCondition, ListCollectionsResponse, ListValue, Match, NamedVectors, NestedCondition,
    PayloadExcludeSelector, PayloadIncludeSelector, PayloadIndexParams, PayloadSchemaInfo,
    PayloadSchemaType, PointId, ProductQuantization, QuantizationConfig, QuantizationSearchParams,
    QuantizationType, Range, RepeatedIntegers, RepeatedStrings, ScalarQuantization, ScoredPoint,
    SearchParams, ShardKey, Struct, TextIndexParams, TokenizerType, Value, ValuesCount, Vector,
    Vectors, VectorsSelector, WithPayloadSelector, WithVectorsSelector,
};

pub fn payload_to_proto(payload: segment::types::Payload) -> HashMap<String, Value> {
    payload
        .into_iter()
        .map(|(k, v)| (k, json_to_proto(v)))
        .collect()
}

fn json_to_proto(json_value: serde_json::Value) -> Value {
    match json_value {
        serde_json::Value::Null => Value {
            kind: Some(Kind::NullValue(0)),
        },
        serde_json::Value::Bool(v) => Value {
            kind: Some(Kind::BoolValue(v)),
        },
        serde_json::Value::Number(n) => Value {
            kind: if let Some(int) = n.as_i64() {
                Some(Kind::IntegerValue(int))
            } else {
                Some(Kind::DoubleValue(n.as_f64().unwrap()))
            },
        },
        serde_json::Value::String(s) => Value {
            kind: Some(Kind::StringValue(s)),
        },
        serde_json::Value::Array(v) => {
            let list = v.into_iter().map(json_to_proto).collect();
            Value {
                kind: Some(Kind::ListValue(ListValue { values: list })),
            }
        }
        serde_json::Value::Object(m) => {
            let map = m.into_iter().map(|(k, v)| (k, json_to_proto(v))).collect();
            Value {
                kind: Some(Kind::StructValue(Struct { fields: map })),
            }
        }
    }
}

pub fn proto_to_payloads(proto: HashMap<String, Value>) -> Result<segment::types::Payload, Status> {
    let mut map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for (k, v) in proto.into_iter() {
        map.insert(k, proto_to_json(v)?);
    }
    Ok(map.into())
}

fn proto_to_json(proto: Value) -> Result<serde_json::Value, Status> {
    match proto.kind {
        None => Ok(serde_json::Value::default()),
        Some(kind) => match kind {
            Kind::NullValue(_) => Ok(serde_json::Value::Null),
            Kind::DoubleValue(n) => {
                let v = match serde_json::Number::from_f64(n) {
                    Some(f) => f,
                    None => return Err(Status::invalid_argument("cannot convert to json number")),
                };
                Ok(serde_json::Value::Number(v))
            }
            Kind::IntegerValue(i) => Ok(serde_json::Value::Number(i.into())),
            Kind::StringValue(s) => Ok(serde_json::Value::String(s)),
            Kind::BoolValue(b) => Ok(serde_json::Value::Bool(b)),
            Kind::StructValue(s) => {
                let mut map = serde_json::Map::new();
                for (k, v) in s.fields.into_iter() {
                    map.insert(k, proto_to_json(v)?);
                }
                Ok(serde_json::Value::Object(map))
            }
            Kind::ListValue(l) => {
                let mut list = Vec::new();
                for v in l.values.into_iter() {
                    list.push(proto_to_json(v)?);
                }
                Ok(serde_json::Value::Array(list))
            }
        },
    }
}

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

impl From<segment::data_types::text_index::TokenizerType> for TokenizerType {
    fn from(tokenizer_type: segment::data_types::text_index::TokenizerType) -> Self {
        match tokenizer_type {
            segment::data_types::text_index::TokenizerType::Prefix => TokenizerType::Prefix,
            segment::data_types::text_index::TokenizerType::Whitespace => TokenizerType::Whitespace,
            segment::data_types::text_index::TokenizerType::Multilingual => {
                TokenizerType::Multilingual
            }
            segment::data_types::text_index::TokenizerType::Word => TokenizerType::Word,
        }
    }
}

impl From<segment::data_types::text_index::TextIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::text_index::TextIndexParams) -> Self {
        let tokenizer = TokenizerType::from(params.tokenizer);
        PayloadIndexParams {
            index_params: Some(IndexParams::TextIndexParams(TextIndexParams {
                tokenizer: tokenizer as i32,
                lowercase: params.lowercase,
                min_token_len: params.min_token_len.map(|x| x as u64),
                max_token_len: params.max_token_len.map(|x| x as u64),
            })),
        }
    }
}

impl From<segment::data_types::integer_index::IntegerParams> for PayloadIndexParams {
    fn from(params: segment::data_types::integer_index::IntegerParams) -> Self {
        PayloadIndexParams {
            index_params: Some(IndexParams::IntegerParams(IntegerParams {
                lookup: params.lookup,
                range: params.range,
            })),
        }
    }
}

impl From<segment::types::PayloadIndexInfo> for PayloadSchemaInfo {
    fn from(schema: segment::types::PayloadIndexInfo) -> Self {
        PayloadSchemaInfo {
            data_type: match schema.data_type {
                segment::types::PayloadSchemaType::Keyword => PayloadSchemaType::Keyword,
                segment::types::PayloadSchemaType::Integer => PayloadSchemaType::Integer,
                segment::types::PayloadSchemaType::Float => PayloadSchemaType::Float,
                segment::types::PayloadSchemaType::Geo => PayloadSchemaType::Geo,
                segment::types::PayloadSchemaType::Text => PayloadSchemaType::Text,
                segment::types::PayloadSchemaType::Bool => PayloadSchemaType::Bool,
            }
            .into(),
            params: schema.params.map(|params| match params {
                segment::types::PayloadSchemaParams::Text(text_index_params) => {
                    text_index_params.into()
                }
                segment::types::PayloadSchemaParams::Integer(integer_params) => {
                    integer_params.into()
                }
            }),
            points: Some(schema.points as u64),
        }
    }
}

impl TryFrom<TokenizerType> for segment::data_types::text_index::TokenizerType {
    type Error = Status;
    fn try_from(tokenizer_type: TokenizerType) -> Result<Self, Self::Error> {
        match tokenizer_type {
            TokenizerType::Unknown => Err(Status::invalid_argument("unknown tokenizer type")),
            TokenizerType::Prefix => Ok(segment::data_types::text_index::TokenizerType::Prefix),
            TokenizerType::Multilingual => {
                Ok(segment::data_types::text_index::TokenizerType::Multilingual)
            }
            TokenizerType::Whitespace => {
                Ok(segment::data_types::text_index::TokenizerType::Whitespace)
            }
            TokenizerType::Word => Ok(segment::data_types::text_index::TokenizerType::Word),
        }
    }
}

impl TryFrom<TextIndexParams> for segment::data_types::text_index::TextIndexParams {
    type Error = Status;
    fn try_from(params: TextIndexParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::text_index::TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::from_i32(params.tokenizer)
                .map(|x| x.try_into())
                .unwrap_or_else(|| Err(Status::invalid_argument("unknown tokenizer type")))?,
            lowercase: params.lowercase,
            min_token_len: params.min_token_len.map(|x| x as usize),
            max_token_len: params.max_token_len.map(|x| x as usize),
        })
    }
}

impl TryFrom<IntegerParams> for segment::data_types::integer_index::IntegerParams {
    type Error = Status;
    fn try_from(params: IntegerParams) -> Result<Self, Self::Error> {
        Ok(segment::data_types::integer_index::IntegerParams {
            r#type: IntegerIndexType::Integer,
            lookup: params.lookup,
            range: params.range,
        })
    }
}

impl TryFrom<IndexParams> for segment::types::PayloadSchemaParams {
    type Error = Status;

    fn try_from(value: IndexParams) -> Result<Self, Self::Error> {
        match value {
            IndexParams::TextIndexParams(text_index_params) => Ok(
                segment::types::PayloadSchemaParams::Text(text_index_params.try_into()?),
            ),
            IndexParams::IntegerParams(integer_params) => Ok(
                segment::types::PayloadSchemaParams::Integer(integer_params.try_into()?),
            ),
        }
    }
}

impl TryFrom<PayloadSchemaInfo> for segment::types::PayloadIndexInfo {
    type Error = Status;

    fn try_from(schema: PayloadSchemaInfo) -> Result<Self, Self::Error> {
        let data_type = match PayloadSchemaType::from_i32(schema.data_type) {
            None => {
                return Err(Status::invalid_argument(
                    "Malformed payload schema".to_string(),
                ))
            }
            Some(data_type) => match data_type {
                PayloadSchemaType::Keyword => segment::types::PayloadSchemaType::Keyword,
                PayloadSchemaType::Integer => segment::types::PayloadSchemaType::Integer,
                PayloadSchemaType::Float => segment::types::PayloadSchemaType::Float,
                PayloadSchemaType::Geo => segment::types::PayloadSchemaType::Geo,
                PayloadSchemaType::Text => segment::types::PayloadSchemaType::Text,
                PayloadSchemaType::Bool => segment::types::PayloadSchemaType::Bool,
                PayloadSchemaType::UnknownType => {
                    return Err(Status::invalid_argument(
                        "Malformed payload schema".to_string(),
                    ))
                }
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
                SelectorOptions::Exclude(s) => {
                    segment::types::PayloadSelectorExclude::new(s.fields).into()
                }
                SelectorOptions::Include(s) => {
                    segment::types::PayloadSelectorInclude::new(s.fields).into()
                }
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
                SelectorOptions::Include(PayloadIncludeSelector { fields })
            }
            segment::types::WithPayloadInterface::Selector(selector) => match selector {
                segment::types::PayloadSelector::Include(s) => {
                    SelectorOptions::Include(PayloadIncludeSelector { fields: s.include })
                }
                segment::types::PayloadSelector::Exclude(s) => {
                    SelectorOptions::Exclude(PayloadExcludeSelector { fields: s.exclude })
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

impl From<segment::data_types::vectors::Vector> for Vector {
    fn from(vector: segment::data_types::vectors::Vector) -> Self {
        match vector {
            segment::data_types::vectors::Vector::Dense(vector) => Self {
                data: vector,
                indices: None,
            },
            segment::data_types::vectors::Vector::Sparse(vector) => Self {
                data: vector.values,
                indices: Some(SparseIndices {
                    data: vector.indices,
                }),
            },
        }
    }
}

impl From<Vector> for segment::data_types::vectors::Vector {
    fn from(vector: Vector) -> Self {
        match vector.indices {
            None => segment::data_types::vectors::Vector::Dense(vector.data),
            Some(indices) => segment::data_types::vectors::Vector::Sparse(
                sparse::common::sparse_vector::SparseVector {
                    values: vector.data,
                    indices: indices.data,
                },
            ),
        }
    }
}

impl From<HashMap<String, segment::data_types::vectors::Vector>> for NamedVectors {
    fn from(vectors: HashMap<String, segment::data_types::vectors::Vector>) -> Self {
        Self {
            vectors: vectors
                .into_iter()
                .map(|(name, vector)| (name, vector.into()))
                .collect(),
        }
    }
}

impl From<segment::data_types::vectors::VectorStruct> for Vectors {
    fn from(vector_struct: segment::data_types::vectors::VectorStruct) -> Self {
        match vector_struct {
            segment::data_types::vectors::VectorStruct::Single(vector) => {
                let vector: segment::data_types::vectors::Vector = vector.into();
                Self {
                    vectors_options: Some(VectorsOptions::Vector(vector.into())),
                }
            }
            segment::data_types::vectors::VectorStruct::Multi(vectors) => Self {
                vectors_options: Some(VectorsOptions::Vectors(NamedVectors {
                    vectors: HashMap::from_iter(
                        vectors
                            .iter()
                            .map(|(name, vector)| (name.clone(), vector.clone().into())),
                    ),
                })),
            },
        }
    }
}

impl From<segment::types::ScoredPoint> for ScoredPoint {
    fn from(point: segment::types::ScoredPoint) -> Self {
        Self {
            id: Some(point.id.into()),
            payload: point.payload.map(payload_to_proto).unwrap_or_default(),
            score: point.score,
            version: point.version,
            vectors: point.vector.map(|v| v.into()),
            shard_key: point.shard_key.map(convert_shard_key_to_grpc),
        }
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

impl From<NamedVectors> for HashMap<String, segment::data_types::vectors::Vector> {
    fn from(vectors: NamedVectors) -> Self {
        vectors
            .vectors
            .into_iter()
            .map(|(name, vector)| (name, segment::data_types::vectors::Vector::from(vector)))
            .collect()
    }
}

impl TryFrom<Vectors> for segment::data_types::vectors::VectorStruct {
    type Error = Status;

    fn try_from(vectors: Vectors) -> Result<Self, Self::Error> {
        match vectors.vectors_options {
            Some(vectors_options) => Ok(match vectors_options {
                VectorsOptions::Vector(vector) => {
                    segment::data_types::vectors::VectorStruct::Single(vector.data)
                }
                VectorsOptions::Vectors(vectors) => {
                    segment::data_types::vectors::VectorStruct::Multi(vectors.into())
                }
            }),
            None => Err(Status::invalid_argument("No Provided")),
        }
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
                r#type: match QuantizationType::from_i32(value.r#type) {
                    Some(QuantizationType::Int8) => segment::types::ScalarType::Int8,
                    Some(QuantizationType::UnknownQuantization) | None => {
                        return Err(Status::invalid_argument("Unknown quantization type"))
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
                compression: match CompressionRatio::from_i32(value.compression) {
                    None => {
                        return Err(Status::invalid_argument(
                            "Unknown compression ratio".to_string(),
                        ))
                    }
                    Some(CompressionRatio::X4) => segment::types::CompressionRatio::X4,
                    Some(CompressionRatio::X8) => segment::types::CompressionRatio::X8,
                    Some(CompressionRatio::X16) => segment::types::CompressionRatio::X16,
                    Some(CompressionRatio::X32) => segment::types::CompressionRatio::X32,
                    Some(CompressionRatio::X64) => segment::types::CompressionRatio::X64,
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
                conditions.into_iter().map(|c| c.into()).collect()
            }
        }
    }
}

impl TryFrom<Filter> for segment::types::Filter {
    type Error = Status;

    fn try_from(value: Filter) -> Result<Self, Self::Error> {
        Ok(Self {
            should: conditions_helper_from_grpc(value.should)?,
            must: conditions_helper_from_grpc(value.must)?,
            must_not: conditions_helper_from_grpc(value.must_not)?,
        })
    }
}

impl From<segment::types::Filter> for Filter {
    fn from(value: segment::types::Filter) -> Self {
        Self {
            should: conditions_helper_to_grpc(value.should),
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
                    Ok(segment::types::Condition::IsEmpty(is_empty.into()))
                }
                ConditionOneOf::IsNull(is_null) => {
                    Ok(segment::types::Condition::IsNull(is_null.into()))
                }
                ConditionOneOf::Nested(nested) => Ok(segment::types::Condition::Nested(
                    segment::types::NestedCondition::new(nested.try_into()?),
                )),
            };
        }
        Err(Status::invalid_argument("Malformed Condition type"))
    }
}

impl From<segment::types::Condition> for Condition {
    fn from(value: segment::types::Condition) -> Self {
        let condition_one_of = match value {
            segment::types::Condition::Field(field) => ConditionOneOf::Field(field.into()),
            segment::types::Condition::IsEmpty(is_empty) => {
                ConditionOneOf::IsEmpty(is_empty.into())
            }
            segment::types::Condition::IsNull(is_null) => ConditionOneOf::IsNull(is_null.into()),
            segment::types::Condition::HasId(has_id) => ConditionOneOf::HasId(has_id.into()),
            segment::types::Condition::Filter(filter) => ConditionOneOf::Filter(filter.into()),
            segment::types::Condition::Nested(nested) => {
                ConditionOneOf::Nested(nested.nested.into())
            }
        };

        Self {
            condition_one_of: Some(condition_one_of),
        }
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
                key: value.key,
                filter: filter.try_into()?,
            }),
        }
    }
}

impl From<segment::types::Nested> for NestedCondition {
    fn from(value: segment::types::Nested) -> Self {
        Self {
            key: value.key,
            filter: Some(value.filter.into()),
        }
    }
}

impl From<IsEmptyCondition> for segment::types::IsEmptyCondition {
    fn from(value: IsEmptyCondition) -> Self {
        segment::types::IsEmptyCondition {
            is_empty: segment::types::PayloadField { key: value.key },
        }
    }
}

impl From<segment::types::IsEmptyCondition> for IsEmptyCondition {
    fn from(value: segment::types::IsEmptyCondition) -> Self {
        Self {
            key: value.is_empty.key,
        }
    }
}

impl From<IsNullCondition> for segment::types::IsNullCondition {
    fn from(value: IsNullCondition) -> Self {
        segment::types::IsNullCondition {
            is_null: segment::types::PayloadField { key: value.key },
        }
    }
}

impl From<segment::types::IsNullCondition> for IsNullCondition {
    fn from(value: segment::types::IsNullCondition) -> Self {
        Self {
            key: value.is_null.key,
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
        } = value;

        let geo_bounding_box =
            geo_bounding_box.map_or_else(|| Ok(None), |g| g.try_into().map(Some))?;
        let geo_radius = geo_radius.map_or_else(|| Ok(None), |g| g.try_into().map(Some))?;
        let geo_polygon = geo_polygon.map_or_else(|| Ok(None), |g| g.try_into().map(Some))?;
        Ok(Self {
            key,
            r#match: r#match.map_or_else(|| Ok(None), |m| m.try_into().map(Some))?,
            range: range.map(Into::into),
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

        let geo_bounding_box = geo_bounding_box.map(Into::into);
        let geo_radius = geo_radius.map(Into::into);
        let geo_polygon = geo_polygon.map(Into::into);
        Self {
            key,
            r#match: r#match.map(Into::into),
            range: range.map(Into::into),
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count: values_count.map(Into::into),
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
            _ => Err(Status::invalid_argument("Malformed GeoPolygon type")),
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

impl From<Range> for segment::types::Range {
    fn from(value: Range) -> Self {
        Self {
            lt: value.lt,
            gt: value.gt,
            gte: value.gte,
            lte: value.lte,
        }
    }
}

impl From<segment::types::Range> for Range {
    fn from(value: segment::types::Range) -> Self {
        Self {
            lt: value.lt,
            gt: value.gt,
            gte: value.gte,
            lte: value.lte,
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
                segment::types::ValueVariants::Keyword(kw) => MatchValue::Keyword(kw),
                segment::types::ValueVariants::Integer(int) => MatchValue::Integer(int),
                segment::types::ValueVariants::Bool(flag) => MatchValue::Boolean(flag),
            },
            segment::types::Match::Text(segment::types::MatchText { text }) => {
                MatchValue::Text(text)
            }
            segment::types::Match::Any(any) => match any.any {
                segment::types::AnyVariants::Keywords(strings) => {
                    MatchValue::Keywords(RepeatedStrings { strings })
                }
                segment::types::AnyVariants::Integers(integers) => {
                    MatchValue::Integers(RepeatedIntegers { integers })
                }
            },
            segment::types::Match::Except(except) => match except.except {
                segment::types::AnyVariants::Keywords(strings) => {
                    MatchValue::ExceptKeywords(RepeatedStrings { strings })
                }
                segment::types::AnyVariants::Integers(integers) => {
                    MatchValue::ExceptIntegers(RepeatedIntegers { integers })
                }
            },
        };
        Self {
            match_value: Some(match_value),
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

pub fn date_time_to_proto(date_time: NaiveDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: date_time.timestamp(), // number of non-leap seconds since the midnight on January 1, 1970.
        nanos: date_time.nanosecond() as i32,
    }
}

impl TryFrom<Distance> for segment::types::Distance {
    type Error = Status;

    fn try_from(value: Distance) -> Result<Self, Self::Error> {
        Ok(match value {
            Distance::UnknownDistance => {
                return Err(Status::invalid_argument(
                    "Malformed distance parameter: UnknownDistance",
                ))
            }
            Distance::Cosine => segment::types::Distance::Cosine,
            Distance::Euclid => segment::types::Distance::Euclid,
            Distance::Dot => segment::types::Distance::Dot,
            Distance::Manhattan => segment::types::Distance::Manhattan,
        })
    }
}

pub fn from_grpc_dist(dist: i32) -> Result<segment::types::Distance, Status> {
    match Distance::from_i32(dist) {
        None => Err(Status::invalid_argument(format!(
            "Malformed distance parameter, unexpected value: {dist}"
        ))),
        Some(grpc_distance) => Ok(grpc_distance.try_into()?),
    }
}

pub fn into_named_vector_struct(
    vector_name: Option<String>,
    vector: Vec<VectorElementType>,
    indices: Option<SparseIndices>,
) -> Result<segment::data_types::vectors::NamedVectorStruct, Status> {
    use segment::data_types::vectors::{NamedSparseVector, NamedVector, NamedVectorStruct};
    use sparse::common::sparse_vector::SparseVector;
    Ok(match indices {
        Some(indices) => NamedVectorStruct::Sparse(NamedSparseVector {
            name: vector_name
                .ok_or_else(|| Status::invalid_argument("Sparse vector must have a name"))?,
            vector: SparseVector {
                values: vector,
                indices: indices.data,
            },
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
