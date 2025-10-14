use std::collections::{BTreeMap, HashMap};
use std::str::FromStr as _;
use std::time::Instant;

use ahash::AHashSet;
use chrono::{NaiveDateTime, Timelike};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_data::HardwareData;
use common::types::ScoreType;
use itertools::Itertools;
use segment::common::operation_error::OperationError;
use segment::data_types::index::{
    BoolIndexType, DatetimeIndexType, FloatIndexType, GeoIndexType, IntegerIndexType,
    KeywordIndexType, SnowballLanguage, TextIndexType, UuidIndexType,
};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, NamedMultiDenseVector, VectorInternal};
use segment::data_types::{facets as segment_facets, vectors as segment_vectors};
use segment::index::query_optimization::rescore_formula::parsed_formula::{
    DatetimeExpression, DecayKind, ParsedExpression, ParsedFormula,
};
use segment::types::{DateTimePayloadType, FloatPayloadType, default_quantization_ignore_value};
use segment::vector_storage::query as segment_query;
use sparse::common::sparse_vector::validate_sparse_vector_impl;
use tonic::Status;
use uuid::Uuid;

use super::qdrant::{
    BinaryQuantization, BoolIndexParams, CompressionRatio, DatetimeIndexParams, DatetimeRange,
    Direction, FacetHit, FacetHitInternal, FacetValue, FacetValueInternal, FieldType,
    FloatIndexParams, GeoIndexParams, GeoLineString, GroupId, HardwareUsage, HasVectorCondition,
    KeywordIndexParams, LookupLocation, MaxOptimizationThreads, MultiVectorComparator,
    MultiVectorConfig, OrderBy, OrderValue, Range, RawVector, RecommendStrategy, RetrievedPoint,
    SearchMatrixPair, SearchPointGroups, SearchPoints, ShardKeySelector, StartFrom,
    StrictModeMultivector, StrictModeMultivectorConfig, StrictModeSparse, StrictModeSparseConfig,
    UuidIndexParams, VectorsOutput, WithLookup, raw_query, start_from,
};
use super::stemming_algorithm::StemmingParams;
use super::{Expression, Formula, RecoQuery, SnowballParams, StemmingAlgorithm, Usage};
use crate::conversions::json::{self, json_to_proto};
use crate::grpc::qdrant::condition::ConditionOneOf;
use crate::grpc::qdrant::r#match::MatchValue;
use crate::grpc::qdrant::payload_index_params::IndexParams;
use crate::grpc::qdrant::point_id::PointIdOptions;
use crate::grpc::qdrant::with_payload_selector::SelectorOptions;
use crate::grpc::qdrant::{
    CollectionDescription, CollectionOperationResponse, Condition, Distance, FieldCondition,
    Filter, GeoBoundingBox, GeoPoint, GeoPolygon, GeoRadius, HasIdCondition, HealthCheckReply,
    HnswConfigDiff, IntegerIndexParams, IsEmptyCondition, IsNullCondition, ListCollectionsResponse,
    Match, MinShould, NamedVectors, NestedCondition, PayloadExcludeSelector,
    PayloadIncludeSelector, PayloadIndexParams, PayloadSchemaInfo, PayloadSchemaType, PointId,
    PointStruct, PointsOperationResponse, PointsOperationResponseInternal, ProductQuantization,
    QuantizationConfig, QuantizationSearchParams, QuantizationType, RepeatedIntegers,
    RepeatedStrings, ScalarQuantization, ScoredPoint, SearchParams, ShardKey, StopwordsSet,
    StrictModeConfig, TextIndexParams, TokenizerType, UpdateResult, UpdateResultInternal,
    ValuesCount, VectorsSelector, WithPayloadSelector, WithVectorsSelector, shard_key,
    with_vectors_selector,
};
use crate::grpc::{
    self, BinaryQuantizationEncoding, BinaryQuantizationQueryEncoding, DecayParamsExpression,
    DivExpression, GeoDistance, MultExpression, PowExpression, SumExpression,
};
use crate::rest::models::{CollectionsResponse, VersionInfo};
use crate::rest::schema as rest;

pub fn convert_shard_key_to_grpc(value: segment::types::ShardKey) -> ShardKey {
    match value {
        segment::types::ShardKey::Keyword(keyword) => ShardKey {
            key: Some(shard_key::Key::Keyword(keyword.to_string())),
        },
        segment::types::ShardKey::Number(number) => ShardKey {
            key: Some(shard_key::Key::Number(number)),
        },
    }
}

pub fn convert_shard_key_from_grpc(value: ShardKey) -> Option<segment::types::ShardKey> {
    let ShardKey { key } = value;
    key.map(|key| match key {
        shard_key::Key::Keyword(keyword) => segment::types::ShardKey::from(keyword),
        shard_key::Key::Number(number) => segment::types::ShardKey::Number(number),
    })
}

pub fn convert_shard_key_from_grpc_opt(
    value: Option<ShardKey>,
) -> Option<segment::types::ShardKey> {
    value.and_then(|value| value.key).map(|key| match key {
        shard_key::Key::Keyword(keyword) => segment::types::ShardKey::from(keyword),
        shard_key::Key::Number(number) => segment::types::ShardKey::Number(number),
    })
}
impl From<ShardKeySelector> for rest::ShardKeySelector {
    fn from(value: ShardKeySelector) -> Self {
        let ShardKeySelector { shard_keys } = value;
        let shard_keys: Vec<_> = shard_keys
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
        let VersionInfo {
            title,
            version,
            commit,
        } = info;
        HealthCheckReply {
            title,
            version,
            commit,
        }
    }
}

impl From<(Instant, CollectionsResponse)> for ListCollectionsResponse {
    fn from(value: (Instant, CollectionsResponse)) -> Self {
        let (timing, response) = value;
        let CollectionsResponse { collections } = response;
        let collections = collections
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
        let segment::data_types::index::KeywordIndexParams {
            r#type: _,
            is_tenant,
            on_disk,
        } = params;
        PayloadIndexParams {
            index_params: Some(IndexParams::KeywordIndexParams(KeywordIndexParams {
                is_tenant,
                on_disk,
            })),
        }
    }
}

impl From<segment::data_types::index::IntegerIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::IntegerIndexParams) -> Self {
        let segment::data_types::index::IntegerIndexParams {
            r#type: _,
            lookup,
            range,
            on_disk,
            is_principal,
        } = params;
        PayloadIndexParams {
            index_params: Some(IndexParams::IntegerIndexParams(IntegerIndexParams {
                lookup,
                range,
                is_principal,
                on_disk,
            })),
        }
    }
}

impl From<segment::data_types::index::FloatIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::FloatIndexParams) -> Self {
        let segment::data_types::index::FloatIndexParams {
            r#type: _,
            on_disk,
            is_principal,
        } = params;
        PayloadIndexParams {
            index_params: Some(IndexParams::FloatIndexParams(FloatIndexParams {
                on_disk,
                is_principal,
            })),
        }
    }
}

impl From<segment::data_types::index::GeoIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::GeoIndexParams) -> Self {
        let segment::data_types::index::GeoIndexParams { r#type: _, on_disk } = params;
        PayloadIndexParams {
            index_params: Some(IndexParams::GeoIndexParams(GeoIndexParams { on_disk })),
        }
    }
}

impl From<segment::data_types::index::TextIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::TextIndexParams) -> Self {
        let segment::data_types::index::TextIndexParams {
            r#type: _,
            tokenizer,
            min_token_len,
            max_token_len,
            lowercase,
            ascii_folding,
            phrase_matching,
            on_disk,
            stopwords,
            stemmer,
        } = params;
        let tokenizer = TokenizerType::from(tokenizer);

        // Convert stopwords if present
        let stopwords_set = stopwords.map(StopwordsSet::from);

        let stemming_algo = stemmer.map(StemmingAlgorithm::from);

        PayloadIndexParams {
            index_params: Some(IndexParams::TextIndexParams(TextIndexParams {
                tokenizer: tokenizer as i32,
                lowercase,
                ascii_folding,
                min_token_len: min_token_len.map(|x| x as u64),
                max_token_len: max_token_len.map(|x| x as u64),
                phrase_matching,
                on_disk,
                stopwords: stopwords_set,
                stemmer: stemming_algo,
            })),
        }
    }
}

impl From<segment::data_types::index::BoolIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::BoolIndexParams) -> Self {
        let segment::data_types::index::BoolIndexParams { r#type: _, on_disk } = params;
        PayloadIndexParams {
            index_params: Some(IndexParams::BoolIndexParams(BoolIndexParams { on_disk })),
        }
    }
}

impl From<segment::data_types::index::UuidIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::UuidIndexParams) -> Self {
        let segment::data_types::index::UuidIndexParams {
            r#type: _,
            is_tenant,
            on_disk,
        } = params;
        PayloadIndexParams {
            index_params: Some(IndexParams::UuidIndexParams(UuidIndexParams {
                is_tenant,
                on_disk,
            })),
        }
    }
}

impl From<segment::data_types::index::DatetimeIndexParams> for PayloadIndexParams {
    fn from(params: segment::data_types::index::DatetimeIndexParams) -> Self {
        let segment::data_types::index::DatetimeIndexParams {
            r#type: _,
            on_disk,
            is_principal,
        } = params;
        PayloadIndexParams {
            index_params: Some(IndexParams::DatetimeIndexParams(DatetimeIndexParams {
                on_disk,
                is_principal,
            })),
        }
    }
}

impl From<segment::types::PayloadIndexInfo> for PayloadSchemaInfo {
    fn from(schema: segment::types::PayloadIndexInfo) -> Self {
        let segment::types::PayloadIndexInfo {
            data_type,
            params,
            points,
        } = schema;
        PayloadSchemaInfo {
            data_type: PayloadSchemaType::from(data_type) as i32,
            params: params.map(|p| p.into()),
            points: Some(points as u64),
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

impl From<segment::data_types::index::StopwordsInterface> for StopwordsSet {
    fn from(stopwords: segment::data_types::index::StopwordsInterface) -> Self {
        match stopwords {
            segment::data_types::index::StopwordsInterface::Language(lang) => {
                let lang_str = lang.to_string();

                StopwordsSet {
                    languages: vec![lang_str],
                    custom: vec![],
                }
            }
            segment::data_types::index::StopwordsInterface::Set(set) => {
                let languages = if let Some(languages) = set.languages {
                    languages.iter().map(|lang| lang.to_string()).collect()
                } else {
                    vec![]
                };

                let custom = if let Some(custom) = set.custom {
                    custom.into_iter().collect()
                } else {
                    vec![]
                };

                StopwordsSet { languages, custom }
            }
        }
    }
}

impl From<segment::data_types::index::StemmingAlgorithm> for StemmingAlgorithm {
    fn from(value: segment::data_types::index::StemmingAlgorithm) -> Self {
        let stemming_params = match value {
            segment::data_types::index::StemmingAlgorithm::Snowball(snowball_params) => {
                let segment::data_types::index::SnowballParams {
                    r#type: _,
                    language,
                } = snowball_params;
                let language = language.to_string();
                StemmingParams::Snowball(SnowballParams { language })
            }
        };

        StemmingAlgorithm {
            stemming_params: Some(stemming_params),
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
        let KeywordIndexParams { is_tenant, on_disk } = params;
        Ok(segment::data_types::index::KeywordIndexParams {
            r#type: KeywordIndexType::Keyword,
            is_tenant,
            on_disk,
        })
    }
}

impl TryFrom<IntegerIndexParams> for segment::data_types::index::IntegerIndexParams {
    type Error = Status;
    fn try_from(params: IntegerIndexParams) -> Result<Self, Self::Error> {
        let IntegerIndexParams {
            lookup,
            range,
            is_principal,
            on_disk,
        } = params;
        Ok(segment::data_types::index::IntegerIndexParams {
            r#type: IntegerIndexType::Integer,
            lookup,
            range,
            is_principal,
            on_disk,
        })
    }
}

impl TryFrom<FloatIndexParams> for segment::data_types::index::FloatIndexParams {
    type Error = Status;
    fn try_from(params: FloatIndexParams) -> Result<Self, Self::Error> {
        let FloatIndexParams {
            on_disk,
            is_principal,
        } = params;
        Ok(segment::data_types::index::FloatIndexParams {
            r#type: FloatIndexType::Float,
            on_disk,
            is_principal,
        })
    }
}

impl TryFrom<GeoIndexParams> for segment::data_types::index::GeoIndexParams {
    type Error = Status;
    fn try_from(params: GeoIndexParams) -> Result<Self, Self::Error> {
        let GeoIndexParams { on_disk } = params;
        Ok(segment::data_types::index::GeoIndexParams {
            r#type: GeoIndexType::Geo,
            on_disk,
        })
    }
}

impl TryFrom<StopwordsSet> for segment::data_types::index::StopwordsInterface {
    type Error = Status;

    fn try_from(value: StopwordsSet) -> Result<Self, Self::Error> {
        let StopwordsSet { languages, custom } = value;

        let result_languages = if languages.is_empty() {
            None
        } else {
            Some(
                languages
                    .into_iter()
                    .map(|lang| segment::data_types::index::Language::from_str(&lang))
                    .collect::<Result<_, _>>()
                    .map_err(|e| Status::invalid_argument(format!("unknown language: {e}")))?,
            )
        };

        let result_custom = if custom.is_empty() {
            None
        } else {
            Some(custom.into_iter().map(|word| word.to_lowercase()).collect())
        };

        Ok(segment::data_types::index::StopwordsInterface::Set(
            segment::data_types::index::StopwordsSet {
                languages: result_languages,
                custom: result_custom,
            },
        ))
    }
}

impl TryFrom<TextIndexParams> for segment::data_types::index::TextIndexParams {
    type Error = Status;
    fn try_from(params: TextIndexParams) -> Result<Self, Self::Error> {
        let TextIndexParams {
            tokenizer,
            lowercase,
            ascii_folding,
            min_token_len,
            max_token_len,
            phrase_matching,
            on_disk,
            stopwords,
            stemmer,
        } = params;

        // Convert stopwords if present
        let stopwords_converted = if let Some(set) = stopwords {
            Some(segment::data_types::index::StopwordsInterface::try_from(
                set,
            )?)
        } else {
            None
        };

        let stemmer = stemmer
            .and_then(|i| i.stemming_params)
            .map(segment::data_types::index::StemmingAlgorithm::try_from)
            .transpose()?;

        Ok(segment::data_types::index::TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::try_from(tokenizer)
                .map(|x| x.try_into())
                .unwrap_or_else(|_| Err(Status::invalid_argument("unknown tokenizer type")))?,
            lowercase,
            ascii_folding,
            min_token_len: min_token_len.map(|x| x as usize),
            max_token_len: max_token_len.map(|x| x as usize),
            phrase_matching,
            on_disk,
            stopwords: stopwords_converted,
            stemmer,
        })
    }
}

impl TryFrom<StemmingParams> for segment::data_types::index::StemmingAlgorithm {
    type Error = Status;

    fn try_from(value: StemmingParams) -> Result<Self, Self::Error> {
        match value {
            StemmingParams::Snowball(params) => {
                let language = SnowballLanguage::from_str(&params.language).map_err(|_| {
                    Status::invalid_argument(format!("Language {:?} not found.", params.language))
                })?;
                Ok(segment::data_types::index::StemmingAlgorithm::Snowball(
                    segment::data_types::index::SnowballParams {
                        r#type: segment::data_types::index::Snowball::Snowball,
                        language,
                    },
                ))
            }
        }
    }
}

impl TryFrom<BoolIndexParams> for segment::data_types::index::BoolIndexParams {
    type Error = Status;
    fn try_from(params: BoolIndexParams) -> Result<Self, Self::Error> {
        let BoolIndexParams { on_disk } = params;
        Ok(segment::data_types::index::BoolIndexParams {
            r#type: BoolIndexType::Bool,
            on_disk,
        })
    }
}

impl TryFrom<DatetimeIndexParams> for segment::data_types::index::DatetimeIndexParams {
    type Error = Status;
    fn try_from(params: DatetimeIndexParams) -> Result<Self, Self::Error> {
        let DatetimeIndexParams {
            on_disk,
            is_principal,
        } = params;
        Ok(segment::data_types::index::DatetimeIndexParams {
            r#type: DatetimeIndexType::Datetime,
            on_disk,
            is_principal,
        })
    }
}

impl TryFrom<UuidIndexParams> for segment::data_types::index::UuidIndexParams {
    type Error = Status;
    fn try_from(params: UuidIndexParams) -> Result<Self, Self::Error> {
        let UuidIndexParams { is_tenant, on_disk } = params;
        Ok(segment::data_types::index::UuidIndexParams {
            r#type: UuidIndexType::Uuid,
            is_tenant,
            on_disk,
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
        let PayloadSchemaInfo {
            data_type,
            params,
            points,
        } = schema;
        let data_type = match PayloadSchemaType::try_from(data_type) {
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
        let params = match params {
            None => None,
            Some(PayloadIndexParams { index_params: None }) => None,
            Some(PayloadIndexParams {
                index_params: Some(index_params),
            }) => Some(index_params.try_into()?),
        };

        Ok(segment::types::PayloadIndexInfo {
            data_type,
            params,
            points: points.unwrap_or(0) as usize,
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
        let segment::types::GeoPoint { lon, lat } = geo;
        Self { lon, lat }
    }
}

impl TryFrom<WithPayloadSelector> for segment::types::WithPayloadInterface {
    type Error = Status;

    fn try_from(value: WithPayloadSelector) -> Result<Self, Self::Error> {
        let WithPayloadSelector { selector_options } = value;
        match selector_options {
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
        let QuantizationSearchParams {
            ignore,
            rescore,
            oversampling,
        } = params;
        Self {
            ignore: ignore.unwrap_or(default_quantization_ignore_value()),
            rescore,
            oversampling,
        }
    }
}

impl From<segment::types::QuantizationSearchParams> for QuantizationSearchParams {
    fn from(params: segment::types::QuantizationSearchParams) -> Self {
        let segment::types::QuantizationSearchParams {
            ignore,
            rescore,
            oversampling,
        } = params;
        Self {
            ignore: Some(ignore),
            rescore,
            oversampling,
        }
    }
}

impl From<SearchParams> for segment::types::SearchParams {
    fn from(params: SearchParams) -> Self {
        let SearchParams {
            hnsw_ef,
            exact,
            quantization,
            indexed_only,
        } = params;
        Self {
            hnsw_ef: hnsw_ef.map(|x| x as usize),
            exact: exact.unwrap_or(false),
            quantization: quantization.map(|q| q.into()),
            indexed_only: indexed_only.unwrap_or(false),
        }
    }
}

impl From<segment::types::SearchParams> for SearchParams {
    fn from(params: segment::types::SearchParams) -> Self {
        let segment::types::SearchParams {
            hnsw_ef,
            exact,
            quantization,
            indexed_only,
        } = params;
        Self {
            hnsw_ef: hnsw_ef.map(|x| x as u64),
            exact: Some(exact),
            quantization: quantization.map(|q| q.into()),
            indexed_only: Some(indexed_only),
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
        let rest::Record {
            id,
            payload,
            vector,
            shard_key,
            order_value,
        } = record;
        let retrieved_point = Self {
            id: Some(PointId::from(id)),
            payload: payload.map(json::payload_to_proto).unwrap_or_default(),
            vectors: vector.map(VectorsOutput::try_from).transpose()?,
            shard_key: shard_key.map(convert_shard_key_to_grpc),
            order_value: order_value.map(From::from),
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

        let OrderValue { variant } = value;

        let variant = variant.ok_or_else(|| {
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
        let segment::types::ScoredPoint {
            id,
            version,
            score,
            payload,
            vector,
            shard_key,
            order_value,
        } = point;
        Self {
            id: Some(PointId::from(id)),
            payload: payload.map(json::payload_to_proto).unwrap_or_default(),
            score,
            version,
            vectors: vector.map(VectorsOutput::from),
            shard_key: shard_key.map(convert_shard_key_to_grpc),
            order_value: order_value.map(OrderValue::from),
        }
    }
}

impl TryFrom<rest::ScoredPoint> for ScoredPoint {
    type Error = OperationError;
    fn try_from(point: rest::ScoredPoint) -> Result<Self, Self::Error> {
        let rest::ScoredPoint {
            id,
            version,
            score,
            payload,
            vector,
            shard_key,
            order_value,
        } = point;
        Ok(Self {
            id: Some(PointId::from(id)),
            payload: payload.map(json::payload_to_proto).unwrap_or_default(),
            score,
            version,
            vectors: vector.map(VectorsOutput::try_from).transpose()?,
            shard_key: shard_key.map(convert_shard_key_to_grpc),
            order_value: order_value.map(OrderValue::from),
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
        let NamedVectors { vectors } = vectors;
        vectors
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
        let WithVectorsSelector { selector_options } = with_vectors_selector;
        match selector_options {
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
        let PointId { point_id_options } = value;
        match point_id_options {
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
        let segment::types::ScalarQuantization { scalar } = value;
        let config = scalar;
        ScalarQuantization {
            r#type: match config.r#type {
                segment::types::ScalarType::Int8 => QuantizationType::Int8 as i32,
            },
            quantile: config.quantile,
            always_ram: config.always_ram,
        }
    }
}

impl TryFrom<ScalarQuantization> for segment::types::ScalarQuantization {
    type Error = Status;

    fn try_from(value: ScalarQuantization) -> Result<Self, Self::Error> {
        let ScalarQuantization {
            r#type,
            quantile,
            always_ram,
        } = value;
        Ok(segment::types::ScalarQuantization {
            scalar: segment::types::ScalarQuantizationConfig {
                r#type: match QuantizationType::try_from(r#type).ok() {
                    Some(QuantizationType::Int8) => segment::types::ScalarType::Int8,
                    Some(QuantizationType::UnknownQuantization) | None => {
                        return Err(Status::invalid_argument("Unknown quantization type"));
                    }
                },
                quantile,
                always_ram,
            },
        })
    }
}

impl From<segment::types::ProductQuantization> for ProductQuantization {
    fn from(value: segment::types::ProductQuantization) -> Self {
        let segment::types::ProductQuantization { product } = value;
        let segment::types::ProductQuantizationConfig {
            compression,
            always_ram,
        } = product;
        ProductQuantization {
            compression: match compression {
                segment::types::CompressionRatio::X4 => CompressionRatio::X4 as i32,
                segment::types::CompressionRatio::X8 => CompressionRatio::X8 as i32,
                segment::types::CompressionRatio::X16 => CompressionRatio::X16 as i32,
                segment::types::CompressionRatio::X32 => CompressionRatio::X32 as i32,
                segment::types::CompressionRatio::X64 => CompressionRatio::X64 as i32,
            },
            always_ram,
        }
    }
}

impl TryFrom<ProductQuantization> for segment::types::ProductQuantization {
    type Error = Status;

    fn try_from(value: ProductQuantization) -> Result<Self, Self::Error> {
        let ProductQuantization {
            compression,
            always_ram,
        } = value;
        Ok(segment::types::ProductQuantization {
            product: segment::types::ProductQuantizationConfig {
                compression: match CompressionRatio::try_from(compression) {
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
                always_ram,
            },
        })
    }
}

impl From<segment::types::BinaryQuantizationEncoding> for BinaryQuantizationEncoding {
    fn from(value: segment::types::BinaryQuantizationEncoding) -> Self {
        match value {
            segment::types::BinaryQuantizationEncoding::OneBit => {
                BinaryQuantizationEncoding::OneBit
            }
            segment::types::BinaryQuantizationEncoding::TwoBits => {
                BinaryQuantizationEncoding::TwoBits
            }
            segment::types::BinaryQuantizationEncoding::OneAndHalfBits => {
                BinaryQuantizationEncoding::OneAndHalfBits
            }
        }
    }
}

impl From<BinaryQuantizationEncoding> for segment::types::BinaryQuantizationEncoding {
    fn from(value: BinaryQuantizationEncoding) -> Self {
        match value {
            BinaryQuantizationEncoding::OneBit => {
                segment::types::BinaryQuantizationEncoding::OneBit
            }
            BinaryQuantizationEncoding::TwoBits => {
                segment::types::BinaryQuantizationEncoding::TwoBits
            }
            BinaryQuantizationEncoding::OneAndHalfBits => {
                segment::types::BinaryQuantizationEncoding::OneAndHalfBits
            }
        }
    }
}

impl From<segment::types::BinaryQuantization> for BinaryQuantization {
    fn from(value: segment::types::BinaryQuantization) -> Self {
        let segment::types::BinaryQuantization { binary } = value;
        let segment::types::BinaryQuantizationConfig {
            always_ram,
            encoding,
            query_encoding,
        } = binary;
        BinaryQuantization {
            always_ram,
            encoding: encoding
                .map(|encoding| i32::from(BinaryQuantizationEncoding::from(encoding))),
            query_encoding: query_encoding.map(BinaryQuantizationQueryEncoding::from),
        }
    }
}

impl TryFrom<BinaryQuantization> for segment::types::BinaryQuantization {
    type Error = Status;

    fn try_from(value: BinaryQuantization) -> Result<Self, Self::Error> {
        let BinaryQuantization {
            always_ram,
            encoding,
            query_encoding,
        } = value;
        let encoding = encoding
            .map(BinaryQuantizationEncoding::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("Unknown binary quantization encoding"))?;
        Ok(segment::types::BinaryQuantization {
            binary: segment::types::BinaryQuantizationConfig {
                always_ram,
                encoding: encoding.map(segment::types::BinaryQuantizationEncoding::from),
                query_encoding: query_encoding
                    .map(segment::types::BinaryQuantizationQueryEncoding::try_from)
                    .transpose()
                    .map_err(|_| {
                        Status::invalid_argument("Unknown binary quantization query encoding")
                    })?,
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
        let QuantizationConfig { quantization } = value;
        let value = quantization
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

impl TryFrom<BinaryQuantizationQueryEncoding> for segment::types::BinaryQuantizationQueryEncoding {
    type Error = Status;

    fn try_from(value: BinaryQuantizationQueryEncoding) -> Result<Self, Self::Error> {
        use crate::grpc::qdrant::binary_quantization_query_encoding::{Setting, Variant};

        let BinaryQuantizationQueryEncoding { variant } = value;
        let variant = variant.ok_or_else(|| {
            Status::invalid_argument("Malformed `BinaryQuantizationQueryEncoding`")
        })?;

        let converted = match variant {
            Variant::Setting(setting_int) => {
                let setting = Setting::try_from(setting_int).map_err(|err| {
                    Status::invalid_argument(format!(
                        "Invalid `BinaryQuantizationQueryEncoding` setting: {err}"
                    ))
                })?;
                match setting {
                    Setting::Default => segment::types::BinaryQuantizationQueryEncoding::Default,
                    Setting::Binary => segment::types::BinaryQuantizationQueryEncoding::Binary,
                    Setting::Scalar4Bits => {
                        segment::types::BinaryQuantizationQueryEncoding::Scalar4Bits
                    }
                    Setting::Scalar8Bits => {
                        segment::types::BinaryQuantizationQueryEncoding::Scalar8Bits
                    }
                }
            }
        };
        Ok(converted)
    }
}

impl From<segment::types::BinaryQuantizationQueryEncoding> for BinaryQuantizationQueryEncoding {
    fn from(value: segment::types::BinaryQuantizationQueryEncoding) -> Self {
        use crate::grpc::qdrant::binary_quantization_query_encoding::{Setting, Variant};

        let variant = match value {
            segment::types::BinaryQuantizationQueryEncoding::Default => {
                Variant::Setting(Setting::Default.into())
            }
            segment::types::BinaryQuantizationQueryEncoding::Binary => {
                Variant::Setting(Setting::Binary.into())
            }
            segment::types::BinaryQuantizationQueryEncoding::Scalar4Bits => {
                Variant::Setting(Setting::Scalar4Bits.into())
            }
            segment::types::BinaryQuantizationQueryEncoding::Scalar8Bits => {
                Variant::Setting(Setting::Scalar8Bits.into())
            }
        };

        Self {
            variant: Some(variant),
        }
    }
}

impl From<segment::types::MultiVectorConfig> for MultiVectorConfig {
    fn from(value: segment::types::MultiVectorConfig) -> Self {
        let segment::types::MultiVectorConfig { comparator } = value;
        Self {
            comparator: MultiVectorComparator::from(comparator) as i32,
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
        let MultiVectorConfig { comparator } = value;
        let comparator = MultiVectorComparator::try_from(comparator)
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
    // Convert gRPC into internal conditions, filter out empty conditions
    // See: <https://github.com/qdrant/qdrant/pull/5690>
    let mut converted = Vec::with_capacity(conditions.len());
    for condition in conditions {
        if let Some(condition) = grpc_condition_into_condition(condition)? {
            converted.push(condition);
        }
    }

    if converted.is_empty() {
        Ok(None)
    } else {
        Ok(Some(converted))
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
        let Filter {
            should,
            min_should,
            must,
            must_not,
        } = value;
        Ok(Self {
            should: conditions_helper_from_grpc(should)?,
            min_should: {
                match min_should {
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
            must: conditions_helper_from_grpc(must)?,
            must_not: conditions_helper_from_grpc(must_not)?,
        })
    }
}

impl From<segment::types::Filter> for Filter {
    fn from(value: segment::types::Filter) -> Self {
        let segment::types::Filter {
            should,
            min_should,
            must,
            must_not,
        } = value;
        Self {
            should: conditions_helper_to_grpc(should),
            min_should: {
                if let Some(segment::types::MinShould {
                    conditions,
                    min_count,
                }) = min_should
                {
                    Some(MinShould {
                        conditions: conditions_helper_to_grpc(Some(conditions)),
                        min_count: min_count as u64,
                    })
                } else {
                    None
                }
            },
            must: conditions_helper_to_grpc(must),
            must_not: conditions_helper_to_grpc(must_not),
        }
    }
}

/// Convert a gRPC into an internal condition
///
/// Returns `Ok(None)` if the condition is empty.
pub fn grpc_condition_into_condition(
    value: Condition,
) -> Result<Option<segment::types::Condition>, Status> {
    let Some(condition) = value.condition_one_of else {
        return Ok(None);
    };

    let condition = match condition {
        ConditionOneOf::Field(field) => Some(segment::types::Condition::Field(field.try_into()?)),
        ConditionOneOf::HasId(has_id) => Some(segment::types::Condition::HasId(has_id.try_into()?)),
        ConditionOneOf::Filter(filter) => {
            Some(segment::types::Condition::Filter(filter.try_into()?))
        }
        ConditionOneOf::IsEmpty(is_empty) => {
            Some(segment::types::Condition::IsEmpty(is_empty.try_into()?))
        }
        ConditionOneOf::IsNull(is_null) => {
            Some(segment::types::Condition::IsNull(is_null.try_into()?))
        }
        ConditionOneOf::Nested(nested) => Some(segment::types::Condition::Nested(
            segment::types::NestedCondition::new(nested.try_into()?),
        )),
        ConditionOneOf::HasVector(has_vector) => Some(segment::types::Condition::HasVector(
            segment::types::HasVectorCondition {
                has_vector: has_vector.has_vector,
            },
        )),
    };

    Ok(condition)
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
        let NestedCondition { key, filter } = value;
        match filter {
            None => Err(Status::invalid_argument(
                "Nested condition must have a filter",
            )),
            Some(filter) => Ok(Self {
                key: json::json_path_from_proto(&key)?,
                filter: filter.try_into()?,
            }),
        }
    }
}

impl From<segment::types::Nested> for NestedCondition {
    fn from(value: segment::types::Nested) -> Self {
        let segment::types::Nested { key, filter } = value;
        Self {
            key: key.to_string(),
            filter: Some(filter.into()),
        }
    }
}

impl TryFrom<IsEmptyCondition> for segment::types::IsEmptyCondition {
    type Error = Status;

    fn try_from(value: IsEmptyCondition) -> Result<Self, Status> {
        let IsEmptyCondition { key } = value;
        Ok(segment::types::IsEmptyCondition {
            is_empty: segment::types::PayloadField {
                key: json::json_path_from_proto(&key)?,
            },
        })
    }
}

impl From<segment::types::IsEmptyCondition> for IsEmptyCondition {
    fn from(value: segment::types::IsEmptyCondition) -> Self {
        let segment::types::IsEmptyCondition { is_empty } = value;
        Self {
            key: is_empty.key.to_string(),
        }
    }
}

impl TryFrom<IsNullCondition> for segment::types::IsNullCondition {
    type Error = Status;

    fn try_from(value: IsNullCondition) -> Result<Self, Status> {
        let IsNullCondition { key } = value;
        Ok(segment::types::IsNullCondition {
            is_null: segment::types::PayloadField {
                key: json::json_path_from_proto(&key)?,
            },
        })
    }
}

impl From<segment::types::IsNullCondition> for IsNullCondition {
    fn from(value: segment::types::IsNullCondition) -> Self {
        let segment::types::IsNullCondition { is_null } = value;
        Self {
            key: is_null.key.to_string(),
        }
    }
}

impl TryFrom<HasIdCondition> for segment::types::HasIdCondition {
    type Error = Status;

    fn try_from(value: HasIdCondition) -> Result<Self, Self::Error> {
        let HasIdCondition { has_id } = value;
        let set: AHashSet<segment::types::PointIdType> = has_id
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?;
        Ok(Self::from(set))
    }
}

impl From<segment::types::HasIdCondition> for HasIdCondition {
    fn from(value: segment::types::HasIdCondition) -> Self {
        let segment::types::HasIdCondition { has_id } = value;
        let set: Vec<PointId> = has_id.into_inner().into_iter().map(PointId::from).collect();
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
            is_empty,
            is_null,
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
            is_empty,
            is_null,
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
            is_empty,
            is_null,
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
            is_empty,
            is_null,
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
        let segment::types::GeoBoundingBox {
            top_left,
            bottom_right,
        } = value;
        Self {
            top_left: Some(top_left.into()),
            bottom_right: Some(bottom_right.into()),
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
        let segment::types::GeoRadius { center, radius } = value;
        Self {
            center: Some(center.into()),
            radius: radius as f32, // TODO lossy ok?
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
        let segment::types::GeoPolygon {
            exterior,
            interiors,
        } = value;
        Self {
            exterior: Some(exterior.into()),
            interiors: interiors
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<GeoPoint> for segment::types::GeoPoint {
    fn from(value: GeoPoint) -> Self {
        let GeoPoint { lon, lat } = value;
        Self { lon, lat }
    }
}

impl From<GeoLineString> for segment::types::GeoLineString {
    fn from(value: GeoLineString) -> Self {
        let GeoLineString { points } = value;
        Self {
            points: points.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<segment::types::GeoLineString> for GeoLineString {
    fn from(value: segment::types::GeoLineString) -> Self {
        let segment::types::GeoLineString { points } = value;
        Self {
            points: points.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Range> for segment::types::Range<FloatPayloadType> {
    fn from(value: Range) -> Self {
        let Range { lt, gt, gte, lte } = value;
        Self { lt, gt, gte, lte }
    }
}

impl From<segment::types::Range<FloatPayloadType>> for Range {
    fn from(value: segment::types::Range<FloatPayloadType>) -> Self {
        let segment::types::Range { lt, gt, gte, lte } = value;
        Self { lt, gt, gte, lte }
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
        let DatetimeRange { lt, gt, gte, lte } = value;
        Ok(Self::DateTime(segment::types::Range {
            lt: lt.map(try_date_time_from_proto).transpose()?,
            gt: gt.map(try_date_time_from_proto).transpose()?,
            gte: gte.map(try_date_time_from_proto).transpose()?,
            lte: lte.map(try_date_time_from_proto).transpose()?,
        }))
    }
}

impl From<segment::types::Range<DateTimePayloadType>> for DatetimeRange {
    fn from(value: segment::types::Range<DateTimePayloadType>) -> Self {
        let segment::types::Range { lt, gt, gte, lte } = value;
        Self {
            lt: lt.map(date_time_to_proto),
            gt: gt.map(date_time_to_proto),
            gte: gte.map(date_time_to_proto),
            lte: lte.map(date_time_to_proto),
        }
    }
}

impl From<ValuesCount> for segment::types::ValuesCount {
    fn from(value: ValuesCount) -> Self {
        let ValuesCount { lt, gt, gte, lte } = value;
        Self {
            lt: lt.map(|x| x as usize),
            gt: gt.map(|x| x as usize),
            gte: gte.map(|x| x as usize),
            lte: lte.map(|x| x as usize),
        }
    }
}

impl From<segment::types::ValuesCount> for ValuesCount {
    fn from(value: segment::types::ValuesCount) -> Self {
        let segment::types::ValuesCount { lt, gt, gte, lte } = value;
        Self {
            lt: lt.map(|x| x as u64),
            gt: gt.map(|x| x as u64),
            gte: gte.map(|x| x as u64),
            lte: lte.map(|x| x as u64),
        }
    }
}

impl TryFrom<Match> for segment::types::Match {
    type Error = Status;

    fn try_from(value: Match) -> Result<Self, Self::Error> {
        let Match { match_value } = value;
        match match_value {
            Some(mv) => Ok(match mv {
                MatchValue::Keyword(kw) => kw.into(),
                MatchValue::Integer(int) => int.into(),
                MatchValue::Boolean(flag) => flag.into(),
                MatchValue::Text(text) => segment::types::Match::Text(text.into()),
                MatchValue::Phrase(phrase) => segment::types::Match::Phrase(phrase.into()),
                MatchValue::Keywords(kwds) => kwds.strings.into(),
                MatchValue::Integers(ints) => ints.integers.into(),
                MatchValue::ExceptIntegers(kwds) => {
                    segment::types::Match::Except(kwds.integers.into())
                }
                MatchValue::ExceptKeywords(ints) => {
                    segment::types::Match::Except(ints.strings.into())
                }
                MatchValue::TextAny(text_any) => {
                    segment::types::Match::TextAny(segment::types::MatchTextAny { text_any })
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
            segment::types::Match::Phrase(segment::types::MatchPhrase { phrase }) => {
                MatchValue::Phrase(phrase)
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
            segment::types::Match::TextAny(segment::types::MatchTextAny { text_any }) => {
                MatchValue::TextAny(text_any)
            }
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

        let OrderBy {
            key,
            direction,
            start_from,
        } = value;

        let direction = direction
            .and_then(|x|
                // XXX: Invalid values silently converted to None
                Direction::try_from(x).ok())
            .map(segment::data_types::order_by::Direction::from);

        let start_from = start_from
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
            key: json::json_path_from_proto(&key)?,
            direction,
            start_from,
        })
    }
}

impl From<segment::data_types::order_by::OrderBy> for OrderBy {
    fn from(value: segment::data_types::order_by::OrderBy) -> Self {
        let segment::data_types::order_by::OrderBy {
            key,
            direction,
            start_from,
        } = value;
        Self {
            key: key.to_string(),
            direction: direction.map(|d| Direction::from(d) as i32),
            start_from: start_from.map(|start_from| start_from.into()),
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

impl TryFrom<MaxOptimizationThreads> for rest::MaxOptimizationThreads {
    type Error = Status;

    fn try_from(value: MaxOptimizationThreads) -> Result<Self, Self::Error> {
        use crate::grpc::qdrant::max_optimization_threads::{Setting, Variant};

        let MaxOptimizationThreads { variant } = value;
        let variant =
            variant.ok_or_else(|| Status::invalid_argument("Malformed MaxOptimizationThreads"))?;

        let converted = match variant {
            Variant::Setting(setting_int) => {
                let setting = Setting::try_from(setting_int).map_err(|err| {
                    Status::invalid_argument(format!(
                        "Invalid MaxOptimizationThreads setting: {err}"
                    ))
                })?;

                match setting {
                    Setting::Auto => Self::Setting(rest::MaxOptimizationThreadsSetting::Auto),
                }
            }
            Variant::Value(num_threads) => Self::Threads(num_threads as usize),
        };
        Ok(converted)
    }
}

impl TryFrom<MaxOptimizationThreads> for Option<usize> {
    type Error = Status;

    fn try_from(value: MaxOptimizationThreads) -> Result<Self, Self::Error> {
        use crate::grpc::qdrant::max_optimization_threads::{Setting, Variant};

        let MaxOptimizationThreads { variant } = value;

        let variant =
            variant.ok_or_else(|| Status::invalid_argument("Malformed MaxOptimizationThreads"))?;

        Ok(match variant {
            Variant::Setting(setting_int) => {
                let setting = Setting::try_from(setting_int).map_err(|err| {
                    Status::invalid_argument(format!(
                        "Invalid MaxOptimizationThreads setting: {err}"
                    ))
                })?;

                match setting {
                    Setting::Auto => None,
                }
            }
            Variant::Value(num_threads) => Some(num_threads as usize),
        })
    }
}

impl From<Option<usize>> for MaxOptimizationThreads {
    fn from(value: Option<usize>) -> Self {
        use crate::grpc::qdrant::max_optimization_threads::{Setting, Variant};

        let variant = match value {
            None => Variant::Setting(Setting::Auto.into()),
            Some(n) => Variant::Value(n as u64),
        };

        Self {
            variant: Some(variant),
        }
    }
}

impl From<HnswConfigDiff> for segment::types::HnswConfig {
    fn from(hnsw_config: HnswConfigDiff) -> Self {
        let HnswConfigDiff {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads,
            on_disk,
            payload_m,
            inline_storage,
        } = hnsw_config;
        Self {
            m: m.unwrap_or_default() as usize,
            ef_construct: ef_construct.unwrap_or_default() as usize,
            full_scan_threshold: full_scan_threshold.unwrap_or_default() as usize,
            max_indexing_threads: max_indexing_threads.unwrap_or_default() as usize,
            on_disk,
            payload_m: payload_m.map(|x| x as usize),
            inline_storage,
        }
    }
}

impl From<StrictModeConfig> for segment::types::StrictModeConfig {
    fn from(value: StrictModeConfig) -> Self {
        let StrictModeConfig {
            enabled,
            max_query_limit,
            max_timeout,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef,
            search_allow_exact,
            search_max_oversampling,
            upsert_max_batchsize,
            max_collection_vector_size_bytes,
            read_rate_limit,
            write_rate_limit,
            max_collection_payload_size_bytes,
            max_points_count,
            filter_max_conditions,
            condition_max_size,
            multivector_config,
            sparse_config,
            max_payload_index_count,
        } = value;
        Self {
            enabled,
            max_query_limit: max_query_limit.map(|i| i as usize),
            max_timeout: max_timeout.map(|i| i as usize),
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef: search_max_hnsw_ef.map(|i| i as usize),
            search_allow_exact,
            search_max_oversampling: search_max_oversampling.map(f64::from),
            upsert_max_batchsize: upsert_max_batchsize.map(|i| i as usize),
            max_collection_vector_size_bytes: max_collection_vector_size_bytes.map(|i| i as usize),
            read_rate_limit: read_rate_limit.map(|i| i as usize),
            write_rate_limit: write_rate_limit.map(|i| i as usize),
            max_collection_payload_size_bytes: max_collection_payload_size_bytes
                .map(|i| i as usize),
            max_points_count: max_points_count.map(|i| i as usize),
            filter_max_conditions: filter_max_conditions.map(|i| i as usize),
            condition_max_size: condition_max_size.map(|i| i as usize),
            multivector_config: multivector_config
                .map(segment::types::StrictModeMultivectorConfig::from),
            sparse_config: sparse_config.map(segment::types::StrictModeSparseConfig::from),
            max_payload_index_count: max_payload_index_count.map(|i| i as usize),
        }
    }
}

impl From<StrictModeMultivectorConfig> for segment::types::StrictModeMultivectorConfig {
    fn from(value: StrictModeMultivectorConfig) -> Self {
        let StrictModeMultivectorConfig { multivector_config } = value;
        Self {
            config: multivector_config
                .iter()
                .map(|(name, config)| {
                    (
                        name.clone(),
                        segment::types::StrictModeMultivector {
                            max_vectors: config.max_vectors.map(|i| i as usize),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<StrictModeSparseConfig> for segment::types::StrictModeSparseConfig {
    fn from(value: StrictModeSparseConfig) -> Self {
        let StrictModeSparseConfig { sparse_config } = value;
        Self {
            config: sparse_config
                .into_iter()
                .map(|(name, config)| {
                    (
                        name,
                        segment::types::StrictModeSparse {
                            max_length: config.max_length.map(|i| i as usize),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<segment::types::StrictModeSparseConfig> for StrictModeSparseConfig {
    fn from(value: segment::types::StrictModeSparseConfig) -> Self {
        let segment::types::StrictModeSparseConfig { config } = value;
        Self {
            sparse_config: config
                .into_iter()
                .map(|(name, config)| {
                    (
                        name,
                        StrictModeSparse {
                            max_length: config.max_length.map(|i| i as u64),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<segment::types::StrictModeSparseConfigOutput> for StrictModeSparseConfig {
    fn from(value: segment::types::StrictModeSparseConfigOutput) -> Self {
        let segment::types::StrictModeSparseConfigOutput { config } = value;
        Self {
            sparse_config: config
                .into_iter()
                .map(|(name, config)| {
                    (
                        name,
                        StrictModeSparse {
                            max_length: config.max_length.map(|i| i as u64),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<segment::types::StrictModeConfigOutput> for StrictModeConfig {
    fn from(value: segment::types::StrictModeConfigOutput) -> Self {
        let segment::types::StrictModeConfigOutput {
            enabled,
            max_query_limit,
            max_timeout,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef,
            search_allow_exact,
            search_max_oversampling,
            upsert_max_batchsize,
            max_collection_vector_size_bytes,
            read_rate_limit,
            write_rate_limit,
            max_collection_payload_size_bytes,
            max_points_count,
            filter_max_conditions,
            condition_max_size,
            multivector_config,
            sparse_config,
            max_payload_index_count,
        } = value;
        Self {
            enabled,
            max_query_limit: max_query_limit.map(|i| i as u32),
            max_timeout: max_timeout.map(|i| i as u32),
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef: search_max_hnsw_ef.map(|i| i as u32),
            search_allow_exact,
            search_max_oversampling: search_max_oversampling.map(|i| i as f32),
            upsert_max_batchsize: upsert_max_batchsize.map(|i| i as u64),
            max_collection_vector_size_bytes: max_collection_vector_size_bytes.map(|i| i as u64),
            read_rate_limit: read_rate_limit.map(|i| i as u32),
            write_rate_limit: write_rate_limit.map(|i| i as u32),
            max_collection_payload_size_bytes: max_collection_payload_size_bytes.map(|i| i as u64),
            filter_max_conditions: filter_max_conditions.map(|i| i as u64),
            condition_max_size: condition_max_size.map(|i| i as u64),
            multivector_config: multivector_config.map(StrictModeMultivectorConfig::from),
            sparse_config: sparse_config.map(StrictModeSparseConfig::from),
            max_points_count: max_points_count.map(|i| i as u64),
            max_payload_index_count: max_payload_index_count.map(|i| i as u64),
        }
    }
}

impl From<StrictModeConfig> for segment::types::StrictModeConfigOutput {
    fn from(value: StrictModeConfig) -> Self {
        let StrictModeConfig {
            enabled,
            max_query_limit,
            max_timeout,
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef,
            search_allow_exact,
            search_max_oversampling,
            upsert_max_batchsize,
            max_collection_vector_size_bytes,
            read_rate_limit,
            write_rate_limit,
            max_collection_payload_size_bytes,
            max_points_count,
            filter_max_conditions,
            condition_max_size,
            multivector_config,
            sparse_config,
            max_payload_index_count,
        } = value;
        Self {
            enabled,
            max_query_limit: max_query_limit.map(|i| i as usize),
            max_timeout: max_timeout.map(|i| i as usize),
            unindexed_filtering_retrieve,
            unindexed_filtering_update,
            search_max_hnsw_ef: search_max_hnsw_ef.map(|i| i as usize),
            search_allow_exact,
            search_max_oversampling: search_max_oversampling.map(f64::from),
            upsert_max_batchsize: upsert_max_batchsize.map(|i| i as usize),
            max_collection_vector_size_bytes: max_collection_vector_size_bytes.map(|i| i as usize),
            read_rate_limit: read_rate_limit.map(|i| i as usize),
            write_rate_limit: write_rate_limit.map(|i| i as usize),
            max_collection_payload_size_bytes: max_collection_payload_size_bytes
                .map(|i| i as usize),
            max_points_count: max_points_count.map(|i| i as usize),
            filter_max_conditions: filter_max_conditions.map(|i| i as usize),
            condition_max_size: condition_max_size.map(|i| i as usize),
            multivector_config: multivector_config
                .map(segment::types::StrictModeMultivectorConfigOutput::from),
            sparse_config: sparse_config.map(segment::types::StrictModeSparseConfigOutput::from),
            max_payload_index_count: max_payload_index_count.map(|i| i as usize),
        }
    }
}

impl From<StrictModeMultivectorConfig> for segment::types::StrictModeMultivectorConfigOutput {
    fn from(value: StrictModeMultivectorConfig) -> Self {
        let StrictModeMultivectorConfig { multivector_config } = value;
        let mut config = BTreeMap::new();
        for (name, strict_config) in multivector_config {
            config.insert(
                name,
                segment::types::StrictModeMultivectorOutput {
                    max_vectors: strict_config.max_vectors.map(|i| i as usize),
                },
            );
        }
        Self { config }
    }
}

impl From<segment::types::StrictModeMultivectorConfig> for StrictModeMultivectorConfig {
    fn from(value: segment::types::StrictModeMultivectorConfig) -> Self {
        let segment::types::StrictModeMultivectorConfig { config } = value;
        Self {
            multivector_config: config
                .iter()
                .map(|(name, config)| {
                    (
                        name.clone(),
                        StrictModeMultivector {
                            max_vectors: config.max_vectors.map(|i| i as u64),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<segment::types::StrictModeMultivectorConfigOutput> for StrictModeMultivectorConfig {
    fn from(value: segment::types::StrictModeMultivectorConfigOutput) -> Self {
        let segment::types::StrictModeMultivectorConfigOutput { config } = value;
        Self {
            multivector_config: config
                .iter()
                .map(|(name, config)| {
                    (
                        name.clone(),
                        StrictModeMultivector {
                            max_vectors: config.max_vectors.map(|i| i as u64),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<StrictModeSparseConfig> for segment::types::StrictModeSparseConfigOutput {
    fn from(value: StrictModeSparseConfig) -> Self {
        let StrictModeSparseConfig { sparse_config } = value;
        let mut config = BTreeMap::new();
        for (name, strict_config) in sparse_config {
            config.insert(
                name,
                segment::types::StrictModeSparseOutput {
                    max_length: strict_config.max_length.map(|i| i as usize),
                },
            );
        }
        Self { config }
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
    vector_internal: VectorInternal,
) -> Result<segment_vectors::NamedVectorStruct, Status> {
    use segment_vectors::{NamedSparseVector, NamedVector, NamedVectorStruct};
    use sparse::common::sparse_vector::SparseVector;

    Ok(match vector_internal {
        VectorInternal::Dense(vector) => {
            if let Some(name) = vector_name {
                NamedVectorStruct::Dense(NamedVector { name, vector })
            } else {
                NamedVectorStruct::Default(vector)
            }
        }
        VectorInternal::Sparse(sparse) => {
            if let Some(name) = vector_name {
                let sparse_vector =
                    SparseVector::new(sparse.indices, sparse.values).map_err(|e| {
                        Status::invalid_argument(format!(
                            "Sparse indices does not match sparse vector conditions: {e}"
                        ))
                    })?;
                NamedVectorStruct::Sparse(NamedSparseVector {
                    name,
                    vector: sparse_vector,
                })
            } else {
                return Err(Status::invalid_argument(
                    "Sparse vector must have a name specified",
                ));
            }
        }
        VectorInternal::MultiDense(multi_vector) => {
            let vector_name = vector_name.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string());
            NamedVectorStruct::MultiDense(NamedMultiDenseVector {
                name: vector_name,
                vector: multi_vector,
            })
        }
    })
}

impl From<PointsOperationResponseInternal> for PointsOperationResponse {
    fn from(resp: PointsOperationResponseInternal) -> Self {
        let PointsOperationResponseInternal {
            result,
            time,
            hardware_usage,
            inference_usage,
        } = resp;
        Self {
            result: result.map(grpc::UpdateResult::from),
            time,
            usage: Some(Usage {
                hardware: hardware_usage,
                inference: inference_usage,
            }),
        }
    }
}

// TODO: Make it explicit `from_operations_response` method instead of `impl From<PointsOperationResponse>`?
impl From<PointsOperationResponse> for PointsOperationResponseInternal {
    fn from(resp: PointsOperationResponse) -> Self {
        let PointsOperationResponse {
            result,
            time,
            usage,
        } = resp;
        let Usage {
            hardware,
            inference,
        } = usage.unwrap_or_default();
        Self {
            result: result.map(Into::into),
            time,
            hardware_usage: hardware,
            inference_usage: inference,
        }
    }
}

impl From<UpdateResultInternal> for UpdateResult {
    fn from(res: UpdateResultInternal) -> Self {
        let UpdateResultInternal {
            operation_id,
            status,
            clock_tag: _,
        } = res;
        Self {
            operation_id,
            status,
        }
    }
}

// TODO: Make it explicit `from_update_result` method instead of `impl From<UpdateResult>`?
impl From<UpdateResult> for UpdateResultInternal {
    fn from(res: UpdateResult) -> Self {
        let UpdateResult {
            operation_id,
            status,
        } = res;
        Self {
            operation_id,
            status,
            clock_tag: None,
        }
    }
}

impl From<RecommendStrategy> for crate::rest::RecommendStrategy {
    fn from(value: RecommendStrategy) -> Self {
        match value {
            RecommendStrategy::AverageVector => crate::rest::RecommendStrategy::AverageVector,
            RecommendStrategy::BestScore => crate::rest::RecommendStrategy::BestScore,
            RecommendStrategy::SumScores => crate::rest::RecommendStrategy::SumScores,
        }
    }
}

impl TryFrom<i32> for rest::RecommendStrategy {
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
        let segment_query::RecoQuery {
            positives,
            negatives,
        } = value;
        Self {
            positives: positives.into_iter().map(RawVector::from).collect(),
            negatives: negatives.into_iter().map(RawVector::from).collect(),
        }
    }
}

impl TryFrom<raw_query::Recommend> for segment_query::RecoQuery<segment_vectors::VectorInternal> {
    type Error = Status;
    fn try_from(value: raw_query::Recommend) -> Result<Self, Self::Error> {
        let raw_query::Recommend {
            positives,
            negatives,
        } = value;
        Ok(Self {
            positives: positives
                .into_iter()
                .map(segment_vectors::VectorInternal::try_from)
                .try_collect()?,
            negatives: negatives
                .into_iter()
                .map(segment_vectors::VectorInternal::try_from)
                .try_collect()?,
        })
    }
}

impl From<segment_query::RecoQuery<segment_vectors::VectorInternal>> for RecoQuery {
    fn from(query: segment_query::RecoQuery<segment_vectors::VectorInternal>) -> Self {
        Self {
            positives: query.positives.into_iter().map(From::from).collect(),
            negatives: query.negatives.into_iter().map(From::from).collect(),
        }
    }
}

impl TryFrom<RecoQuery> for segment_query::RecoQuery<segment_vectors::VectorInternal> {
    type Error = Status;

    fn try_from(request: RecoQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            positives: request
                .positives
                .into_iter()
                .map(segment_vectors::VectorInternal::try_from)
                .collect::<Result<_, _>>()?,
            negatives: request
                .negatives
                .into_iter()
                .map(segment_vectors::VectorInternal::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<segment_query::ContextPair<segment_vectors::VectorInternal>>
    for raw_query::RawContextPair
{
    fn from(value: segment_query::ContextPair<segment_vectors::VectorInternal>) -> Self {
        let segment_query::ContextPair { positive, negative } = value;
        Self {
            positive: Some(RawVector::from(positive)),
            negative: Some(RawVector::from(negative)),
        }
    }
}

impl TryFrom<raw_query::RawContextPair>
    for segment_query::ContextPair<segment_vectors::VectorInternal>
{
    type Error = Status;
    fn try_from(value: raw_query::RawContextPair) -> Result<Self, Self::Error> {
        let raw_query::RawContextPair { positive, negative } = value;
        Ok(Self {
            positive: positive
                .map(segment_vectors::VectorInternal::try_from)
                .transpose()?
                .ok_or_else(|| {
                    Status::invalid_argument("No positive part of context pair provided")
                })?,
            negative: negative
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
        let segment_query::ContextQuery { pairs } = value;
        Self {
            context: pairs
                .into_iter()
                .map(raw_query::RawContextPair::from)
                .collect(),
        }
    }
}

impl TryFrom<raw_query::Context> for segment_query::ContextQuery<segment_vectors::VectorInternal> {
    type Error = Status;
    fn try_from(value: raw_query::Context) -> Result<Self, Self::Error> {
        let raw_query::Context { context } = value;
        Ok(Self {
            pairs: context
                .into_iter()
                .map(segment_query::ContextPair::try_from)
                .try_collect()?,
        })
    }
}

impl From<segment_query::DiscoveryQuery<segment_vectors::VectorInternal>> for raw_query::Discovery {
    fn from(value: segment_query::DiscoveryQuery<segment_vectors::VectorInternal>) -> Self {
        let segment_query::DiscoveryQuery { target, pairs } = value;
        Self {
            target: Some(RawVector::from(target)),
            context: pairs
                .into_iter()
                .map(raw_query::RawContextPair::from)
                .collect(),
        }
    }
}

impl TryFrom<raw_query::Discovery>
    for segment_query::DiscoveryQuery<segment_vectors::VectorInternal>
{
    type Error = Status;
    fn try_from(value: raw_query::Discovery) -> Result<Self, Self::Error> {
        let raw_query::Discovery { target, context } = value;
        Ok(Self {
            target: target
                .map(segment_vectors::VectorInternal::try_from)
                .transpose()?
                .ok_or_else(|| Status::invalid_argument("No target provided"))?,
            pairs: context
                .into_iter()
                .map(segment_query::ContextPair::try_from)
                .try_collect()?,
        })
    }
}

impl TryFrom<SearchPoints> for rest::SearchRequestInternal {
    type Error = Status;

    fn try_from(value: SearchPoints) -> Result<Self, Self::Error> {
        let SearchPoints {
            collection_name: _,
            vector,
            filter,
            limit,
            with_payload,
            params,
            score_threshold,
            offset,
            vector_name,
            with_vectors,
            read_consistency: _,
            timeout: _,
            shard_key_selector: _,
            sparse_indices,
        } = value;

        let vector_internal =
            VectorInternal::from_vector_and_indices(vector, sparse_indices.map(|v| v.data));

        let named_struct = into_named_vector_struct(vector_name, vector_internal)?;
        let vector = match named_struct {
            segment_vectors::NamedVectorStruct::Default(v) => rest::NamedVectorStruct::Default(v),
            segment_vectors::NamedVectorStruct::Dense(v) => rest::NamedVectorStruct::Dense(v),
            segment_vectors::NamedVectorStruct::Sparse(v) => rest::NamedVectorStruct::Sparse(v),
            segment_vectors::NamedVectorStruct::MultiDense(_) => {
                return Err(Status::invalid_argument(
                    "MultiDense vector is not supported in search request",
                ));
            }
        };
        Ok(Self {
            vector,
            filter: filter.map(|f| f.try_into()).transpose()?,
            params: params.map(|p| p.into()),
            limit: limit as usize,
            offset: offset.map(|x| x as usize),
            with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: Some(
                with_vectors
                    .map(|with_vectors| with_vectors.into())
                    .unwrap_or_default(),
            ),
            score_threshold,
        })
    }
}

impl TryFrom<SearchPointGroups> for rest::SearchGroupsRequestInternal {
    type Error = Status;

    fn try_from(value: SearchPointGroups) -> Result<Self, Self::Error> {
        let SearchPointGroups {
            collection_name,
            vector,
            filter,
            limit,
            with_payload,
            params,
            score_threshold,
            vector_name,
            with_vectors,
            group_by,
            group_size,
            read_consistency,
            with_lookup,
            timeout,
            shard_key_selector,
            sparse_indices,
        } = value;
        let search_points = SearchPoints {
            vector,
            filter,
            params,
            with_payload,
            with_vectors,
            score_threshold,
            vector_name,
            limit: 0,
            offset: None,
            collection_name,
            read_consistency,
            timeout,
            shard_key_selector,
            sparse_indices,
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
        } = rest::SearchRequestInternal::try_from(search_points)?;

        Ok(Self {
            vector,
            filter,
            params,
            with_payload,
            with_vector,
            score_threshold,
            group_request: rest::BaseGroupRequest {
                group_by: json::json_path_from_proto(&group_by)?,
                limit,
                group_size,
                with_lookup: with_lookup
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
        let WithLookup {
            collection,
            with_payload,
            with_vectors,
        } = value;
        Ok(Self {
            collection_name: collection,
            with_payload: with_payload
                .map(|wp| wp.try_into())
                .transpose()?
                .or_else(with_default_payload),
            with_vectors: with_vectors.map(|wv| wv.into()),
        })
    }
}

impl From<LookupLocation> for rest::LookupLocation {
    fn from(value: LookupLocation) -> Self {
        let LookupLocation {
            collection_name,
            vector_name,
            shard_key_selector,
        } = value;
        Self {
            collection: collection_name,
            vector: vector_name,
            shard_key: shard_key_selector.map(rest::ShardKeySelector::from),
        }
    }
}

impl TryFrom<FacetHitInternal> for segment_facets::FacetValueHit {
    type Error = Status;

    fn try_from(hit: FacetHitInternal) -> Result<Self, Self::Error> {
        let FacetHitInternal { value, count } = hit;
        let value = value.ok_or_else(|| Status::internal("expected FacetHit to have a value"))?;

        Ok(Self {
            value: segment_facets::FacetValue::try_from(value)?,
            count: count as usize,
        })
    }
}

impl From<segment_facets::FacetValueHit> for FacetHitInternal {
    fn from(hit: segment_facets::FacetValueHit) -> Self {
        let segment_facets::FacetValueHit { value, count } = hit;
        Self {
            value: Some(From::from(value)),
            count: count as u64,
        }
    }
}

impl From<segment_facets::FacetValueHit> for FacetHit {
    fn from(hit: segment_facets::FacetValueHit) -> Self {
        let segment_facets::FacetValueHit { value, count } = hit;
        Self {
            value: Some(value.into()),
            count: count as u64,
        }
    }
}

impl TryFrom<FacetValueInternal> for segment_facets::FacetValue {
    type Error = Status;

    fn try_from(value: FacetValueInternal) -> Result<Self, Self::Error> {
        use super::qdrant::facet_value_internal::Variant;
        let FacetValueInternal { variant } = value;
        let variant = variant
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
        let rest::SearchMatrixPair { a, b, score } = pair;
        Self {
            a: Some(a.into()),
            b: Some(b.into()),
            score,
        }
    }
}

impl From<HwMeasurementAcc> for HardwareUsage {
    fn from(value: HwMeasurementAcc) -> Self {
        Self {
            cpu: value.get_cpu() as u64,
            payload_io_read: value.get_payload_io_read() as u64,
            payload_io_write: value.get_payload_io_write() as u64,
            payload_index_io_read: value.get_payload_index_io_read() as u64,
            payload_index_io_write: value.get_payload_index_io_write() as u64,
            vector_io_read: value.get_vector_io_read() as u64,
            vector_io_write: value.get_vector_io_write() as u64,
        }
    }
}

impl From<HardwareUsage> for HardwareData {
    fn from(value: HardwareUsage) -> Self {
        let HardwareUsage {
            cpu,
            payload_io_read,
            payload_io_write,
            payload_index_io_read,
            payload_index_io_write,
            vector_io_read,
            vector_io_write,
        } = value;

        HardwareData {
            cpu: cpu as usize,
            payload_io_read: payload_io_read as usize,
            payload_io_write: payload_io_write as usize,
            payload_index_io_read: payload_index_io_read as usize,
            payload_index_io_write: payload_index_io_write as usize,
            vector_io_read: vector_io_read as usize,
            vector_io_write: vector_io_write as usize,
        }
    }
}

impl Formula {
    /// This implementation is only used to forward a request to remote shards.
    ///
    /// It is preferred to pay the cost of un-parsing->re-parsing the formula, and keep the parsed representation
    /// out of the API surface, than to expose the implementation details to the interface and avoid the extra work.
    /// Conversion should be cheap enough.
    pub fn from_parsed(value: ParsedFormula) -> Self {
        let ParsedFormula {
            formula,
            payload_vars: _, // they are already in the expression
            conditions,
            defaults,
        } = value;

        let expression = unparse_expression(formula, &conditions);

        let defaults = defaults
            .into_iter()
            .map(|(key, value)| (key.unparse(), json_to_proto(value)))
            .collect();

        Formula {
            expression: Some(expression),
            defaults,
        }
    }
}

fn unparse_expression(
    expression: ParsedExpression,
    conditions: &Vec<segment::types::Condition>,
) -> Expression {
    use segment::index::query_optimization::rescore_formula::parsed_formula::VariableId;

    use super::expression::Variant;

    let variant = match expression {
        ParsedExpression::Constant(c) => Variant::Constant(c.0 as ScoreType),
        ParsedExpression::Variable(variable_id) => match variable_id {
            var_id @ VariableId::Score(_) => Variant::Variable(var_id.unparse()),
            var_id @ VariableId::Payload(_) => Variant::Variable(var_id.unparse()),
            VariableId::Condition(cond_idx) => {
                Variant::Condition(Condition::from(conditions[cond_idx].clone()))
            }
        },
        ParsedExpression::GeoDistance { origin, key } => Variant::GeoDistance(GeoDistance {
            origin: Some(GeoPoint::from(origin)),
            to: key.to_string(),
        }),
        ParsedExpression::Datetime(dt_expr) => match dt_expr {
            DatetimeExpression::Constant(date_time_wrapper) => {
                Variant::Datetime(date_time_wrapper.to_string())
            }
            DatetimeExpression::PayloadVariable(json_path) => {
                Variant::DatetimeKey(json_path.to_string())
            }
        },
        ParsedExpression::Mult(exprs) => Variant::Mult(MultExpression {
            mult: exprs
                .into_iter()
                .map(|expr| unparse_expression(expr, conditions))
                .collect(),
        }),
        ParsedExpression::Sum(exprs) => Variant::Sum(SumExpression {
            sum: exprs
                .into_iter()
                .map(|expr| unparse_expression(expr, conditions))
                .collect(),
        }),
        ParsedExpression::Neg(expr) => {
            Variant::Neg(Box::new(unparse_expression(*expr, conditions)))
        }
        ParsedExpression::Div {
            left,
            right,
            by_zero_default,
        } => Variant::Div(Box::new(DivExpression {
            left: Some(Box::new(unparse_expression(*left, conditions))),
            right: Some(Box::new(unparse_expression(*right, conditions))),
            by_zero_default: by_zero_default.map(|v| v.0 as f32),
        })),
        ParsedExpression::Sqrt(expr) => {
            Variant::Sqrt(Box::new(unparse_expression(*expr, conditions)))
        }
        ParsedExpression::Pow { base, exponent } => Variant::Pow(Box::new(PowExpression {
            base: Some(Box::new(unparse_expression(*base, conditions))),
            exponent: Some(Box::new(unparse_expression(*exponent, conditions))),
        })),
        ParsedExpression::Exp(expr) => {
            Variant::Exp(Box::new(unparse_expression(*expr, conditions)))
        }
        ParsedExpression::Log10(expr) => {
            Variant::Log10(Box::new(unparse_expression(*expr, conditions)))
        }
        ParsedExpression::Ln(expr) => Variant::Ln(Box::new(unparse_expression(*expr, conditions))),
        ParsedExpression::Abs(expr) => {
            Variant::Abs(Box::new(unparse_expression(*expr, conditions)))
        }
        ParsedExpression::Decay {
            kind,
            target,
            lambda,
            x,
        } => {
            let (midpoint, scale) = ParsedExpression::decay_lambda_to_params(lambda.0, kind);
            let params = DecayParamsExpression {
                x: Some(Box::new(unparse_expression(*x, conditions))),
                target: target.map(|t| Box::new(unparse_expression(*t, conditions))),
                midpoint: Some(midpoint),
                scale: Some(scale),
            };
            match kind {
                DecayKind::Lin => Variant::LinDecay(Box::new(params)),
                DecayKind::Exp => Variant::ExpDecay(Box::new(params)),
                DecayKind::Gauss => Variant::GaussDecay(Box::new(params)),
            }
        }
    };

    Expression {
        variant: Some(variant),
    }
}
