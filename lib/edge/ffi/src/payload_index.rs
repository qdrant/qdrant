//! Payload-index parameter types shared by the write and read surfaces:
//! accepted by
//! [`UpdateOperation::create_field_index_with_params`](crate::update::UpdateOperation::create_field_index_with_params)
//! and reported back by [`EdgeShard::info`](crate::EdgeShard::info) inside
//! [`PayloadIndexInfo`].
//!
//! Every parameter the engine's `PayloadSchemaParams` carries is mirrored
//! here, with two boundary-wide normalizations:
//!
//! - The deprecated `on_disk` flag is not part of the surface. Writes always
//!   leave it unset; reads resolve it into the equivalent [`Memory`] placement
//!   (the explicit `memory` parameter wins when both are present), so configs
//!   written by older tooling still report their effective placement.
//! - The serde-only single-variant `type` tags (`KeywordIndexType`, …) carry
//!   no information and do not cross the boundary; the index type is the
//!   [`PayloadIndexParams`] variant itself.

use segment::data_types::index as segment_index;
use segment::types::{
    Memory as SegmentMemory, PayloadIndexInfo as SegmentPayloadIndexInfo, PayloadSchemaParams,
};

use crate::config::Memory;
use crate::error::EdgeError;
use crate::update::PayloadSchemaType;

// ── PayloadIndexInfo ────────────────────────────────────────────────────────

/// A payload index of the shard, as reported by
/// [`EdgeShard::info`](crate::EdgeShard::info).
#[derive(Clone, Debug, uniffi::Record)]
pub struct PayloadIndexInfo {
    /// The indexed value type.
    pub data_type: PayloadSchemaType,
    /// The per-type parameters the index was created with. `None`/`null` when
    /// the index was created from a bare type (e.g. via
    /// [`UpdateOperation::create_field_index`](crate::update::UpdateOperation::create_field_index)),
    /// meaning every parameter is at its engine default.
    pub params: Option<PayloadIndexParams>,
    /// Number of points indexed under this field, summed over all segments.
    pub points: u64,
}

impl From<SegmentPayloadIndexInfo> for PayloadIndexInfo {
    fn from(info: SegmentPayloadIndexInfo) -> Self {
        let SegmentPayloadIndexInfo {
            data_type,
            params,
            points,
        } = info;
        PayloadIndexInfo {
            data_type: PayloadSchemaType::from(data_type),
            params: params.map(PayloadIndexParams::from),
            points: points as u64,
        }
    }
}

// ── PayloadIndexParams ──────────────────────────────────────────────────────

/// Per-type payload index parameters.
///
/// The variant selects the index type (the parameterized counterpart of
/// [`PayloadSchemaType`]); its `config` carries the type's tuning options.
/// Unset options fall back to the engine defaults documented per field.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum PayloadIndexParams {
    /// Exact-match strings (categories, tags).
    Keyword { config: KeywordIndexParams },
    /// 64-bit integers, indexed for matching and/or ranges.
    Integer { config: IntegerIndexParams },
    /// Floating-point numbers, indexed for ranges.
    Float { config: FloatIndexParams },
    /// Geo points, indexed for radius/bounding-box/polygon filters.
    Geo { config: GeoIndexParams },
    /// Full-text index over string values.
    Text { config: TextIndexParams },
    /// Booleans.
    Bool { config: BoolIndexParams },
    /// RFC 3339 datetimes, indexed for ranges.
    Datetime { config: DatetimeIndexParams },
    /// UUID strings (more compact than `Keyword` for UUID data).
    Uuid { config: UuidIndexParams },
}

/// Parameters of a keyword payload index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct KeywordIndexParams {
    /// If true, the field is used for tenant optimization: the storage is
    /// organized so that points sharing a value cluster together. Default:
    /// false.
    #[uniffi(default = None)]
    pub is_tenant: Option<bool>,
    /// Memory placement of the index. `None`/`null` uses the engine default
    /// (`Pinned`).
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// If true, build additional HNSW links for this field (requires
    /// `payload_m > 0` in the HNSW config). Default: true.
    #[uniffi(default = None)]
    pub enable_hnsw: Option<bool>,
    /// If true, enable prefix matching on this field. Default: false.
    #[uniffi(default = None)]
    pub prefix: Option<bool>,
}

/// Parameters of an integer payload index.
///
/// `lookup` and `range` cannot both be disabled; such params are rejected
/// with [`EdgeError::InvalidArgument`] at conversion.
#[derive(Clone, Debug, uniffi::Record)]
pub struct IntegerIndexParams {
    /// If true, support direct value lookups (`match`). Default: true.
    #[uniffi(default = None)]
    pub lookup: Option<bool>,
    /// If true, support range filters. Default: true.
    #[uniffi(default = None)]
    pub range: Option<bool>,
    /// If true, the field is used to organize the storage of the collection
    /// data, assuming most filtered requests use it. Default: false.
    #[uniffi(default = None)]
    pub is_principal: Option<bool>,
    /// Memory placement of the index. `None`/`null` uses the engine default
    /// (`Pinned`).
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// If true, build additional HNSW links for this field (requires
    /// `payload_m > 0` in the HNSW config). Default: true.
    #[uniffi(default = None)]
    pub enable_hnsw: Option<bool>,
}

/// Parameters of a float payload index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FloatIndexParams {
    /// If true, the field is used to organize the storage of the collection
    /// data, assuming most filtered requests use it. Default: false.
    #[uniffi(default = None)]
    pub is_principal: Option<bool>,
    /// Memory placement of the index. `None`/`null` uses the engine default
    /// (`Pinned`).
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// If true, build additional HNSW links for this field (requires
    /// `payload_m > 0` in the HNSW config). Default: true.
    #[uniffi(default = None)]
    pub enable_hnsw: Option<bool>,
}

/// Parameters of a geo payload index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GeoIndexParams {
    /// Memory placement of the index. `None`/`null` uses the engine default
    /// (`Pinned`).
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// If true, build additional HNSW links for this field (requires
    /// `payload_m > 0` in the HNSW config). Default: true.
    #[uniffi(default = None)]
    pub enable_hnsw: Option<bool>,
}

/// Parameters of a full-text payload index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct TextIndexParams {
    /// How string values are split into tokens. `None`/`null` uses the engine
    /// default (`Word`).
    #[uniffi(default = None)]
    pub tokenizer: Option<TokenizerType>,
    /// Minimum token length to index; shorter tokens are dropped.
    #[uniffi(default = None)]
    pub min_token_len: Option<u64>,
    /// Maximum token length to index; longer tokens are dropped.
    #[uniffi(default = None)]
    pub max_token_len: Option<u64>,
    /// If true, lowercase all tokens. Default: true.
    #[uniffi(default = None)]
    pub lowercase: Option<bool>,
    /// If true, fold accented characters to ASCII (e.g. "ação" → "acao").
    /// Default: false.
    #[uniffi(default = None)]
    pub ascii_folding: Option<bool>,
    /// If true, support phrase matching. Default: false.
    #[uniffi(default = None)]
    pub phrase_matching: Option<bool>,
    /// Tokens to ignore: predefined per-language lists and/or a custom set.
    #[uniffi(default = None)]
    pub stopwords: Option<Stopwords>,
    /// Memory placement of the index. `None`/`null` uses the engine default
    /// (`Pinned`).
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// Stemming algorithm. `None`/`null` uses the language default; pass
    /// [`Stemmer::Disabled`] to explicitly opt out.
    #[uniffi(default = None)]
    pub stemmer: Option<Stemmer>,
    /// If true, build additional HNSW links for this field (requires
    /// `payload_m > 0` in the HNSW config). Default: true.
    #[uniffi(default = None)]
    pub enable_hnsw: Option<bool>,
}

/// Parameters of a bool payload index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct BoolIndexParams {
    /// Memory placement of the index. `None`/`null` uses the engine default
    /// (`Pinned`).
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// If true, build additional HNSW links for this field (requires
    /// `payload_m > 0` in the HNSW config). Default: true.
    #[uniffi(default = None)]
    pub enable_hnsw: Option<bool>,
}

/// Parameters of a datetime payload index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct DatetimeIndexParams {
    /// If true, the field is used to organize the storage of the collection
    /// data, assuming most filtered requests use it. Default: false.
    #[uniffi(default = None)]
    pub is_principal: Option<bool>,
    /// Memory placement of the index. `None`/`null` uses the engine default
    /// (`Pinned`).
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// If true, build additional HNSW links for this field (requires
    /// `payload_m > 0` in the HNSW config). Default: true.
    #[uniffi(default = None)]
    pub enable_hnsw: Option<bool>,
}

/// Parameters of a UUID payload index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct UuidIndexParams {
    /// If true, the field is used for tenant optimization: the storage is
    /// organized so that points sharing a value cluster together. Default:
    /// false.
    #[uniffi(default = None)]
    pub is_tenant: Option<bool>,
    /// Memory placement of the index. `None`/`null` uses the engine default
    /// (`Pinned`).
    #[uniffi(default = None)]
    pub memory: Option<Memory>,
    /// If true, build additional HNSW links for this field (requires
    /// `payload_m > 0` in the HNSW config). Default: true.
    #[uniffi(default = None)]
    pub enable_hnsw: Option<bool>,
}

// ── Full-text options ───────────────────────────────────────────────────────

/// How a full-text index splits string values into tokens.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum TokenizerType {
    /// Index every prefix of each word (enables cheap prefix search).
    Prefix,
    /// Split on whitespace only.
    Whitespace,
    /// Split on any non-alphanumeric character (engine default).
    Word,
    /// Unicode-aware segmentation for languages without word separators.
    Multilingual,
}

impl From<TokenizerType> for segment_index::TokenizerType {
    fn from(t: TokenizerType) -> Self {
        match t {
            TokenizerType::Prefix => segment_index::TokenizerType::Prefix,
            TokenizerType::Whitespace => segment_index::TokenizerType::Whitespace,
            TokenizerType::Word => segment_index::TokenizerType::Word,
            TokenizerType::Multilingual => segment_index::TokenizerType::Multilingual,
        }
    }
}

impl From<segment_index::TokenizerType> for TokenizerType {
    fn from(t: segment_index::TokenizerType) -> Self {
        match t {
            segment_index::TokenizerType::Prefix => TokenizerType::Prefix,
            segment_index::TokenizerType::Whitespace => TokenizerType::Whitespace,
            segment_index::TokenizerType::Word => TokenizerType::Word,
            segment_index::TokenizerType::Multilingual => TokenizerType::Multilingual,
        }
    }
}

/// Stopwords of a full-text index: tokens excluded from indexing.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum Stopwords {
    /// The predefined stopword list of a single language.
    Language { language: Language },
    /// A combination of predefined language lists and custom tokens, merged
    /// together. Duplicates are collapsed.
    Set {
        languages: Option<Vec<Language>>,
        custom: Option<Vec<String>>,
    },
}

impl From<Stopwords> for segment_index::StopwordsInterface {
    fn from(s: Stopwords) -> Self {
        match s {
            Stopwords::Language { language } => {
                segment_index::StopwordsInterface::Language(segment_index::Language::from(language))
            }
            Stopwords::Set { languages, custom } => {
                segment_index::StopwordsInterface::Set(segment_index::StopwordsSet {
                    languages: languages.map(|languages| {
                        languages
                            .into_iter()
                            .map(segment_index::Language::from)
                            .collect()
                    }),
                    custom: custom.map(|custom| custom.into_iter().collect()),
                })
            }
        }
    }
}

impl From<segment_index::StopwordsInterface> for Stopwords {
    fn from(s: segment_index::StopwordsInterface) -> Self {
        match s {
            segment_index::StopwordsInterface::Language(language) => Stopwords::Language {
                language: Language::from(language),
            },
            segment_index::StopwordsInterface::Set(set) => {
                let segment_index::StopwordsSet { languages, custom } = set;
                Stopwords::Set {
                    // BTreeSet iteration is ordered, so the reported lists are
                    // deterministic.
                    languages: languages
                        .map(|languages| languages.into_iter().map(Language::from).collect()),
                    custom: custom.map(|custom| custom.into_iter().collect()),
                }
            }
        }
    }
}

/// Stemming of a full-text index: reducing tokens to their word stem before
/// indexing and matching.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum Stemmer {
    /// The Snowball stemmer for the given language.
    Snowball { language: SnowballLanguage },
    /// Explicitly no stemming. Differs from leaving the `stemmer` option
    /// unset, which falls back to the language default stemmer.
    Disabled,
}

impl From<Stemmer> for segment_index::StemmingAlgorithm {
    fn from(s: Stemmer) -> Self {
        match s {
            Stemmer::Snowball { language } => {
                segment_index::StemmingAlgorithm::Snowball(segment_index::SnowballParams {
                    r#type: segment_index::Snowball::Snowball,
                    language: segment_index::SnowballLanguage::from(language),
                })
            }
            Stemmer::Disabled => {
                segment_index::StemmingAlgorithm::Disabled(segment_index::DisabledStemmerParams {
                    r#type: segment_index::NoStemmer::None,
                })
            }
        }
    }
}

impl From<segment_index::StemmingAlgorithm> for Stemmer {
    fn from(s: segment_index::StemmingAlgorithm) -> Self {
        match s {
            segment_index::StemmingAlgorithm::Snowball(params) => {
                let segment_index::SnowballParams {
                    r#type: _,
                    language,
                } = params;
                Stemmer::Snowball {
                    language: SnowballLanguage::from(language),
                }
            }
            segment_index::StemmingAlgorithm::Disabled(params) => {
                let segment_index::DisabledStemmerParams { r#type: _ } = params;
                Stemmer::Disabled
            }
        }
    }
}

/// Languages with a predefined stopword list.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum Language {
    Arabic,
    Azerbaijani,
    Basque,
    Bengali,
    Catalan,
    Chinese,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hebrew,
    Hinglish,
    Hungarian,
    Indonesian,
    Italian,
    Japanese,
    Kazakh,
    Nepali,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Slovene,
    Spanish,
    Swedish,
    Tajik,
    Turkish,
}

impl From<Language> for segment_index::Language {
    fn from(l: Language) -> Self {
        match l {
            Language::Arabic => segment_index::Language::Arabic,
            Language::Azerbaijani => segment_index::Language::Azerbaijani,
            Language::Basque => segment_index::Language::Basque,
            Language::Bengali => segment_index::Language::Bengali,
            Language::Catalan => segment_index::Language::Catalan,
            Language::Chinese => segment_index::Language::Chinese,
            Language::Danish => segment_index::Language::Danish,
            Language::Dutch => segment_index::Language::Dutch,
            Language::English => segment_index::Language::English,
            Language::Finnish => segment_index::Language::Finnish,
            Language::French => segment_index::Language::French,
            Language::German => segment_index::Language::German,
            Language::Greek => segment_index::Language::Greek,
            Language::Hebrew => segment_index::Language::Hebrew,
            Language::Hinglish => segment_index::Language::Hinglish,
            Language::Hungarian => segment_index::Language::Hungarian,
            Language::Indonesian => segment_index::Language::Indonesian,
            Language::Italian => segment_index::Language::Italian,
            Language::Japanese => segment_index::Language::Japanese,
            Language::Kazakh => segment_index::Language::Kazakh,
            Language::Nepali => segment_index::Language::Nepali,
            Language::Norwegian => segment_index::Language::Norwegian,
            Language::Portuguese => segment_index::Language::Portuguese,
            Language::Romanian => segment_index::Language::Romanian,
            Language::Russian => segment_index::Language::Russian,
            Language::Slovene => segment_index::Language::Slovene,
            Language::Spanish => segment_index::Language::Spanish,
            Language::Swedish => segment_index::Language::Swedish,
            Language::Tajik => segment_index::Language::Tajik,
            Language::Turkish => segment_index::Language::Turkish,
        }
    }
}

impl From<segment_index::Language> for Language {
    fn from(l: segment_index::Language) -> Self {
        match l {
            segment_index::Language::Arabic => Language::Arabic,
            segment_index::Language::Azerbaijani => Language::Azerbaijani,
            segment_index::Language::Basque => Language::Basque,
            segment_index::Language::Bengali => Language::Bengali,
            segment_index::Language::Catalan => Language::Catalan,
            segment_index::Language::Chinese => Language::Chinese,
            segment_index::Language::Danish => Language::Danish,
            segment_index::Language::Dutch => Language::Dutch,
            segment_index::Language::English => Language::English,
            segment_index::Language::Finnish => Language::Finnish,
            segment_index::Language::French => Language::French,
            segment_index::Language::German => Language::German,
            segment_index::Language::Greek => Language::Greek,
            segment_index::Language::Hebrew => Language::Hebrew,
            segment_index::Language::Hinglish => Language::Hinglish,
            segment_index::Language::Hungarian => Language::Hungarian,
            segment_index::Language::Indonesian => Language::Indonesian,
            segment_index::Language::Italian => Language::Italian,
            segment_index::Language::Japanese => Language::Japanese,
            segment_index::Language::Kazakh => Language::Kazakh,
            segment_index::Language::Nepali => Language::Nepali,
            segment_index::Language::Norwegian => Language::Norwegian,
            segment_index::Language::Portuguese => Language::Portuguese,
            segment_index::Language::Romanian => Language::Romanian,
            segment_index::Language::Russian => Language::Russian,
            segment_index::Language::Slovene => Language::Slovene,
            segment_index::Language::Spanish => Language::Spanish,
            segment_index::Language::Swedish => Language::Swedish,
            segment_index::Language::Tajik => Language::Tajik,
            segment_index::Language::Turkish => Language::Turkish,
        }
    }
}

/// Languages supported by the Snowball stemmer.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum SnowballLanguage {
    Arabic,
    Armenian,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
}

impl From<SnowballLanguage> for segment_index::SnowballLanguage {
    fn from(l: SnowballLanguage) -> Self {
        match l {
            SnowballLanguage::Arabic => segment_index::SnowballLanguage::Arabic,
            SnowballLanguage::Armenian => segment_index::SnowballLanguage::Armenian,
            SnowballLanguage::Danish => segment_index::SnowballLanguage::Danish,
            SnowballLanguage::Dutch => segment_index::SnowballLanguage::Dutch,
            SnowballLanguage::English => segment_index::SnowballLanguage::English,
            SnowballLanguage::Finnish => segment_index::SnowballLanguage::Finnish,
            SnowballLanguage::French => segment_index::SnowballLanguage::French,
            SnowballLanguage::German => segment_index::SnowballLanguage::German,
            SnowballLanguage::Greek => segment_index::SnowballLanguage::Greek,
            SnowballLanguage::Hungarian => segment_index::SnowballLanguage::Hungarian,
            SnowballLanguage::Italian => segment_index::SnowballLanguage::Italian,
            SnowballLanguage::Norwegian => segment_index::SnowballLanguage::Norwegian,
            SnowballLanguage::Portuguese => segment_index::SnowballLanguage::Portuguese,
            SnowballLanguage::Romanian => segment_index::SnowballLanguage::Romanian,
            SnowballLanguage::Russian => segment_index::SnowballLanguage::Russian,
            SnowballLanguage::Spanish => segment_index::SnowballLanguage::Spanish,
            SnowballLanguage::Swedish => segment_index::SnowballLanguage::Swedish,
            SnowballLanguage::Tamil => segment_index::SnowballLanguage::Tamil,
            SnowballLanguage::Turkish => segment_index::SnowballLanguage::Turkish,
        }
    }
}

impl From<segment_index::SnowballLanguage> for SnowballLanguage {
    fn from(l: segment_index::SnowballLanguage) -> Self {
        match l {
            segment_index::SnowballLanguage::Arabic => SnowballLanguage::Arabic,
            segment_index::SnowballLanguage::Armenian => SnowballLanguage::Armenian,
            segment_index::SnowballLanguage::Danish => SnowballLanguage::Danish,
            segment_index::SnowballLanguage::Dutch => SnowballLanguage::Dutch,
            segment_index::SnowballLanguage::English => SnowballLanguage::English,
            segment_index::SnowballLanguage::Finnish => SnowballLanguage::Finnish,
            segment_index::SnowballLanguage::French => SnowballLanguage::French,
            segment_index::SnowballLanguage::German => SnowballLanguage::German,
            segment_index::SnowballLanguage::Greek => SnowballLanguage::Greek,
            segment_index::SnowballLanguage::Hungarian => SnowballLanguage::Hungarian,
            segment_index::SnowballLanguage::Italian => SnowballLanguage::Italian,
            segment_index::SnowballLanguage::Norwegian => SnowballLanguage::Norwegian,
            segment_index::SnowballLanguage::Portuguese => SnowballLanguage::Portuguese,
            segment_index::SnowballLanguage::Romanian => SnowballLanguage::Romanian,
            segment_index::SnowballLanguage::Russian => SnowballLanguage::Russian,
            segment_index::SnowballLanguage::Spanish => SnowballLanguage::Spanish,
            segment_index::SnowballLanguage::Swedish => SnowballLanguage::Swedish,
            segment_index::SnowballLanguage::Tamil => SnowballLanguage::Tamil,
            segment_index::SnowballLanguage::Turkish => SnowballLanguage::Turkish,
        }
    }
}

// ── Conversions: FFI params → engine params (write side) ────────────────────

/// Casts a host-supplied token length to the engine's `usize`.
///
/// Only fallible on 32-bit targets; rejecting (instead of truncating) keeps
/// the behavior identical across architectures.
fn token_len(name: &str, len: Option<u64>) -> Result<Option<usize>, EdgeError> {
    len.map(|len| {
        usize::try_from(len)
            .map_err(|_| EdgeError::invalid_argument(format!("{name} {len} out of range")))
    })
    .transpose()
}

// The deprecated `on_disk` flags exist solely because the internal structs
// still declare them; the FFI surface only carries the `memory` placement, so
// they are always written as `None`.
#[allow(deprecated)]
impl TryFrom<PayloadIndexParams> for PayloadSchemaParams {
    type Error = EdgeError;

    fn try_from(params: PayloadIndexParams) -> Result<Self, Self::Error> {
        match params {
            PayloadIndexParams::Keyword { config } => {
                let KeywordIndexParams {
                    is_tenant,
                    memory,
                    enable_hnsw,
                    prefix,
                } = config;
                Ok(PayloadSchemaParams::Keyword(
                    segment_index::KeywordIndexParams {
                        r#type: segment_index::KeywordIndexType::Keyword,
                        is_tenant,
                        on_disk: None,
                        memory: memory.map(SegmentMemory::from),
                        enable_hnsw,
                        prefix,
                    },
                ))
            }
            PayloadIndexParams::Integer { config } => {
                let IntegerIndexParams {
                    lookup,
                    range,
                    is_principal,
                    memory,
                    enable_hnsw,
                } = config;
                // Same rule the server enforces on its API: an integer index
                // with both capabilities disabled would index nothing.
                segment_index::validate_integer_index_params(&lookup, &range).map_err(|_| {
                    EdgeError::invalid_argument(
                        "integer index: the 'lookup' and 'range' capabilities \
                         can't be both disabled",
                    )
                })?;
                Ok(PayloadSchemaParams::Integer(
                    segment_index::IntegerIndexParams {
                        r#type: segment_index::IntegerIndexType::Integer,
                        lookup,
                        range,
                        is_principal,
                        on_disk: None,
                        memory: memory.map(SegmentMemory::from),
                        enable_hnsw,
                    },
                ))
            }
            PayloadIndexParams::Float { config } => {
                let FloatIndexParams {
                    is_principal,
                    memory,
                    enable_hnsw,
                } = config;
                Ok(PayloadSchemaParams::Float(
                    segment_index::FloatIndexParams {
                        r#type: segment_index::FloatIndexType::Float,
                        is_principal,
                        on_disk: None,
                        memory: memory.map(SegmentMemory::from),
                        enable_hnsw,
                    },
                ))
            }
            PayloadIndexParams::Geo { config } => {
                let GeoIndexParams {
                    memory,
                    enable_hnsw,
                } = config;
                Ok(PayloadSchemaParams::Geo(segment_index::GeoIndexParams {
                    r#type: segment_index::GeoIndexType::Geo,
                    on_disk: None,
                    memory: memory.map(SegmentMemory::from),
                    enable_hnsw,
                }))
            }
            PayloadIndexParams::Text { config } => {
                let TextIndexParams {
                    tokenizer,
                    min_token_len,
                    max_token_len,
                    lowercase,
                    ascii_folding,
                    phrase_matching,
                    stopwords,
                    memory,
                    stemmer,
                    enable_hnsw,
                } = config;
                Ok(PayloadSchemaParams::Text(segment_index::TextIndexParams {
                    r#type: segment_index::TextIndexType::Text,
                    tokenizer: tokenizer
                        .map(segment_index::TokenizerType::from)
                        .unwrap_or_default(),
                    min_token_len: token_len("min_token_len", min_token_len)?,
                    max_token_len: token_len("max_token_len", max_token_len)?,
                    lowercase,
                    ascii_folding,
                    phrase_matching,
                    stopwords: stopwords.map(segment_index::StopwordsInterface::from),
                    on_disk: None,
                    memory: memory.map(SegmentMemory::from),
                    stemmer: stemmer.map(segment_index::StemmingAlgorithm::from),
                    enable_hnsw,
                }))
            }
            PayloadIndexParams::Bool { config } => {
                let BoolIndexParams {
                    memory,
                    enable_hnsw,
                } = config;
                Ok(PayloadSchemaParams::Bool(segment_index::BoolIndexParams {
                    r#type: segment_index::BoolIndexType::Bool,
                    on_disk: None,
                    memory: memory.map(SegmentMemory::from),
                    enable_hnsw,
                }))
            }
            PayloadIndexParams::Datetime { config } => {
                let DatetimeIndexParams {
                    is_principal,
                    memory,
                    enable_hnsw,
                } = config;
                Ok(PayloadSchemaParams::Datetime(
                    segment_index::DatetimeIndexParams {
                        r#type: segment_index::DatetimeIndexType::Datetime,
                        is_principal,
                        on_disk: None,
                        memory: memory.map(SegmentMemory::from),
                        enable_hnsw,
                    },
                ))
            }
            PayloadIndexParams::Uuid { config } => {
                let UuidIndexParams {
                    is_tenant,
                    memory,
                    enable_hnsw,
                } = config;
                Ok(PayloadSchemaParams::Uuid(segment_index::UuidIndexParams {
                    r#type: segment_index::UuidIndexType::Uuid,
                    is_tenant,
                    on_disk: None,
                    memory: memory.map(SegmentMemory::from),
                    enable_hnsw,
                }))
            }
        }
    }
}

// ── Conversions: engine params → FFI params (read side) ─────────────────────

/// Resolves the effective memory placement from the engine's pair of fields:
/// the explicit `memory` parameter wins over the deprecated `on_disk` flag
/// (translated with the heap-component rule, matching
/// `PayloadSchemaParams::memory_placement`). `None` when neither is set.
fn resolve_memory(memory: Option<SegmentMemory>, on_disk: Option<bool>) -> Option<Memory> {
    SegmentMemory::resolve(memory, on_disk.map(SegmentMemory::from_on_disk_heap)).map(Memory::from)
}

// The deprecated `on_disk` flags are consumed only through `resolve_memory`
// (which folds them into the reported `memory` placement); the exhaustive
// destructuring must still name them.
#[allow(deprecated)]
impl From<PayloadSchemaParams> for PayloadIndexParams {
    fn from(params: PayloadSchemaParams) -> Self {
        match params {
            PayloadSchemaParams::Keyword(params) => {
                let memory = resolve_memory(params.memory, params.on_disk);
                let segment_index::KeywordIndexParams {
                    r#type: _,
                    is_tenant,
                    on_disk: _,
                    memory: _,
                    enable_hnsw,
                    prefix,
                } = params;
                PayloadIndexParams::Keyword {
                    config: KeywordIndexParams {
                        is_tenant,
                        memory,
                        enable_hnsw,
                        prefix,
                    },
                }
            }
            PayloadSchemaParams::Integer(params) => {
                let memory = resolve_memory(params.memory, params.on_disk);
                let segment_index::IntegerIndexParams {
                    r#type: _,
                    lookup,
                    range,
                    is_principal,
                    on_disk: _,
                    memory: _,
                    enable_hnsw,
                } = params;
                PayloadIndexParams::Integer {
                    config: IntegerIndexParams {
                        lookup,
                        range,
                        is_principal,
                        memory,
                        enable_hnsw,
                    },
                }
            }
            PayloadSchemaParams::Float(params) => {
                let memory = resolve_memory(params.memory, params.on_disk);
                let segment_index::FloatIndexParams {
                    r#type: _,
                    is_principal,
                    on_disk: _,
                    memory: _,
                    enable_hnsw,
                } = params;
                PayloadIndexParams::Float {
                    config: FloatIndexParams {
                        is_principal,
                        memory,
                        enable_hnsw,
                    },
                }
            }
            PayloadSchemaParams::Geo(params) => {
                let memory = resolve_memory(params.memory, params.on_disk);
                let segment_index::GeoIndexParams {
                    r#type: _,
                    on_disk: _,
                    memory: _,
                    enable_hnsw,
                } = params;
                PayloadIndexParams::Geo {
                    config: GeoIndexParams {
                        memory,
                        enable_hnsw,
                    },
                }
            }
            PayloadSchemaParams::Text(params) => {
                let memory = resolve_memory(params.memory, params.on_disk);
                let segment_index::TextIndexParams {
                    r#type: _,
                    tokenizer,
                    min_token_len,
                    max_token_len,
                    lowercase,
                    ascii_folding,
                    phrase_matching,
                    stopwords,
                    on_disk: _,
                    memory: _,
                    stemmer,
                    enable_hnsw,
                } = params;
                PayloadIndexParams::Text {
                    config: TextIndexParams {
                        tokenizer: Some(TokenizerType::from(tokenizer)),
                        min_token_len: min_token_len.map(|len| len as u64),
                        max_token_len: max_token_len.map(|len| len as u64),
                        lowercase,
                        ascii_folding,
                        phrase_matching,
                        stopwords: stopwords.map(Stopwords::from),
                        memory,
                        stemmer: stemmer.map(Stemmer::from),
                        enable_hnsw,
                    },
                }
            }
            PayloadSchemaParams::Bool(params) => {
                let memory = resolve_memory(params.memory, params.on_disk);
                let segment_index::BoolIndexParams {
                    r#type: _,
                    on_disk: _,
                    memory: _,
                    enable_hnsw,
                } = params;
                PayloadIndexParams::Bool {
                    config: BoolIndexParams {
                        memory,
                        enable_hnsw,
                    },
                }
            }
            PayloadSchemaParams::Datetime(params) => {
                let memory = resolve_memory(params.memory, params.on_disk);
                let segment_index::DatetimeIndexParams {
                    r#type: _,
                    is_principal,
                    on_disk: _,
                    memory: _,
                    enable_hnsw,
                } = params;
                PayloadIndexParams::Datetime {
                    config: DatetimeIndexParams {
                        is_principal,
                        memory,
                        enable_hnsw,
                    },
                }
            }
            PayloadSchemaParams::Uuid(params) => {
                let memory = resolve_memory(params.memory, params.on_disk);
                let segment_index::UuidIndexParams {
                    r#type: _,
                    is_tenant,
                    on_disk: _,
                    memory: _,
                    enable_hnsw,
                } = params;
                PayloadIndexParams::Uuid {
                    config: UuidIndexParams {
                        is_tenant,
                        memory,
                        enable_hnsw,
                    },
                }
            }
        }
    }
}
