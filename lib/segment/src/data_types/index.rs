use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError, ValidationErrors};

// Keyword

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KeywordIndexType {
    #[default]
    Keyword,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct KeywordIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: KeywordIndexType,

    /// If true - used for tenant optimization. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_tenant: Option<bool>,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

// Integer

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntegerIndexType {
    #[default]
    Integer,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct IntegerIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: IntegerIndexType,

    /// If true - support direct lookups.
    /// Default is true.
    pub lookup: Option<bool>,

    /// If true - support ranges filters.
    /// Default is true.
    pub range: Option<bool>,

    /// If true - use this key to organize storage of the collection data.
    /// This option assumes that this key will be used in majority of filtered requests.
    /// Default is false.
    pub is_principal: Option<bool>,

    /// If true, store the index on disk. Default: false.
    /// Default is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

impl Validate for IntegerIndexParams {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let IntegerIndexParams {
            r#type: _,
            lookup,
            range,
            is_principal: _,
            on_disk: _,
        } = &self;
        validate_integer_index_params(lookup, range)
    }
}

pub fn validate_integer_index_params(
    lookup: &Option<bool>,
    range: &Option<bool>,
) -> Result<(), ValidationErrors> {
    if lookup == &Some(false) && range == &Some(false) {
        let mut errors = ValidationErrors::new();
        let error =
            ValidationError::new("the 'lookup' and 'range' capabilities can't be both disabled");
        errors.add("lookup", error);
        return Err(errors);
    }
    Ok(())
}

// UUID

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UuidIndexType {
    #[default]
    Uuid,
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct UuidIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: UuidIndexType,

    /// If true - used for tenant optimization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_tenant: Option<bool>,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

// Float

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FloatIndexType {
    #[default]
    Float,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct FloatIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: FloatIndexType,

    /// If true - use this key to organize storage of the collection data.
    /// This option assumes that this key will be used in majority of filtered requests.
    pub is_principal: Option<bool>,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

// Geo

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GeoIndexType {
    #[default]
    Geo,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct GeoIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: GeoIndexType,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

// Text

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TextIndexType {
    #[default]
    Text,
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TokenizerType {
    Prefix,
    Whitespace,
    #[default]
    Word,
    Multilingual,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct TextIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: TextIndexType,

    #[serde(default)]
    pub tokenizer: TokenizerType,

    /// Minimum characters to be tokenized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_token_len: Option<usize>,

    /// Maximum characters to be tokenized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_token_len: Option<usize>,

    /// If true, lowercase all tokens. Default: true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lowercase: Option<bool>,

    /// If true, normalize tokens by folding accented characters to ASCII (e.g., "ação" -> "acao"). Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ascii_folding: Option<bool>,

    /// If true, support phrase matching. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phrase_matching: Option<bool>,

    /// Ignore this set of tokens. Can select from predefined languages and/or provide a custom set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords: Option<StopwordsInterface>,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,

    /// Algorithm for stemming. Default: disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stemmer: Option<StemmingAlgorithm>,
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Snowball {
    #[default]
    Snowball,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
pub struct SnowballParams {
    pub r#type: Snowball,
    pub language: SnowballLanguage,
}

/// Different stemming algorithms with their configs.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(untagged)]
pub enum StemmingAlgorithm {
    Snowball(SnowballParams),
}

/// Languages supported by snowball stemmer.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnowballLanguage {
    #[serde(alias = "ar")]
    Arabic,
    #[serde(alias = "hy")]
    Armenian,
    #[serde(alias = "da")]
    Danish,
    #[serde(alias = "nl")]
    Dutch,
    #[serde(alias = "en")]
    English,
    #[serde(alias = "fi")]
    Finnish,
    #[serde(alias = "fr")]
    French,
    #[serde(alias = "de")]
    German,
    #[serde(alias = "el")]
    Greek,
    #[serde(alias = "hu")]
    Hungarian,
    #[serde(alias = "it")]
    Italian,
    #[serde(alias = "no")]
    Norwegian,
    #[serde(alias = "pt")]
    Portuguese,
    #[serde(alias = "ro")]
    Romanian,
    #[serde(alias = "ru")]
    Russian,
    #[serde(alias = "es")]
    Spanish,
    #[serde(alias = "sv")]
    Swedish,
    #[serde(alias = "ta")]
    Tamil,
    #[serde(alias = "tr")]
    Turkish,
}

impl From<SnowballLanguage> for rust_stemmers::Algorithm {
    fn from(value: SnowballLanguage) -> Self {
        match value {
            SnowballLanguage::Arabic => rust_stemmers::Algorithm::Arabic,
            SnowballLanguage::Armenian => rust_stemmers::Algorithm::Armenian,
            SnowballLanguage::Danish => rust_stemmers::Algorithm::Danish,
            SnowballLanguage::Dutch => rust_stemmers::Algorithm::Dutch,
            SnowballLanguage::English => rust_stemmers::Algorithm::English,
            SnowballLanguage::Finnish => rust_stemmers::Algorithm::Finnish,
            SnowballLanguage::French => rust_stemmers::Algorithm::French,
            SnowballLanguage::German => rust_stemmers::Algorithm::German,
            SnowballLanguage::Greek => rust_stemmers::Algorithm::Greek,
            SnowballLanguage::Hungarian => rust_stemmers::Algorithm::Hungarian,
            SnowballLanguage::Italian => rust_stemmers::Algorithm::Italian,
            SnowballLanguage::Norwegian => rust_stemmers::Algorithm::Norwegian,
            SnowballLanguage::Portuguese => rust_stemmers::Algorithm::Portuguese,
            SnowballLanguage::Romanian => rust_stemmers::Algorithm::Romanian,
            SnowballLanguage::Russian => rust_stemmers::Algorithm::Russian,
            SnowballLanguage::Spanish => rust_stemmers::Algorithm::Spanish,
            SnowballLanguage::Swedish => rust_stemmers::Algorithm::Swedish,
            SnowballLanguage::Tamil => rust_stemmers::Algorithm::Tamil,
            SnowballLanguage::Turkish => rust_stemmers::Algorithm::Turkish,
        }
    }
}

impl fmt::Display for SnowballLanguage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let json_string = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        f.write_str(json_string.trim_matches('"'))
    }
}

impl FromStr for SnowballLanguage {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(&format!("\"{s}\""))
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(untagged)]
pub enum StopwordsInterface {
    Language(Language),
    Set(StopwordsSet),
}

impl StopwordsInterface {
    #[cfg(feature = "testing")]
    pub fn new_custom(custom: &[&str]) -> Self {
        StopwordsInterface::Set(StopwordsSet {
            languages: None,
            custom: Some(custom.iter().map(|s| (*s).to_string()).collect()),
        })
    }

    #[cfg(feature = "testing")]
    pub fn new_language(language: Language) -> Self {
        StopwordsInterface::Language(language)
    }

    #[cfg(feature = "testing")]
    pub fn new_set(languages: &[Language], custom: &[&str]) -> Self {
        StopwordsInterface::Set(StopwordsSet {
            languages: Some(languages.iter().cloned().collect()),
            custom: Some(custom.iter().map(|s| (*s).to_string()).collect()),
        })
    }
}

#[derive(
    Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, PartialOrd, Ord, Hash, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    #[serde(alias = "ar")]
    Arabic,
    #[serde(alias = "az")]
    Azerbaijani,
    #[serde(alias = "eu")]
    Basque,
    #[serde(alias = "bn")]
    Bengali,
    #[serde(alias = "ca")]
    Catalan,
    #[serde(alias = "zh")]
    Chinese,
    #[serde(alias = "da")]
    Danish,
    #[serde(alias = "nl")]
    Dutch,
    #[serde(alias = "en")]
    English,
    #[serde(alias = "fi")]
    Finnish,
    #[serde(alias = "fr")]
    French,
    #[serde(alias = "de")]
    German,
    #[serde(alias = "el")]
    Greek,
    #[serde(alias = "he")]
    Hebrew,
    #[serde(alias = "hi-en")]
    Hinglish,
    #[serde(alias = "hu")]
    Hungarian,
    #[serde(alias = "id")]
    Indonesian,
    #[serde(alias = "it")]
    Italian,
    #[serde(alias = "jp")]
    Japanese,
    #[serde(alias = "kk")]
    Kazakh,
    #[serde(alias = "ne")]
    Nepali,
    #[serde(alias = "no")]
    Norwegian,
    #[serde(alias = "pt")]
    Portuguese,
    #[serde(alias = "ro")]
    Romanian,
    #[serde(alias = "ru")]
    Russian,
    #[serde(alias = "sl")]
    Slovene,
    #[serde(alias = "es")]
    Spanish,
    #[serde(alias = "sv")]
    Swedish,
    #[serde(alias = "tg")]
    Tajik,
    #[serde(alias = "tr")]
    Turkish,
}

impl fmt::Display for Language {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let json_string = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        f.write_str(json_string.trim_matches('"'))
    }
}

impl FromStr for Language {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(&format!("\"{s}\""))
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
pub struct StopwordsSet {
    /// Set of languages to use for stopwords.
    /// Multiple pre-defined lists of stopwords can be combined.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub languages: Option<BTreeSet<Language>>,

    /// Custom stopwords set. Will be merged with the languages set.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<BTreeSet<String>>,
}

// Bool

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BoolIndexType {
    #[default]
    Bool,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct BoolIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: BoolIndexType,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

// Datetime

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DatetimeIndexType {
    #[default]
    Datetime,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DatetimeIndexParams {
    // Required for OpenAPI schema without anonymous types, versus #[serde(tag = "type")]
    pub r#type: DatetimeIndexType,

    /// If true - use this key to organize storage of the collection data.
    /// This option assumes that this key will be used in majority of filtered requests.
    pub is_principal: Option<bool>,

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stopwords_option_language_serialization() {
        let stopwords = StopwordsInterface::Language(Language::English);
        let json = serde_json::to_string(&stopwords).unwrap();
        assert_eq!(json, r#""english""#);

        let deserialized: StopwordsInterface = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, stopwords);
    }

    #[test]
    fn test_stopwords_option_set_serialization() {
        let stopwords = StopwordsInterface::new_set(&[Language::English], &["AAA"]);
        let json = serde_json::to_string(&stopwords).unwrap();
        let expected = r#"{"languages":["english"],"custom":["AAA"]}"#;
        assert_eq!(json, expected);

        let deserialized: StopwordsInterface = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, stopwords);
    }

    #[test]
    fn test_deserialize_stopwords_from_json_examples() {
        let json1 = r#"{"custom": ["as", "the", "a"]}"#;
        let stopwords1: StopwordsInterface = serde_json::from_str(json1).unwrap();

        let expected = StopwordsInterface::new_custom(&["as", "the", "a"]);
        assert_eq!(stopwords1, expected);
        let json2 = r#""english""#;
        let stopwords2: StopwordsInterface = serde_json::from_str(json2).unwrap();
        if let StopwordsInterface::Language(lang) = stopwords2 {
            assert_eq!(lang, Language::English);
        } else {
            panic!("Expected Language");
        }

        // single language
        let json3 = r#"{"languages": ["english"], "custom": ["AAA"]}"#;
        let stopwords3: StopwordsInterface = serde_json::from_str(json3).unwrap();
        let expected = StopwordsInterface::new_set(&[Language::English], &["AAA"]);
        assert_eq!(stopwords3, expected);

        // languages array
        let json4 = r#"{"languages": ["english", "spanish"], "custom": ["AAA"]}"#;
        let stopwords4: StopwordsInterface = serde_json::from_str(json4).unwrap();

        let expected =
            StopwordsInterface::new_set(&[Language::English, Language::Spanish], &["AAA"]);
        assert_eq!(stopwords4, expected);

        // null languages field
        let json5 = r#"{"languages": null, "custom": ["AAA"]}"#;
        let stopwords5: StopwordsInterface = serde_json::from_str(json5).unwrap();

        let expected = StopwordsInterface::new_custom(&["AAA"]);
        assert_eq!(stopwords5, expected);
    }

    #[test]
    fn test_language_aliases() {
        // Test that language aliases work for deserialization
        let json_en = r#""en""#;
        let lang_en: Language = serde_json::from_str(json_en).unwrap();
        assert_eq!(lang_en, Language::English);

        let json_fr = r#""fr""#;
        let lang_fr: Language = serde_json::from_str(json_fr).unwrap();
        assert_eq!(lang_fr, Language::French);

        let json_es = r#""es""#;
        let lang_es: Language = serde_json::from_str(json_es).unwrap();
        assert_eq!(lang_es, Language::Spanish);

        // Test aliases in StopwordsInterface
        let json_interface = r#""en""#;
        let stopwords: StopwordsInterface = serde_json::from_str(json_interface).unwrap();
        if let StopwordsInterface::Language(lang) = stopwords {
            assert_eq!(lang, Language::English);
        } else {
            panic!("Expected Language");
        }

        // Test aliases in StopwordsSet
        let json_set = r#"{"languages": ["en"], "custom": ["AAA"]}"#;
        let stopwords_set: StopwordsInterface = serde_json::from_str(json_set).unwrap();

        let expected = StopwordsInterface::new_set(&[Language::English], &["AAA"]);

        assert_eq!(stopwords_set, expected);

        // languages array
        let json_set_compat = r#"{"languages": ["en", "es"], "custom": ["AAA"]}"#;
        let stopwords_set_compat: StopwordsInterface =
            serde_json::from_str(json_set_compat).unwrap();

        let expected_set =
            StopwordsInterface::new_set(&[Language::English, Language::Spanish], &["AAA"]);

        assert_eq!(stopwords_set_compat, expected_set);
    }

    #[test]
    fn test_unsupported_language_error() {
        // Test that unsupported languages are rejected with a clear error message
        let json_unsupported = r#""klingon""#;
        let result = serde_json::from_str::<Language>(json_unsupported);
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("klingon"),
            "Error message should contain 'klingon', got: {error}",
        );

        let json_interface = r#""klingon""#;
        let result = serde_json::from_str::<StopwordsInterface>(json_interface);
        assert!(result.is_err());

        let json_set = r#"{"languages": "klingon", "custom": ["AAA"]}"#;
        let result = serde_json::from_str::<StopwordsInterface>(json_set);
        assert!(result.is_err());

        // languages array
        let json_set_compat = r#"{"languages": ["english", "klingon"], "custom": ["AAA"]}"#;
        let result = serde_json::from_str::<StopwordsInterface>(json_set_compat);
        assert!(result.is_err());
    }

    #[test]
    fn test_language_field_aliasing() {
        // Test that "languages" key works for deserialization

        // Multiple languages using "languages" key should work
        let json_multiple = r#"{"languages": ["english", "french"], "custom": ["AAA"]}"#;
        let stopwords_multiple: StopwordsInterface = serde_json::from_str(json_multiple).unwrap();
        let expected_set =
            StopwordsInterface::new_set(&[Language::English, Language::French], &["AAA"]);

        assert_eq!(stopwords_multiple, expected_set);
    }
}
