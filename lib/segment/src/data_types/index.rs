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
    pub lookup: Option<bool>,

    /// If true - support ranges filters.
    pub range: Option<bool>,

    /// If true - use this key to organize storage of the collection data.
    /// This option assumes that this key will be used in majority of filtered requests.
    pub is_principal: Option<bool>,

    /// If true, store the index on disk. Default: false.
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

    /// If true, store the index on disk. Default: false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,

    // todo(phrase_match): remove skip
    #[serde(skip)]
    pub phrase_matching: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords: Option<StopwordsInterface>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(untagged)]
pub enum StopwordsInterface {
    Language(Language),
    Set(StopwordsSet),
}

#[derive(Debug, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    #[serde(alias = "unspecified")]
    UnspecifiedLanguage,
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

impl<'de> serde::Deserialize<'de> for Language {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let lowercase_s = s.to_lowercase();

        match lowercase_s.as_str() {
            "unspecified" | "unspecifiedlanguage" => Ok(Language::UnspecifiedLanguage),
            "ar" | "arabic" => Ok(Language::Arabic),
            "az" | "azerbaijani" => Ok(Language::Azerbaijani),
            "eu" | "basque" => Ok(Language::Basque),
            "bn" | "bengali" => Ok(Language::Bengali),
            "ca" | "catalan" => Ok(Language::Catalan),
            "zh" | "chinese" => Ok(Language::Chinese),
            "da" | "danish" => Ok(Language::Danish),
            "nl" | "dutch" => Ok(Language::Dutch),
            "en" | "english" => Ok(Language::English),
            "fi" | "finnish" => Ok(Language::Finnish),
            "fr" | "french" => Ok(Language::French),
            "de" | "german" => Ok(Language::German),
            "el" | "greek" => Ok(Language::Greek),
            "he" | "hebrew" => Ok(Language::Hebrew),
            "hi-en" | "hinglish" => Ok(Language::Hinglish),
            "hu" | "hungarian" => Ok(Language::Hungarian),
            "id" | "indonesian" => Ok(Language::Indonesian),
            "it" | "italian" => Ok(Language::Italian),
            "kk" | "kazakh" => Ok(Language::Kazakh),
            "ne" | "nepali" => Ok(Language::Nepali),
            "no" | "norwegian" => Ok(Language::Norwegian),
            "pt" | "portuguese" => Ok(Language::Portuguese),
            "ro" | "romanian" => Ok(Language::Romanian),
            "ru" | "russian" => Ok(Language::Russian),
            "sl" | "slovene" => Ok(Language::Slovene),
            "es" | "spanish" => Ok(Language::Spanish),
            "sv" | "swedish" => Ok(Language::Swedish),
            "tg" | "tajik" => Ok(Language::Tajik),
            "tr" | "turkish" => Ok(Language::Turkish),
            _ => Err(serde::de::Error::custom(format!(
                "Unsupported language: {s}. Please use one of the supported languages.",
            ))),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
pub struct StopwordsSet {
    #[serde(default)]
    pub languages: Vec<Language>,

    #[serde(default)]
    pub custom: Vec<String>,
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
        let stopwords = StopwordsInterface::Set(StopwordsSet {
            languages: vec![Language::English, Language::Spanish],
            custom: vec!["AAA".to_string()],
        });
        let json = serde_json::to_string(&stopwords).unwrap();
        let expected = r#"{"languages":["english","spanish"],"custom":["AAA"]}"#;
        assert_eq!(json, expected);

        let deserialized: StopwordsInterface = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, stopwords);
    }

    #[test]
    fn test_deserialize_stopwords_from_json_examples() {
        let json1 = r#"{"custom": ["as", "the", "a"]}"#;
        let stopwords1: StopwordsInterface = serde_json::from_str(json1).unwrap();
        if let StopwordsInterface::Set(set) = stopwords1 {
            assert_eq!(set.custom, vec!["as", "the", "a"]);
            assert_eq!(set.languages, Vec::<Language>::new());
        } else {
            panic!("Expected StopwordsSet");
        }

        let json2 = r#""english""#;
        let stopwords2: StopwordsInterface = serde_json::from_str(json2).unwrap();
        if let StopwordsInterface::Language(lang) = stopwords2 {
            assert_eq!(lang, Language::English);
        } else {
            panic!("Expected Language");
        }

        let json3 = r#"{"languages": ["english", "spanish"], "custom": ["AAA"]}"#;
        let stopwords3: StopwordsInterface = serde_json::from_str(json3).unwrap();
        if let StopwordsInterface::Set(set) = stopwords3 {
            assert_eq!(set.languages, vec![Language::English, Language::Spanish]);
            assert_eq!(set.custom, vec!["AAA"]);
        } else {
            panic!("Expected StopwordsSet");
        }
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
        let json_set = r#"{"languages": ["en", "es"], "custom": ["AAA"]}"#;
        let stopwords_set: StopwordsInterface = serde_json::from_str(json_set).unwrap();
        if let StopwordsInterface::Set(set) = stopwords_set {
            assert_eq!(set.languages, vec![Language::English, Language::Spanish]);
            assert_eq!(set.custom, vec!["AAA"]);
        } else {
            panic!("Expected StopwordsSet");
        }
    }

    #[test]
    fn test_unsupported_language_error() {
        // Test that unsupported languages are rejected with a clear error message
        let json_unsupported = r#""klingon""#;
        let result = serde_json::from_str::<Language>(json_unsupported);
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("Unsupported language: klingon"),
            "Error message should contain 'Unsupported language: klingon', got: {error}",
        );

        let json_interface = r#""klingon""#;
        let result = serde_json::from_str::<StopwordsInterface>(json_interface);
        assert!(result.is_err());

        let json_set = r#"{"languages": ["english", "klingon"], "custom": ["AAA"]}"#;
        let result = serde_json::from_str::<StopwordsInterface>(json_set);
        assert!(result.is_err());
    }
}
