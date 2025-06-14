use ahash::AHashSet;

use crate::data_types::index::{Language, StopwordsInterface};

pub mod arabic;
pub mod azerbaijani;
pub mod basque;
pub mod bengali;
pub mod catalan;
pub mod chinese;
pub mod danish;
pub mod dutch;
pub mod english;
pub mod finnish;
pub mod french;
pub mod german;
pub mod greek;
pub mod hebrew;
pub mod hinglish;
pub mod hungarian;
pub mod indonesian;
pub mod italian;
pub mod kazakh;
pub mod nepali;
pub mod norwegian;
pub mod portuguese;
pub mod romanian;
pub mod russian;
pub mod slovene;
pub mod spanish;
pub mod swedish;
pub mod tajik;
pub mod turkish;

pub use arabic::ARABIC_STOPWORDS;
pub use azerbaijani::AZERBAIJANI_STOPWORDS;
pub use basque::BASQUE_STOPWORDS;
pub use bengali::BENGALI_STOPWORDS;
pub use catalan::CATALAN_STOPWORDS;
pub use chinese::CHINESE_STOPWORDS;
pub use danish::DANISH_STOPWORDS;
pub use dutch::DUTCH_STOPWORDS;
pub use english::ENGLISH_STOPWORDS;
pub use finnish::FINNISH_STOPWORDS;
pub use french::FRENCH_STOPWORDS;
pub use german::GERMAN_STOPWORDS;
pub use greek::GREEK_STOPWORDS;
pub use hebrew::HEBREW_STOPWORDS;
pub use hinglish::HINGLISH_STOPWORDS;
pub use hungarian::HUNGARIAN_STOPWORDS;
pub use indonesian::INDONESIAN_STOPWORDS;
pub use italian::ITALIAN_STOPWORDS;
pub use kazakh::KAZAKH_STOPWORDS;
pub use nepali::NEPALI_STOPWORDS;
pub use norwegian::NORWEGIAN_STOPWORDS;
pub use portuguese::PORTUGUESE_STOPWORDS;
pub use romanian::ROMANIAN_STOPWORDS;
pub use russian::RUSSIAN_STOPWORDS;
pub use slovene::SLOVENE_STOPWORDS;
pub use spanish::SPANISH_STOPWORDS;
pub use swedish::SWEDISH_STOPWORDS;
pub use tajik::TAJIK_STOPWORDS;
pub use turkish::TURKISH_STOPWORDS;

#[derive(Debug, Clone, Default)]
pub struct StopwordsFilter {
    stopwords: AHashSet<String>,
}

impl StopwordsFilter {
    pub fn new(option: &Option<StopwordsInterface>, lowercase: bool) -> Self {
        let mut stopwords = AHashSet::new();

        if let Some(option) = option {
            match option {
                StopwordsInterface::Language(lang) => {
                    Self::add_language_stopwords(&mut stopwords, lang, lowercase);
                }
                StopwordsInterface::Set(set) => {
                    // Add stopwords from all languages in the languages field
                    if let Some(languages) = set.languages.as_ref() {
                        // If languages are provided, add their stopwords
                        for lang in languages {
                            Self::add_language_stopwords(&mut stopwords, lang, lowercase);
                        }
                    }

                    if let Some(custom) = set.custom.as_ref() {
                        // If custom stopwords are provided, add them
                        for word in custom {
                            if lowercase {
                                stopwords.insert(word.to_lowercase());
                            } else {
                                stopwords.insert(word.clone());
                            }
                        }
                    }
                }
            }
        }

        Self { stopwords }
    }

    /// Check if a token is a stopword
    pub fn is_stopword(&self, token: &str) -> bool {
        self.stopwords.contains(token)
    }

    /// Add stopwords for a specific language
    fn add_language_stopwords(
        stopwords: &mut AHashSet<String>,
        language: &Language,
        lowercase: bool,
    ) {
        let stopwords_array = match language {
            Language::Arabic => ARABIC_STOPWORDS,
            Language::Azerbaijani => AZERBAIJANI_STOPWORDS,
            Language::Basque => BASQUE_STOPWORDS,
            Language::Bengali => BENGALI_STOPWORDS,
            Language::Catalan => CATALAN_STOPWORDS,
            Language::Chinese => CHINESE_STOPWORDS,
            Language::Danish => DANISH_STOPWORDS,
            Language::Dutch => DUTCH_STOPWORDS,
            Language::English => ENGLISH_STOPWORDS,
            Language::Finnish => FINNISH_STOPWORDS,
            Language::French => FRENCH_STOPWORDS,
            Language::German => GERMAN_STOPWORDS,
            Language::Greek => GREEK_STOPWORDS,
            Language::Hebrew => HEBREW_STOPWORDS,
            Language::Hinglish => HINGLISH_STOPWORDS,
            Language::Hungarian => HUNGARIAN_STOPWORDS,
            Language::Indonesian => INDONESIAN_STOPWORDS,
            Language::Italian => ITALIAN_STOPWORDS,
            Language::Kazakh => KAZAKH_STOPWORDS,
            Language::Nepali => NEPALI_STOPWORDS,
            Language::Norwegian => NORWEGIAN_STOPWORDS,
            Language::Portuguese => PORTUGUESE_STOPWORDS,
            Language::Romanian => ROMANIAN_STOPWORDS,
            Language::Russian => RUSSIAN_STOPWORDS,
            Language::Slovene => SLOVENE_STOPWORDS,
            Language::Spanish => SPANISH_STOPWORDS,
            Language::Swedish => SWEDISH_STOPWORDS,
            Language::Tajik => TAJIK_STOPWORDS,
            Language::Turkish => TURKISH_STOPWORDS,
        };

        for &word in stopwords_array {
            if lowercase {
                stopwords.insert(word.to_lowercase());
            } else {
                stopwords.insert(word.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_stopwords() {
        let filter = StopwordsFilter::new(&None, true);
        assert!(!filter.is_stopword("the"));
        assert!(!filter.is_stopword("hello"));
    }

    #[test]
    fn test_language_stopwords() {
        let option = Some(StopwordsInterface::Language(Language::English));
        let filter = StopwordsFilter::new(&option, true);

        assert!(filter.is_stopword("the"));
        assert!(filter.is_stopword("and"));
        assert!(filter.is_stopword("of"));
        assert!(!filter.is_stopword("hello"));
    }

    #[test]
    fn test_custom_stopwords() {
        let option = Some(StopwordsInterface::new_custom(&["hello", "world"]));
        let filter = StopwordsFilter::new(&option, true);

        assert!(filter.is_stopword("hello"));
        assert!(filter.is_stopword("world"));
        assert!(!filter.is_stopword("the"));
    }

    #[test]
    fn test_mixed_stopwords() {
        let option = Some(StopwordsInterface::new_set(
            &[Language::English],
            &["hello", "world"],
        ));
        let filter = StopwordsFilter::new(&option, true);

        assert!(filter.is_stopword("hello"));
        assert!(filter.is_stopword("world"));
        assert!(filter.is_stopword("the"));
        assert!(filter.is_stopword("and"));
        assert!(filter.is_stopword("mustn't"));
        assert!(!filter.is_stopword("programming"));
    }

    #[test]
    fn test_case_sensitivity() {
        let option = Some(StopwordsInterface::new_custom(&["Hello", "World"]));
        let filter = StopwordsFilter::new(&option, false);

        // Should match exact case
        assert!(filter.is_stopword("Hello"));
        assert!(filter.is_stopword("World"));

        // Should not match different case
        assert!(!filter.is_stopword("hello"));
        assert!(!filter.is_stopword("HELLO"));
        assert!(!filter.is_stopword("world"));
        assert!(!filter.is_stopword("WORLD"));
    }

    #[test]
    fn test_language_stopwords_case_sensitivity() {
        let option = Some(StopwordsInterface::Language(Language::English));
        let filter = StopwordsFilter::new(&option, false);

        // English stopwords are typically lowercase in the source arrays
        assert!(filter.is_stopword("the"));
        assert!(filter.is_stopword("and"));

        // Should not match uppercase versions
        assert!(!filter.is_stopword("The"));
        assert!(!filter.is_stopword("AND"));
    }

    #[test]
    fn test_all_languages_stopwords() {
        // Test a common stopword for each language
        let languages = vec![
            (Language::Arabic, "في"),
            (Language::Azerbaijani, "və"),
            (Language::Basque, "eta"),
            (Language::Bengali, "এবং"),
            (Language::Catalan, "i"),
            (Language::Chinese, "的"),
            (Language::Danish, "og"),
            (Language::Dutch, "en"),
            (Language::English, "and"),
            (Language::Finnish, "ja"),
            (Language::French, "et"),
            (Language::German, "und"),
            (Language::Greek, "και"),
            (Language::Hebrew, "את"),
            (Language::Hinglish, "aur"),
            (Language::Hungarian, "és"),
            (Language::Indonesian, "dan"),
            (Language::Italian, "e"),
            (Language::Kazakh, "жоқ"),
            (Language::Nepali, "र"),
            (Language::Norwegian, "og"),
            (Language::Portuguese, "e"),
            (Language::Romanian, "ar"),
            (Language::Russian, "и"),
            (Language::Slovene, "in"),
            (Language::Spanish, "y"),
            (Language::Swedish, "och"),
            (Language::Tajik, "ва"),
            (Language::Turkish, "ve"),
        ];

        for (language, stopword) in languages {
            let option = Some(StopwordsInterface::Language(language.clone()));
            let filter = StopwordsFilter::new(&option, true);
            assert!(
                filter.is_stopword(stopword),
                "Expected '{stopword}' to be a stopword in {language:?}"
            );
            assert!(
                !filter.is_stopword("qdrant"),
                "Expected 'qdrant' not to be a stopword"
            );
        }
    }
}
