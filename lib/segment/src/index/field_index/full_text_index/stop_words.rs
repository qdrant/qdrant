use std::collections::HashSet;

use super::stop_words_container::{
    ARABIC_STOPWORDS, AZERBAIJANI_STOPWORDS, BASQUE_STOPWORDS, BENGALI_STOPWORDS,
    CATALAN_STOPWORDS, CHINESE_STOPWORDS, DANISH_STOPWORDS, DUTCH_STOPWORDS, ENGLISH_STOPWORDS,
    FINNISH_STOPWORDS, FRENCH_STOPWORDS, GERMAN_STOPWORDS, GREEK_STOPWORDS, HEBREW_STOPWORDS,
    HINGLISH_STOPWORDS, HUNGARIAN_STOPWORDS, INDONESIAN_STOPWORDS, ITALIAN_STOPWORDS,
    KAZAKH_STOPWORDS, NEPALI_STOPWORDS, NORWEGIAN_STOPWORDS, PORTUGUESE_STOPWORDS,
    ROMANIAN_STOPWORDS, RUSSIAN_STOPWORDS, SLOVENE_STOPWORDS, SPANISH_STOPWORDS, SWEDISH_STOPWORDS,
    TAJIK_STOPWORDS, TURKISH_STOPWORDS,
};
use crate::data_types::index::{Language, StopwordsOption};

pub struct StopwordsFilter {
    stopwords: HashSet<String>,
}

impl StopwordsFilter {
    pub fn new(option: &Option<StopwordsOption>) -> Self {
        let mut stopwords = HashSet::new();

        if let Some(option) = option {
            match option {
                StopwordsOption::Language(lang) => {
                    Self::add_language_stopwords(&mut stopwords, lang);
                }
                StopwordsOption::Set(set) => {
                    // A language stopwords
                    for lang in &set.languages {
                        Self::add_language_stopwords(&mut stopwords, lang);
                    }

                    // A custom stopwords
                    for word in &set.custom {
                        stopwords.insert(word.to_lowercase());
                    }
                }
            }
        }

        Self { stopwords }
    }

    /// Check if a token is a stopword
    pub fn is_stopword(&self, token: &str) -> bool {
        self.stopwords.contains(&token.to_lowercase())
    }

    /// Add stopwords for a specific language
    fn add_language_stopwords(stopwords: &mut HashSet<String>, language: &Language) {
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
            stopwords.insert(word.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::index::StopwordsSet;

    #[test]
    fn test_empty_stopwords() {
        let filter = StopwordsFilter::new(&None);
        assert!(!filter.is_stopword("the"));
        assert!(!filter.is_stopword("hello"));
    }

    #[test]
    fn test_language_stopwords() {
        let option = Some(StopwordsOption::Language(Language::English));
        let filter = StopwordsFilter::new(&option);

        assert!(filter.is_stopword("the"));
        assert!(filter.is_stopword("and"));
        assert!(filter.is_stopword("of"));
        assert!(!filter.is_stopword("hello"));
    }

    #[test]
    fn test_custom_stopwords() {
        let option = Some(StopwordsOption::Set(StopwordsSet {
            languages: vec![],
            custom: vec!["hello".to_string(), "world".to_string()],
        }));
        let filter = StopwordsFilter::new(&option);

        assert!(filter.is_stopword("hello"));
        assert!(filter.is_stopword("world"));
        assert!(!filter.is_stopword("the"));
    }

    #[test]
    fn test_mixed_stopwords() {
        let option = Some(StopwordsOption::Set(StopwordsSet {
            languages: vec![Language::English],
            custom: vec!["hello".to_string(), "world".to_string()],
        }));
        let filter = StopwordsFilter::new(&option);

        assert!(filter.is_stopword("hello"));
        assert!(filter.is_stopword("world"));
        assert!(filter.is_stopword("the"));
        assert!(filter.is_stopword("and"));
        assert!(filter.is_stopword("mustn't"));
        assert!(!filter.is_stopword("programming"));
    }

    #[test]
    fn test_case_insensitivity() {
        let option = Some(StopwordsOption::Set(StopwordsSet {
            languages: vec![Language::English],
            custom: vec!["Hello".to_string()],
        }));
        let filter = StopwordsFilter::new(&option);

        assert!(filter.is_stopword("hello"));
        assert!(filter.is_stopword("Hello"));
        assert!(filter.is_stopword("HELLO"));
        assert!(filter.is_stopword("The"));
        assert!(filter.is_stopword("THE"));
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
            let option = Some(StopwordsOption::Language(language.clone()));
            let filter = StopwordsFilter::new(&option);
            assert!(
                filter.is_stopword(stopword),
                "Expected '{}' to be a stopword in {:?}",
                stopword,
                language
            );
            assert!(
                !filter.is_stopword("qdrant"),
                "Expected 'qdrant' not to be a stopword"
            );
        }
    }
}
