use segment::payload_storage::condition_checker::ValueChecker;
use segment::types::{Match, MatchPhrase, MatchSubstring, MatchText, MatchTextAny};
use serde_json::Value;

/// Regression for <https://github.com/qdrant/qdrant/issues/10182>
#[test]
fn test_unindexed_text_uses_token_matching_not_substring() {
    let text_match = |query: &str, stored: &str| {
        Match::Text(MatchText {
            text: query.to_string(),
        })
        .check_match(&Value::String(stored.to_string()))
    };

    // "good" is a substring of "goodness" but not a whole token
    assert!(!text_match("good", "goodness only"));
    assert!(text_match("good", "good cheap stuff"));
    assert!(text_match("good cheap", "cheap hardware good"));
    assert!(!text_match("good cheap", "cheap hardware"));
}

/// Regression for <https://github.com/qdrant/qdrant/issues/10182>
#[test]
fn test_unindexed_phrase_requires_token_order() {
    let phrase_match = |phrase: &str, stored: &str| {
        Match::Phrase(MatchPhrase {
            phrase: phrase.to_string(),
        })
        .check_match(&Value::String(stored.to_string()))
    };

    assert!(phrase_match("alpha beta", "foo alpha beta bar"));
    assert!(!phrase_match("alpha beta", "beta alpha"));
    assert!(!phrase_match("alpha beta", "alphabeta"));
    assert!(!phrase_match("good", "goodness only"));
    assert!(phrase_match("good", "goodness only good"));
}

/// Regression for <https://github.com/qdrant/qdrant/issues/10526>
#[test]
fn test_unindexed_text_any_uses_token_matching_not_substring() {
    let text_any_match = |query: &str, stored: &str| {
        Match::TextAny(MatchTextAny {
            text_any: query.to_string(),
        })
        .check_match(&Value::String(stored.to_string()))
    };

    // "good" is a substring of "goodness" but not a whole token
    assert!(!text_any_match("good", "goodness only"));
    assert!(text_any_match("good", "good cheap stuff"));
    // any token is enough (unlike Match::Text which requires all)
    assert!(text_any_match("good cheap", "cheap hardware"));
    assert!(!text_any_match("good cheap", "neutral text"));
    // case-insensitive via default Word tokenizer (lowercase on)
    assert!(text_any_match("hello", "Hello, world!"));
}

/// `substring` keeps the pre-token-aware behaviour of unindexed text
/// matching: a case-sensitive substring test on the raw value.
#[test]
fn test_unindexed_substring_is_case_sensitive_substring() {
    let substring_match = |query: &str, stored: &str| {
        Match::Substring(MatchSubstring {
            substring: query.to_string(),
        })
        .check_match(&Value::String(stored.to_string()))
    };

    assert!(substring_match("good", "goodness only"));
    assert!(substring_match("od che", "good cheap stuff"));
    assert!(!substring_match("good cheap", "cheap hardware"));
    assert!(!substring_match("hello", "Hello, world!"));
    assert!(substring_match("", "anything"));
    assert!(!substring_match("x", ""));

    // Non-string payload values never match.
    let m = Match::Substring(MatchSubstring {
        substring: "1".to_string(),
    });
    assert!(!m.check_match(&Value::Number(1.into())));
    assert!(!m.check_match(&Value::Null));
}
