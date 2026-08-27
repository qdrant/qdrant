use segment::payload_storage::condition_checker::ValueChecker;
use segment::types::{Match, MatchPhrase, MatchText};
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
