//! Soundness of the trigram query: a key the regex matches always satisfies
//! the query derived from it.

use std::collections::HashSet;

use proptest::prelude::*;

use super::matcher::RegexMatcher;
use super::trigram::key_trigrams;

/// Characters picked to exercise folding: case pairs, the Kelvin sign
/// (folds to ASCII `k`), dotted capital I (folds to two chars), sigma forms,
/// multi-byte and control chars.
const ALPHABET: &[char] = &[
    'a', 'b', 'c', 'k', 'A', 'B', 'K', '\u{212A}', 'İ', 'i', 'σ', 'ς', 'Σ', 'é', 'É', '_', '1',
    '\0',
];

fn assert_sound(pattern: &str, keys: &[String]) {
    let Ok(matcher) = RegexMatcher::new(pattern) else {
        return;
    };
    for key in keys {
        if matcher.is_match(key) {
            let trigrams: HashSet<_> = key_trigrams(key).into_iter().collect();
            assert!(
                matcher
                    .query()
                    .matches(&|trigram| trigrams.contains(&trigram)),
                "pattern {pattern:?} matches key {key:?}, but its query {:?} rejects it",
                matcher.query(),
            );
        }
    }
}

fn key_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(ALPHABET), 0..8)
        .prop_map(|chars| chars.into_iter().collect())
}

fn pattern_strategy() -> impl Strategy<Value = String> {
    let atom = prop_oneof![
        prop::sample::select(ALPHABET).prop_map(|c| regex::escape(&c.to_string())),
        prop::collection::vec(prop::sample::select(ALPHABET), 2..5)
            .prop_map(|chars| regex::escape(&chars.into_iter().collect::<String>())),
        prop::sample::select(vec![
            ".",
            "[ab]",
            "[a-c]",
            "[^a]",
            "[kK]",
            r"\d",
            r"\w",
            r"\W",
            "^",
            "$",
            r"\b",
            r"\B",
            r"\A",
            r"\z",
            "(?m:^)",
            "(?m:$)",
            r"\D",
            r"\pL",
            r"\p{Greek}",
            "[[:alpha:]]",
            "",
        ])
        .prop_map(String::from),
    ];
    atom.prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 2..4).prop_map(|parts| parts.concat()),
            prop::collection::vec(inner.clone(), 2..4)
                .prop_map(|parts| format!("(?:{})", parts.join("|"))),
            (
                inner.clone(),
                prop::sample::select(vec![
                    "?", "*", "+", "{2}", "{3}", "{1,2}", "{2,}", "{0,3}", "??", "+?",
                ])
            )
                .prop_map(|(sub, rep)| format!("(?:{sub}){rep}")),
            inner.clone().prop_map(|sub| format!("(?i:{sub})")),
            inner.prop_map(|sub| format!("({sub})")),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn query_admits_every_matching_key(
        pattern in pattern_strategy(),
        keys in prop::collection::vec(key_strategy(), 64),
    ) {
        assert_sound(&pattern, &keys);
    }

    /// Keys built from the pattern's own literal pieces match far more often
    /// than random ones.
    #[test]
    fn query_admits_keys_close_to_the_pattern(
        pieces in prop::collection::vec(
            prop::collection::vec(prop::sample::select(ALPHABET), 1..4)
                .prop_map(|chars| chars.into_iter().collect::<String>()),
            1..4,
        ),
        glue in prop::sample::select(vec!["", ".", ".*", "?", "|", "[a-c]*", r"\w", "$", "^"]),
        case_insensitive in any::<bool>(),
        affixes in prop::collection::vec(key_strategy(), 2),
    ) {
        let body = pieces.iter().map(|piece| regex::escape(piece)).collect::<Vec<_>>().join(glue);
        let pattern = if case_insensitive { format!("(?i){body}") } else { body };
        let joined = pieces.concat();
        let keys: Vec<String> = [
            joined.clone(),
            format!("{}{joined}", affixes[0]),
            format!("{joined}{}", affixes[1]),
            format!("{}{joined}{}", affixes[0], affixes[1]),
            joined.to_uppercase(),
            joined.to_lowercase(),
            pieces.join("_"),
        ]
        .into_iter()
        .collect();
        assert_sound(&pattern, &keys);
    }
}

#[test]
fn query_admits_tricky_keys() {
    let cases: &[(&str, &[&str])] = &[
        // Repeated and adjacent anchors are zero-width.
        ("^^abc", &["abcd"]),
        (r"^\Aabc$$", &["abc"]),
        ("^$", &[""]),
        ("(?:^|_)abc", &["abc", "x_abc"]),
        // Folding: Kelvin sign, dotted I, final sigma, accents.
        ("(?i)kelvin", &["\u{212A}ELVIN", "Kelvin"]),
        ("\u{212A}elvin", &["\u{212A}elvin"]),
        ("İstanbul", &["İstanbul"]),
        ("(?i)istanbul", &["ISTANBUL"]),
        ("ΟΔΟΣ", &["ΟΔΟΣ", "ΟΔΟΣ_1"]),
        ("(?i)οδος", &["ΟΔΟΣ", "οδος", "οδοσ"]),
        ("(?i)été", &["ÉTÉ"]),
        // ASCII-only Perl classes and word boundaries (RE2 semantics).
        (r"\wbc", &["abc", "Ébc_bc"]),
        (r"caf\b", &["café"]),
        // Line anchors and word boundaries don't touch the key boundary.
        ("(?m)^abc", &["x\nabc"]),
        (r"abc\b", &["x abc y"]),
        // Repetitions.
        ("(?:abc){2,}", &["abcabcabc"]),
        ("a{3}b", &["aaab", "xaaaab"]),
        ("colou?r", &["color", "colour"]),
    ];
    for (pattern, keys) in cases {
        let keys: Vec<String> = keys.iter().map(|key| key.to_string()).collect();
        let matcher = RegexMatcher::new(pattern).unwrap();
        for key in &keys {
            assert!(matcher.is_match(key), "{pattern:?} should match {key:?}");
        }
        assert_sound(pattern, &keys);
    }
}
