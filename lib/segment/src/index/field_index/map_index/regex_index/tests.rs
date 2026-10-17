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

/// Key selection of the map index variants, against a scan of the data.
mod map_index {
    use std::collections::BTreeSet;

    use common::ambient;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFs};
    use ecow::EcoString;
    use serde_json::Value;
    use tempfile::TempDir;

    use super::super::{RegexMatcher, StrMapIndexRegexRead};
    use crate::index::field_index::map_index::MapIndex;
    use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
    use crate::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};

    pub(super) const PATTERNS: &[&str] = &[
        "Einstein",
        "(?i)einstein",
        "^Albert_",
        r"_\(film\)$",
        "colou?r",
        "(?i)^colou?r",
        "_[0-9]{2}_",
        "^.{4}$",
        "москва",
        "(?i)МОСКВА",
        "zzz",
        "",
    ];

    pub(super) fn data() -> Vec<Vec<String>> {
        let words = [
            "Albert_Einstein",
            "Einstein_(film)",
            "Colour",
            "color_scheme",
            "Москва",
            "abcd",
        ];
        (0..200)
            .map(|i| match i % 7 {
                // Points without values.
                0 => vec![],
                1 => vec![words[i % words.len()].to_string()],
                2 => vec![format!("item_{i:02}_{}", words[i % words.len()])],
                _ => vec![
                    words[(i + 1) % words.len()].to_string(),
                    format!("{}_{i}", words[i % words.len()]),
                ],
            })
            .collect()
    }

    pub(super) fn expected_keys(data: &[Vec<String>], pattern: &str) -> BTreeSet<String> {
        let matcher = RegexMatcher::new(pattern).unwrap();
        data.iter()
            .flatten()
            .filter(|key| matcher.is_match(key))
            .cloned()
            .collect()
    }

    pub(super) fn check(index: &impl StrMapIndexRegexRead, data: &[Vec<String>]) {
        check_with_live(index, data, data);
    }

    /// `data` holds every value ever indexed (keys are never dropped), `live`
    /// the values of the points not deleted since.
    pub(super) fn check_with_live(
        index: &impl StrMapIndexRegexRead,
        data: &[Vec<String>],
        live: &[Vec<String>],
    ) {
        for pattern in PATTERNS {
            let matcher = RegexMatcher::new(pattern).unwrap();
            let keys: BTreeSet<String> = index
                .regex_keys(&matcher)
                .unwrap()
                .expect("regex index is enabled")
                .into_iter()
                .map(|key: EcoString| key.to_string())
                .collect();
            let expected = expected_keys(data, pattern);
            assert_eq!(keys, expected, "{pattern}");

            if let Some(stats) = index.regex_candidate_stats(matcher.query()).unwrap() {
                let postings = live
                    .iter()
                    .flatten()
                    .filter(|key| expected.contains(*key))
                    .count();
                assert!(stats.keys >= expected.len(), "{pattern}: {stats:?}");
                assert!(stats.postings >= postings, "{pattern}: {stats:?}");
            }
        }
    }

    pub(super) fn build_mutable(dir: &TempDir, data: &[Vec<String>]) {
        let _scope = ambient::test_guard();
        let mut builder = MapIndex::<str>::builder_mutable(dir.path().to_path_buf(), false);
        builder.init().unwrap();
        for (idx, values) in data.iter().enumerate() {
            let values: Vec<Value> = values.iter().map(|v| Value::from(v.as_str())).collect();
            let values: Vec<&Value> = values.iter().collect();
            builder.add_point(idx as PointOffsetType, &values).unwrap();
        }
        builder.finalize().unwrap();
    }

    #[test]
    fn mutable_index_selects_matching_keys() {
        let dir = TempDir::with_prefix("regex_mutable").unwrap();
        let mut data = data();
        build_mutable(&dir, &data);

        let mut index = MapIndex::<str>::new_mutable(dir.path().to_path_buf(), false, false)
            .unwrap()
            .unwrap();
        let MapIndex::Mutable(mutable) = &mut index else {
            panic!("expected the mutable variant");
        };
        assert!(
            mutable
                .regex_keys(&RegexMatcher::new("a").unwrap())
                .unwrap()
                .is_none()
        );
        mutable.enable_regex_index();
        check(&index, &data);

        // Keys added after enabling are indexed too.
        let _scope = ambient::test_guard();
        let new_values = ["Einstein_ring", "colour_wheel", "москва_река"];
        let values: Vec<Value> = new_values.iter().map(|v| Value::from(*v)).collect();
        let values: Vec<&Value> = values.iter().collect();
        index
            .add_point(data.len() as PointOffsetType, &values)
            .unwrap();
        data.push(new_values.iter().map(|v| v.to_string()).collect());
        check(&index, &data);
    }

    #[test]
    fn deleted_points_keep_their_keys_as_candidates() {
        let dir = TempDir::with_prefix("regex_mutable_deleted").unwrap();
        let data = data();
        build_mutable(&dir, &data);

        let mut index = MapIndex::<str>::new_mutable(dir.path().to_path_buf(), false, false)
            .unwrap()
            .unwrap();
        let MapIndex::Mutable(mutable) = &mut index else {
            panic!("expected the mutable variant");
        };
        mutable.enable_regex_index();
        for idx in 0..data.len() {
            index.remove_point(idx as PointOffsetType).unwrap();
        }

        // Keys stay (they have no points left); selection still equals a scan
        // over every key ever indexed.
        check_with_live(&index, &data, &[]);
        let matcher = RegexMatcher::new("Einstein").unwrap();
        let stats = index
            .regex_candidate_stats(matcher.query())
            .unwrap()
            .unwrap();
        assert!(stats.keys > 0);
        assert_eq!(stats.postings, 0);
    }

    #[test]
    fn read_only_appendable_index_selects_matching_keys() {
        let dir = TempDir::with_prefix("regex_read_only_appendable").unwrap();
        let data = data();
        build_mutable(&dir, &data);

        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();
        let mut index: ReadOnlyMapIndex<str, ReadOnly<MmapFile>> =
            ReadOnlyMapIndex::open_appendable(&fs, dir.path().to_path_buf(), PointOffsetType::MAX)
                .unwrap()
                .unwrap();
        let ReadOnlyMapIndex::Appendable(appendable) = &mut index else {
            panic!("expected the appendable variant");
        };
        appendable.enable_regex_index();
        check(&index, &data);
    }
}
