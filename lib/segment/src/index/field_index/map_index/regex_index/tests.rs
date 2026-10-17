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
        let _scope = ambient::test_guard();
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

/// The regex index file, against a scan of its keys.
mod file {
    use std::collections::BTreeSet;

    use common::ambient;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use tempfile::TempDir;

    use super::super::{RegexIndex, RegexMatcher, build_regex_index};

    /// Enough keys for many key blocks.
    fn keys() -> Vec<String> {
        let words = [
            "alpha",
            "Beta",
            "gamma_ray",
            "Δέλτα",
            "colour",
            "color",
            "x",
        ];
        let mut keys: Vec<String> = (0..5000)
            .map(|i| {
                format!(
                    "{}_{i}_{}",
                    words[i % words.len()],
                    words[(i / words.len()) % words.len()],
                )
            })
            .chain(["".to_string(), "a".to_string(), "ab".to_string()])
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    fn build(dir: &TempDir, keys: &[String]) -> RegexIndex<MmapFile> {
        let _scope = ambient::test_guard();
        build_regex_index(
            dir.path(),
            keys.iter()
                .enumerate()
                .map(|(i, key)| (key.as_str(), i % 3 + 1)),
        )
        .unwrap();
        RegexIndex::open(&MmapFs, dir.path(), Populate::No)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn matching_keys_equal_a_scan() {
        let _scope = ambient::test_guard();
        let dir = TempDir::with_prefix("regex_index_file").unwrap();
        let keys = keys();
        let index = build(&dir, &keys);
        assert_eq!(index.key_count(), keys.len());

        for pattern in [
            "alpha",
            "(?i)^beta_1",
            "_42_",
            "colou?r$",
            "(?i)ΔΈΛΤΑ",
            "ray_4999",
            "^a$",
            "^$",
            "^ab",
            r"_\d{4}_",
            ".",
            "nope",
        ] {
            let matcher = RegexMatcher::new(pattern).unwrap();
            let expected: Vec<String> = keys
                .iter()
                .filter(|key| matcher.is_match(key))
                .cloned()
                .collect();
            let actual: Vec<String> = index
                .matching_keys(&matcher)
                .unwrap()
                .into_iter()
                .map(|key| key.to_string())
                .collect();
            assert_eq!(actual, expected, "{pattern}");

            // Bounds hold, with every key carrying `i % 3 + 1` points.
            if let Some(stats) = index.candidate_stats(matcher.query()).unwrap() {
                let expected_points: usize = keys
                    .iter()
                    .enumerate()
                    .filter(|(_, key)| matcher.is_match(key))
                    .map(|(i, _)| i % 3 + 1)
                    .sum();
                assert!(stats.keys >= expected.len(), "{pattern}: {stats:?}");
                assert!(stats.postings >= expected_points, "{pattern}: {stats:?}");
            }
        }
    }

    #[test]
    fn empty_index() {
        let _scope = ambient::test_guard();
        let dir = TempDir::with_prefix("regex_index_empty").unwrap();
        let index = build(&dir, &[]);
        for pattern in ["abc", "."] {
            let matcher = RegexMatcher::new(pattern).unwrap();
            assert!(index.matching_keys(&matcher).unwrap().is_empty());
        }
    }

    #[test]
    fn missing_file_is_no_index() {
        let dir = TempDir::with_prefix("regex_index_missing").unwrap();
        assert!(
            RegexIndex::<MmapFile>::open(&MmapFs, dir.path(), Populate::No)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn selective_queries_read_few_blocks() {
        let _scope = ambient::test_guard();
        let dir = TempDir::with_prefix("regex_index_selective").unwrap();
        let keys = keys();
        let index = build(&dir, &keys);
        let matcher = RegexMatcher::new("_4998_").unwrap();
        let stats = index.candidate_stats(matcher.query()).unwrap().unwrap();
        assert!(stats.keys < 10, "{stats:?}");
        let found: BTreeSet<_> = index.matching_keys(&matcher).unwrap().into_iter().collect();
        assert_eq!(found.len(), 1);
    }
}

/// The immutable variants: built with the regex index file, opened in RAM
/// and on disk, writable and read-only.
mod immutable_map_index {
    use common::ambient;
    use common::bitvec::BitVec;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFs};
    use rstest::rstest;
    use serde_json::Value;
    use tempfile::TempDir;

    use super::super::{REGEX_INDEX_PATH, RegexMatcher, StrMapIndexRegexRead};
    use super::map_index::{check, data};
    use crate::index::field_index::FieldIndexBuilderTrait;
    use crate::index::field_index::map_index::MapIndex;
    use crate::index::field_index::map_index::read_only::ReadOnlyMapIndex;
    use crate::types::Memory;

    fn deleted() -> BitVec {
        BitVec::repeat(false, 1024)
    }

    fn build(dir: &TempDir, data: &[Vec<String>], regex: bool) {
        let _scope = ambient::test_guard();
        let mut builder =
            MapIndex::<str>::builder_immutable(dir.path(), Memory::Pinned, &deleted(), false);
        if regex {
            builder.enable_regex_index();
        }
        builder.init().unwrap();
        for (idx, values) in data.iter().enumerate() {
            let values: Vec<Value> = values.iter().map(|v| Value::from(v.as_str())).collect();
            let values: Vec<&Value> = values.iter().collect();
            builder.add_point(idx as PointOffsetType, &values).unwrap();
        }
        builder.finalize().unwrap();
    }

    #[rstest]
    #[case::in_ram(Memory::Pinned)]
    #[case::on_disk(Memory::Cold)]
    fn selects_matching_keys(#[case] memory: Memory) {
        let dir = TempDir::with_prefix("regex_immutable").unwrap();
        let data = data();
        build(&dir, &data, true);

        let index = MapIndex::<str>::new_immutable(dir.path(), memory, &deleted())
            .unwrap()
            .unwrap();
        assert_eq!(
            matches!(index, MapIndex::Immutable(_)),
            memory == Memory::Pinned,
        );
        check(&index, &data);
        assert!(index.files().contains(&dir.path().join(REGEX_INDEX_PATH)));
    }

    #[rstest]
    #[case::in_ram(Memory::Pinned)]
    #[case::on_disk(Memory::Cold)]
    fn read_only_selects_matching_keys(#[case] memory: Memory) {
        let dir = TempDir::with_prefix("regex_read_only_immutable").unwrap();
        let data = data();
        build(&dir, &data, true);

        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();
        let index: ReadOnlyMapIndex<str, ReadOnly<MmapFile>> =
            ReadOnlyMapIndex::open_immutable(&fs, dir.path(), memory, &deleted())
                .unwrap()
                .unwrap();
        check(&index, &data);
    }

    #[test]
    fn built_without_regex_option() {
        let dir = TempDir::with_prefix("regex_immutable_without").unwrap();
        let data = data();
        build(&dir, &data, false);

        let index = MapIndex::<str>::new_immutable(dir.path(), Memory::Cold, &deleted())
            .unwrap()
            .unwrap();
        let _scope = ambient::test_guard();
        let matcher = RegexMatcher::new("Einstein").unwrap();
        assert!(index.regex_keys(&matcher).unwrap().is_none());
        assert!(!dir.path().join(REGEX_INDEX_PATH).exists());
    }
}

/// The `regex` option of keyword index params reaches every variant.
mod index_option {
    use common::ambient;
    use common::bitvec::BitVec;
    use common::types::PointOffsetType;
    use common::universal_io::MmapFs;
    use rstest::rstest;
    use serde_json::Value;
    use tempfile::TempDir;

    use super::super::{RegexMatcher, StrMapIndexRegexRead};
    use crate::data_types::index::{KeywordIndexParams, KeywordIndexType};
    use crate::index::field_index::index_selector::IndexSelector;
    use crate::index::field_index::{FieldIndex, FieldIndexBuilderTrait, ReadOnlyFieldIndex};
    use crate::json_path::JsonPath;
    use crate::types::{Memory, PayloadFieldSchema, PayloadSchemaParams};

    fn schema(regex: Option<bool>) -> PayloadFieldSchema {
        PayloadFieldSchema::FieldParams(PayloadSchemaParams::Keyword(KeywordIndexParams {
            r#type: KeywordIndexType::Keyword,
            regex,
            ..Default::default()
        }))
    }

    fn has_regex_index(index: &FieldIndex) -> bool {
        let FieldIndex::KeywordIndex(index) = index else {
            panic!("expected a keyword index");
        };
        let matcher = RegexMatcher::new("abc").unwrap();
        index.regex_keys(&matcher).unwrap().is_some()
    }

    #[rstest]
    fn built_and_reopened(
        #[values(true, false)] appendable: bool,
        #[values(Some(true), Some(false), None)] regex: Option<bool>,
    ) {
        let dir = TempDir::with_prefix("regex_index_option").unwrap();
        let field = JsonPath::new("key");
        let schema = schema(regex);
        let deleted = BitVec::repeat(false, 16);
        let selector = if appendable {
            IndexSelector::Appendable { dir: dir.path() }
        } else {
            IndexSelector::NonAppendable {
                dir: dir.path(),
                memory: Memory::Cold,
            }
        };
        let expected = regex == Some(true);

        let _scope = ambient::test_guard();
        let mut builders = selector.index_builder(&field, &schema, &deleted).unwrap();
        let mut builder = builders.pop().unwrap();
        builder.init().unwrap();
        let value = Value::from("abcdef");
        builder.add_point(0 as PointOffsetType, &[&value]).unwrap();
        let built = builder.finalize().unwrap();
        assert_eq!(has_regex_index(&built), expected);
        let index_type = built.get_full_index_type();
        drop(built);

        // Read-only open, as of a segment opened by a reader.
        let read_only = ReadOnlyFieldIndex::open(
            &MmapFs,
            dir.path(),
            &field,
            &schema,
            &index_type,
            1,
            &deleted,
            None,
        )
        .unwrap()
        .unwrap();
        let ReadOnlyFieldIndex::KeywordIndex(read_only) = read_only else {
            panic!("expected a keyword index");
        };
        let matcher = RegexMatcher::new("abc").unwrap();
        assert_eq!(read_only.regex_keys(&matcher).unwrap().is_some(), expected);

        let mut reopened = selector
            .new_index(&field, &schema, false, &deleted)
            .unwrap()
            .unwrap();
        assert_eq!(has_regex_index(&reopened.pop().unwrap()), expected);
    }
}
