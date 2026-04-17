mod test_congruence;

use std::collections::HashSet;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use tempfile::Builder;

use crate::data_types::index::{TextIndexParams, TextIndexType, TokenizerType};
use crate::index::field_index::full_text_index::inverted_index::ParsedQuery;
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::{FieldIndexBuilderTrait as _, ValueIndexer};
use crate::types::{Fuzzy, FuzzyParams, MatchFuzzy};

fn movie_titles() -> Vec<String> {
    vec![
        "2430 A.D.".to_string(),
        "The Acquisitive Chuckle".to_string(),
        "Author! Author!".to_string(),
        "The Bicentennial Man".to_string(),
        "Big Game".to_string(),
        "The Billiard Ball".to_string(),
        "Birth of a Notion".to_string(),
        "Black Friar of the Flame".to_string(),
        "Blank!".to_string(),
        "Blind Alley".to_string(),
        "Breeds There a Man...?".to_string(),
        "Button, Button".to_string(),
        "Buy Jupiter".to_string(),
        "C-Chute".to_string(),
        "Cal".to_string(),
        "The Callistan Menace".to_string(),
        "Catch That Rabbit".to_string(),
        "Christmas on Ganymede".to_string(),
        "Darwinian Pool Room".to_string(),
        "Day of the Hunters".to_string(),
        "Death Sentence".to_string(),
        "Does a Bee Care?".to_string(),
        "Dreaming Is a Private Thing".to_string(),
        "The Dust of Death".to_string(),
        "The Dying Night".to_string(),
        "Each an Explorer".to_string(),
        "Escape!".to_string(),
        "Everest".to_string(),
        "Evidence".to_string(),
        "The Evitable Conflict".to_string(),
        "Exile to Hell".to_string(),
        "Eyes Do More Than See".to_string(),
        "The Feeling of Power".to_string(),
        "Feminine Intuition".to_string(),
        "First Law".to_string(),
        "Flies".to_string(),
        "For the Birds".to_string(),
        "Founding Father".to_string(),
        "The Fun They Had".to_string(),
        "Galley Slave".to_string(),
        "The Gentle Vultures".to_string(),
        "Getting Even".to_string(),
        "Gimmicks Three".to_string(),
        "Gold".to_string(),
        "Good Taste".to_string(),
        "The Greatest Asset".to_string(),
        "Green Patches".to_string(),
        "Half-Breed".to_string(),
        "Half-Breeds on Venus".to_string(),
        "Hallucination".to_string(),
        "The Hazing".to_string(),
        "Hell-Fire".to_string(),
        "Heredity".to_string(),
        "History".to_string(),
        "Homo Sol".to_string(),
        "Hostess".to_string(),
        "I Just Make Them Up, See!".to_string(),
        "I'm in Marsport Without Hilda".to_string(),
        "The Imaginary".to_string(),
        "The Immortal Bard".to_string(),
        "In a Good Cause—".to_string(),
        "Insert Knob A in Hole B".to_string(),
        "The Instability".to_string(),
        "It's Such a Beautiful Day".to_string(),
        "The Key".to_string(),
        "Kid Stuff".to_string(),
        "The Last Answer".to_string(),
        "The Last Question".to_string(),
        "The Last Trump".to_string(),
        "Left to Right".to_string(),
        "Legal Rites".to_string(),
        "Lenny".to_string(),
        "Lest We Remember".to_string(),
        "Let's Not".to_string(),
        "Liar!".to_string(),
        "Light Verse".to_string(),
        "Little Lost Robot".to_string(),
        "The Little Man on the Subway".to_string(),
        "Living Space".to_string(),
        "A Loint of Paw".to_string(),
        "The Magnificent Possession".to_string(),
        "Marching In".to_string(),
        "Marooned off Vesta".to_string(),
        "The Message".to_string(),
        "Mirror Image".to_string(),
        "Mother Earth".to_string(),
        "My Son, the Physicist".to_string(),
        "No Connection".to_string(),
        "No Refuge Could Save".to_string(),
        "Nobody Here But—".to_string(),
        "Not Final!".to_string(),
        "Obituary".to_string(),
        "Old-fashioned".to_string(),
        "Pâté de Foie Gras".to_string(),
        "The Pause".to_string(),
        "Ph as in Phony".to_string(),
        "The Portable Star".to_string(),
        "The Proper Study".to_string(),
        "Rain, Rain, Go Away".to_string(),
        "Reason".to_string(),
        "The Red Queen's Race".to_string(),
        "Rejection Slips".to_string(),
        "Ring Around the Sun".to_string(),
        "Risk".to_string(),
        "Robot AL-76 Goes Astray".to_string(),
        "Robot Dreams".to_string(),
        "Runaround".to_string(),
        "Sally".to_string(),
        "Satisfaction Guaranteed".to_string(),
        "The Secret Sense".to_string(),
        "Shah Guido G.".to_string(),
        "Silly Asses".to_string(),
        "The Singing Bell".to_string(),
        "Sixty Million Trillion Combinations".to_string(),
        "Spell My Name with an S".to_string(),
        "Star Light".to_string(),
        "A Statue for Father".to_string(),
        "Strikebreaker".to_string(),
        "Super-Neutron".to_string(),
        "Take a Match".to_string(),
        "The Talking Stone".to_string(),
        ". . . That Thou Art Mindful of Him".to_string(),
        "Thiotimoline".to_string(),
        "Time Pussy".to_string(),
        "Trends".to_string(),
        "Truth to Tell".to_string(),
        "The Ugly Little Boy".to_string(),
        "The Ultimate Crime".to_string(),
        "Unto the Fourth Generation".to_string(),
        "The Up-to-Date Sorcerer".to_string(),
        "Waterclap".to_string(),
        "The Watery Place".to_string(),
        "The Weapon".to_string(),
        "The Weapon Too Dreadful to Use".to_string(),
        "What If—".to_string(),
        "What Is This Thing Called Love?".to_string(),
        "What's in a Name?".to_string(),
        "The Winnowing".to_string(),
    ]
}

#[test]
fn test_prefix_search() {
    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    let config = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::Prefix,
        min_token_len: None,
        max_token_len: None,
        lowercase: None,
        phrase_matching: None,
        fuzzy_matching: None,
        stopwords: None,
        on_disk: None,
        stemmer: None,
        ascii_folding: None,
        enable_hnsw: None,
    };

    let mut index =
        FullTextIndex::new_gridstore(temp_dir.path().to_path_buf(), config.clone(), true)
            .unwrap()
            .unwrap();

    let hw_counter = HardwareCounterCell::new();

    let texts = movie_titles();

    for (i, text) in texts.iter().enumerate() {
        index
            .add_many(i as PointOffsetType, vec![text.clone()], &hw_counter)
            .unwrap();
    }

    let res: Vec<_> = index.query("ROBO", &hw_counter).collect();

    let query = index.parse_text_query("ROBO", &hw_counter).unwrap();

    for idx in res.iter().copied() {
        assert!(index.check_match(&query, idx));
    }

    assert_eq!(res.len(), 3);

    let res: Vec<_> = index.query("q231", &hw_counter).collect();
    assert!(res.is_empty());

    assert!(index.parse_text_query("q231", &hw_counter).is_none());
}

#[test]
fn test_phrase_matching() {
    let hw_counter = HardwareCounterCell::default();

    // Create a text index with phrase matching enabled
    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    let config = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::default(),
        min_token_len: None,
        max_token_len: None,
        lowercase: Some(true),
        on_disk: None,
        phrase_matching: Some(true), // Enable phrase matching
        fuzzy_matching: None,
        stopwords: None,
        stemmer: None,
        ascii_folding: None,
        enable_hnsw: None,
    };

    let mut mutable_index =
        FullTextIndex::builder_gridstore(temp_dir.path().to_path_buf(), config.clone())
            .make_empty()
            .unwrap();

    let mut mmap_builder =
        FullTextIndex::builder_mmap(temp_dir.path().to_path_buf(), config.clone(), true);
    mmap_builder.init().unwrap();

    // Add some test documents with phrases
    let documents = vec![
        (0, "the quick brown fox jumps over the lazy dog".to_string()),
        (1, "brown fox quick the jumps over lazy dog".to_string()),
        (2, "quick brown fox runs fast".to_string()),
        (3, "the lazy dog sleeps peacefully".to_string()),
        (4, "the brown brown fox".to_string()),
    ];

    for (point_id, text) in documents {
        mutable_index
            .add_many(point_id, vec![text.clone()], &hw_counter)
            .unwrap();
        mmap_builder
            .add_many(point_id, vec![text], &hw_counter)
            .unwrap();
    }

    let mmap_index = mmap_builder.finalize().unwrap();

    let check_matching = |index: FullTextIndex| {
        // Test regular text matching (should match documents containing all tokens regardless of order)
        let text_query = index
            .parse_text_query("quick brown fox", &hw_counter)
            .unwrap();
        assert!(index.check_match(&text_query, 0));
        assert!(index.check_match(&text_query, 1));
        assert!(index.check_match(&text_query, 2));

        let text_results: Vec<_> = index.filter_query(text_query, &hw_counter).collect();

        // Should match documents 0, 1, and 2 (all contain "quick", "brown", "fox")
        assert_eq!(text_results.len(), 3);
        assert!(text_results.contains(&0));
        assert!(text_results.contains(&1));
        assert!(text_results.contains(&2));

        // Test phrase matching (should only match documents with exact phrase in order)
        let phrase_query = index
            .parse_phrase_query("quick brown fox", &hw_counter)
            .unwrap();
        assert!(index.check_match(&phrase_query, 0));
        assert!(index.check_match(&phrase_query, 2));

        let phrase_results: Vec<_> = index.filter_query(phrase_query, &hw_counter).collect();

        // Should only match documents 0 and 2 (contain "quick brown fox" in that exact order)
        assert_eq!(phrase_results.len(), 2);
        assert!(phrase_results.contains(&0));
        assert!(phrase_results.contains(&2));
        assert!(!phrase_results.contains(&1)); // Document 1 has the words but not in the right order

        // Test phrase that doesn't exist
        let missing_query = index
            .parse_phrase_query("fox brown quick", &hw_counter)
            .unwrap();
        let missing_results: Vec<_> = index.filter_query(missing_query, &hw_counter).collect();

        // Should match no documents (no document contains this exact phrase)
        assert_eq!(missing_results.len(), 0);

        // Test valid phrase up to a token that doesn't exist
        let query_with_unknown_token = index.parse_phrase_query("quick brown bird", &hw_counter);
        // the phrase query is not valid because it contains an unknown token
        assert!(query_with_unknown_token.is_none());

        // Test repeated words
        let phrase_query = index
            .parse_phrase_query("brown brown fox", &hw_counter)
            .unwrap();
        assert!(index.check_match(&phrase_query, 4));

        // Should only match document 4
        let filter_results: Vec<_> = index.filter_query(phrase_query, &hw_counter).collect();
        assert_eq!(filter_results.len(), 1);
        assert!(filter_results.contains(&4));
    };

    check_matching(mutable_index);
    check_matching(mmap_index);
}

#[test]
fn test_fuzzy_search_suite() {
    const DOCS: &[(u32, &str)] = &[
        (0, "quickly sapphire falcon glides"),
        (1, "quickly amber rabbit hops"),
        (2, "slowly crimson falcon sleeps"),
        (3, "gentle bronze badger rests"),
        (4, "quick fox jumps"),
        (5, "quirk bug appears"),
        (6, "quack duck swims"),
        (7, "falcon sapphire quickly"),
        (8, "cat"),
        (9, "bat"),
        (10, "rat"),
        (11, "mat"),
        (12, "hat"),
        (13, "the cat sat on the mat"),
        (14, "slyly crimson falcon"),
        (15, "unrelated document here"),
        (16, "quickly"),
        (17, "Bellamy v. Cracker Barrel"),
        (18, "Bath Iron Works Corp. v. Congoleum Corp."),
    ];

    let hw_counter = HardwareCounterCell::default();

    let config = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::default(),
        phrase_matching: Some(true),
        fuzzy_matching: Some(true),
        lowercase: Some(true),
        ..Default::default()
    };

    let dir0 = Builder::new().prefix("test_dir").tempdir().unwrap();
    let dir1 = Builder::new().prefix("test_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("test_dir").tempdir().unwrap();

    let mut mutable = FullTextIndex::builder_gridstore(dir0.path().to_path_buf(), config.clone())
        .make_empty()
        .unwrap();
    assert!(matches!(mutable, FullTextIndex::Mutable(_)));

    let mut immutable_b =
        FullTextIndex::builder_mmap(dir1.path().to_path_buf(), config.clone(), false);
    immutable_b.init().unwrap();

    let mut mmap_b = FullTextIndex::builder_mmap(dir2.path().to_path_buf(), config, true);
    mmap_b.init().unwrap();

    for (id, text) in DOCS {
        let v = text.to_string();
        mutable.add_many(*id, vec![v.clone()], &hw_counter).unwrap();
        immutable_b
            .add_many(*id, vec![v.clone()], &hw_counter)
            .unwrap();
        mmap_b.add_many(*id, vec![v], &hw_counter).unwrap();
    }

    let immutable = immutable_b.finalize().unwrap();
    assert!(matches!(immutable, FullTextIndex::Immutable(_)));
    let mmap = mmap_b.finalize().unwrap();
    assert!(matches!(mmap, FullTextIndex::Mmap(_)));

    let indices = [
        ("mutable", &mutable),
        ("immutable", &immutable),
        ("mmap", &mmap),
    ];

    macro_rules! fp {
        ($e:expr, $p:expr, $x:expr) => {
            Some(FuzzyParams {
                max_edits: $e,
                prefix_length: $p,
                max_expansions: $x,
            })
        };
    }

    // (name, fuzzy, expected_ids)
    // expected_ids = None  → parse_fuzzy_query must return None
    // expected_ids = Some(&[]) with ExpansionsAtMost handled separately below
    type MaybeIds = Option<&'static [u32]>;
    let cases: &[(&str, Fuzzy, MaybeIds)] = &[
        // ── Text: all tokens must match ───────────────────────────────────────
        (
            "text_all_tokens_positive",
            Fuzzy::Text {
                text: "quikly saphire falcon".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[0, 7]),
        ),
        (
            "text_all_tokens_one_missing_in_corpus",
            Fuzzy::Text {
                text: "quikly saphire zzzzzzzz".into(),
                params: fp!(1, 0, 50),
            },
            None,
        ),
        // ── Edit-distance ceiling ─────────────────────────────────────────────
        (
            "two_edit_distance_not_matched",
            Fuzzy::Text {
                text: "slyly".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[14]),
        ),
        (
            "edit_zero_no_fuzzy_noise",
            Fuzzy::Text {
                text: "quickly sapphire falcon".into(),
                params: fp!(0, 0, 50),
            },
            Some(&[0, 7]),
        ),
        (
            "edit_zero_rejects_one_edit_neighbours",
            Fuzzy::Text {
                text: "quikly".into(),
                params: fp!(0, 0, 50),
            },
            None,
        ),
        // ── prefix_length guard ───────────────────────────────────────────────
        (
            "prefix2_permissive_includes_quack",
            Fuzzy::Text {
                text: "quick".into(),
                params: fp!(1, 2, 50),
            },
            Some(&[4, 5, 6]),
        ),
        (
            "prefix3_blocks_quack",
            // "qui" ≠ "qua" → quack filtered even though edit_dist("quick","quack")=1
            Fuzzy::Text {
                text: "quick".into(),
                params: fp!(1, 3, 50),
            },
            Some(&[4, 5]),
        ),
        (
            "prefix_exceeds_term_length_degrades_to_exact",
            Fuzzy::Text {
                text: "cat".into(),
                params: fp!(1, 99, 50),
            },
            Some(&[8, 13]),
        ),
        // ── Phrase: token order matters ───────────────────────────────────────
        (
            "phrase_correct_order_matches",
            // doc7 has same tokens but reversed → absent
            Fuzzy::Phrase {
                phrase: "quikly saphire falcon".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[0]),
        ),
        (
            "phrase_reversed_order_matches_only_reversed_doc",
            Fuzzy::Phrase {
                phrase: "falcon saphire quikly".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[7]),
        ),
        (
            "phrase_fuzzy_bridging_non_adjacent_tokens",
            // "sat" is 1-edit from "mat", so "cat sat" is an adjacent window in doc13
            // but it too short to fuzzy expand
            Fuzzy::Phrase {
                phrase: "cat mat".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[]),
        ),
        (
            "phrase_fuzzy_bridging_non_adjacent_tokens_with_prefix_guard",
            Fuzzy::Phrase {
                phrase: "cat mat".into(),
                params: fp!(1, 1, 50),
            },
            Some(&[]),
        ),
        // ── TextAny: any token suffices ───────────────────────────────────────
        (
            "text_any_one_token_matches",
            // "quikly"→{0,1,7,16}  "sloly"→slowly,slyly→{2,14}
            Fuzzy::TextAny {
                text_any: "quikly sloly".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[0, 1, 2, 7, 14, 16]),
        ),
        (
            "text_any_no_tokens_match",
            Fuzzy::TextAny {
                text_any: "zzzzzz qqqqqq".into(),
                params: fp!(1, 0, 50),
            },
            None,
        ),
        (
            "text_any_failing_token_does_not_pollute_results",
            // "zzzzzz" → nothing; result must equal the "quickly" set exactly
            Fuzzy::TextAny {
                text_any: "quickly zzzzzz".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[0, 1, 7, 16]),
        ),
        (
            "text_any_single_char_token_from_punctuation_matches",
            Fuzzy::TextAny {
                text_any: "v.".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[17, 18]),
        ),
        (
            "text_single_char_token_with_punctuation_matches",
            Fuzzy::Text {
                text: "bellamy v. cracker barrel".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[17]),
        ),
        (
            "phrase_single_char_token_with_punctuation_matches",
            Fuzzy::Phrase {
                phrase: "bellamy v. cracker barrel".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[17]),
        ),
        // ── Token-boundary / substring guard ─────────────────────────────────
        (
            "prefix_substring_not_confused_with_fuzzy",
            // edit_dist("quick","quickly")=2 → must NOT match at max_edits=1
            Fuzzy::Text {
                text: "quick".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[4, 5, 6]),
        ),
        (
            "real_test",
            Fuzzy::Phrase {
                phrase: "Bath Iron Works Corp. v.".into(),
                params: fp!(1, 0, 50),
            },
            Some(&[18]),
        ),
    ];

    // max_expansions cap — checked separately (result set is implementation-dependent)
    let expansion_cases: &[(&str, Fuzzy, usize)] = &[(
        "max_expansions_caps_expansion_count",
        Fuzzy::Text {
            text: "cat".into(),
            params: fp!(1, 0, 2),
        },
        2,
    )];

    for (name, fuzzy, expected_ids) in cases {
        for (iname, index) in &indices {
            let maybe_parsed = index.parse_fuzzy_query(
                &MatchFuzzy {
                    fuzzy: vec![fuzzy.clone()],
                },
                &hw_counter,
            );

            let Some(parsed) = maybe_parsed else {
                if expected_ids.map_or(true, |ids| ids.is_empty()) {
                    continue;
                }
                panic!("[{name}|{iname}] parse returned None but expected non-empty results");
            };

            let actual: HashSet<u32> = index.filter_query(parsed.clone(), &hw_counter).collect();

            for (point_id, _) in DOCS {
                let via_check = index.check_match(&parsed, *point_id);
                let via_filter = actual.contains(point_id);
                assert_eq!(
                    via_check, via_filter,
                    "[{name}|{iname}] check_match/filter_query disagree at doc {point_id}: \
                     filter={via_filter} check={via_check}"
                );
            }

            if let Some(ids) = expected_ids {
                let expected: HashSet<u32> = ids.iter().copied().collect();
                assert_eq!(
                    actual,
                    expected,
                    "[{name}|{iname}]\n  missing={:?}\n  extra={:?}",
                    expected.difference(&actual).collect::<Vec<_>>(),
                    actual.difference(&expected).collect::<Vec<_>>()
                );
            }
        }
    }

    for (name, fuzzy, max) in expansion_cases {
        for (iname, index) in &indices {
            let Some(parsed) = index.parse_fuzzy_query(
                &MatchFuzzy {
                    fuzzy: vec![fuzzy.clone()],
                },
                &hw_counter,
            ) else {
                panic!("[{name}|{iname}] parse returned None");
            };
            let count = match parsed {
                ParsedQuery::FuzzyAllTokens(g) | ParsedQuery::FuzzyPhrase(g) => {
                    g.iter().map(|g| g.len()).sum()
                }
                ParsedQuery::FuzzyAnyTokens(t) => t.len(),
                _ => panic!("expected fuzzy query, got {parsed:?}"),
            };
            assert!(
                count <= *max,
                "expected at most {max} expanded terms, got {count}"
            );
        }
    }
}

#[test]
fn test_multi_fuzzy_clause_semantics() {
    const DOCS: &[(u32, &str)] = &[
        (0, "quickly sapphire falcon glides"),
        (1, "quickly amber rabbit hops"),
        (2, "slowly crimson falcon sleeps"),
        (3, "gentle bronze badger rests"),
        (4, "quick fox jumps"),
        (5, "quirk bug appears"),
        (6, "quack duck swims"),
        (7, "falcon sapphire quickly"),
        (8, "cat"),
        (9, "bat"),
        (10, "rat"),
        (11, "mat"),
        (12, "hat"),
        (13, "the cat sat on the mat"),
        (14, "slyly crimson falcon"),
        (15, "unrelated document here"),
        (16, "quickly"),
        (17, "Bellamy v. Cracker Barrel"),
        (18, "Bath Iron Works Corp. v. Congoleum Corp."),
    ];

    let hw_counter = HardwareCounterCell::default();

    let config = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::default(),
        phrase_matching: Some(true),
        fuzzy_matching: Some(true),
        lowercase: Some(true),
        ..Default::default()
    };

    let dir0 = Builder::new().prefix("test_dir").tempdir().unwrap();
    let dir1 = Builder::new().prefix("test_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("test_dir").tempdir().unwrap();

    let mut mutable = FullTextIndex::builder_gridstore(dir0.path().to_path_buf(), config.clone())
        .make_empty()
        .unwrap();

    let mut immutable_b =
        FullTextIndex::builder_mmap(dir1.path().to_path_buf(), config.clone(), false);
    immutable_b.init().unwrap();

    let mut mmap_b = FullTextIndex::builder_mmap(dir2.path().to_path_buf(), config, true);
    mmap_b.init().unwrap();

    for (id, text) in DOCS {
        let value = text.to_string();
        mutable
            .add_many(*id, vec![value.clone()], &hw_counter)
            .unwrap();
        immutable_b
            .add_many(*id, vec![value.clone()], &hw_counter)
            .unwrap();
        mmap_b.add_many(*id, vec![value], &hw_counter).unwrap();
    }

    let immutable = immutable_b.finalize().unwrap();
    let mmap = mmap_b.finalize().unwrap();

    let indices = [
        ("mutable", &mutable),
        ("immutable", &immutable),
        ("mmap", &mmap),
    ];

    macro_rules! fp {
        ($e:expr, $p:expr, $x:expr) => {
            Some(FuzzyParams {
                max_edits: $e,
                prefix_length: $p,
                max_expansions: $x,
            })
        };
    }

    fn assert_matches_variant(parsed: &ParsedQuery, variant: &str) {
        match variant {
            "all" => assert!(matches!(parsed, ParsedQuery::AllTokens(_))),
            "any" => assert!(matches!(parsed, ParsedQuery::AnyTokens(_))),
            "phrase" => assert!(matches!(parsed, ParsedQuery::Phrase(_))),
            "fuzzy_all" => assert!(matches!(parsed, ParsedQuery::FuzzyAllTokens(_))),
            "fuzzy_any" => assert!(matches!(parsed, ParsedQuery::FuzzyAnyTokens(_))),
            "fuzzy_phrase" => assert!(matches!(parsed, ParsedQuery::FuzzyPhrase(_))),
            other => panic!("unknown variant {other}"),
        }
    }

    let assert_query =
        |name: &str, match_fuzzy: MatchFuzzy, expected_variant: &str, expected_ids: &[u32]| {
            for (iname, index) in &indices {
                let Some(parsed) = index.parse_fuzzy_query(&match_fuzzy, &hw_counter) else {
                    panic!("[{name}|{iname}] parse returned None");
                };

                assert_matches_variant(&parsed, expected_variant);

                let actual: HashSet<u32> =
                    index.filter_query(parsed.clone(), &hw_counter).collect();
                let expected: HashSet<u32> = expected_ids.iter().copied().collect();

                assert_eq!(
                    actual,
                    expected,
                    "[{name}|{iname}]\n  missing={:?}\n  extra={:?}",
                    expected.difference(&actual).collect::<Vec<_>>(),
                    actual.difference(&expected).collect::<Vec<_>>()
                );
            }
        };

    assert_query(
        "multi_text_exact_becomes_all_tokens",
        MatchFuzzy {
            fuzzy: vec![
                Fuzzy::Text {
                    text: "quickly sapphire".into(),
                    params: fp!(0, 0, 50),
                },
                Fuzzy::Text {
                    text: "falcon".into(),
                    params: fp!(0, 0, 50),
                },
            ],
        },
        "all",
        &[0, 7],
    );

    assert_query(
        "multi_text_fuzzy_becomes_fuzzy_all_tokens",
        MatchFuzzy {
            fuzzy: vec![
                Fuzzy::Text {
                    text: "quikly saphire".into(),
                    params: fp!(1, 0, 50),
                },
                Fuzzy::Text {
                    text: "falcon".into(),
                    params: fp!(1, 0, 50),
                },
            ],
        },
        "fuzzy_all",
        &[0, 7],
    );

    assert_query(
        "multi_text_any_exact_flattens_to_any_tokens",
        MatchFuzzy {
            fuzzy: vec![
                Fuzzy::TextAny {
                    text_any: "quickly slowly".into(),
                    params: fp!(0, 0, 50),
                },
                Fuzzy::TextAny {
                    text_any: "falcon rabbit".into(),
                    params: fp!(0, 0, 50),
                },
            ],
        },
        "any",
        &[0, 1, 2, 7, 14, 16],
    );

    assert_query(
        "single_text_any_exact_stays_any_tokens",
        MatchFuzzy {
            fuzzy: vec![Fuzzy::TextAny {
                text_any: "quickly slowly".into(),
                params: fp!(0, 0, 50),
            }],
        },
        "any",
        &[0, 1, 2, 7, 16],
    );

    assert_query(
        "multi_phrase_exact_concatenates_in_order",
        MatchFuzzy {
            fuzzy: vec![
                Fuzzy::Phrase {
                    phrase: "quickly sapphire".into(),
                    params: fp!(0, 0, 50),
                },
                Fuzzy::Phrase {
                    phrase: "falcon".into(),
                    params: fp!(0, 0, 50),
                },
            ],
        },
        "phrase",
        &[0],
    );

    assert_query(
        "multi_phrase_fuzzy_becomes_fuzzy_phrase",
        MatchFuzzy {
            fuzzy: vec![
                Fuzzy::Phrase {
                    phrase: "quikly saphire".into(),
                    params: fp!(1, 0, 50),
                },
                Fuzzy::Phrase {
                    phrase: "falcon".into(),
                    params: fp!(1, 0, 50),
                },
            ],
        },
        "fuzzy_phrase",
        &[0],
    );

    for (iname, index) in &indices {
        let mixed = MatchFuzzy {
            fuzzy: vec![
                Fuzzy::Text {
                    text: "quickly".into(),
                    params: fp!(1, 0, 50),
                },
                Fuzzy::Phrase {
                    phrase: "sapphire falcon".into(),
                    params: fp!(1, 0, 50),
                },
            ],
        };
        assert!(
            index.parse_fuzzy_query(&mixed, &hw_counter).is_none(),
            "[mixed_types_rejected|{iname}] expected parse to return None"
        );

        let empty = MatchFuzzy { fuzzy: vec![] };
        assert!(
            index.parse_fuzzy_query(&empty, &hw_counter).is_none(),
            "[empty_fuzzy_rejected|{iname}] expected parse to return None"
        );
    }
}

#[test]
fn test_fuzzy_prefix_length_uses_char_boundaries_for_all_backends() {
    const DOCS: &[(u32, &str)] = &[(0, "éclair"), (1, "êclair"), (2, "éclait")];

    let hw_counter = HardwareCounterCell::default();

    let config = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::default(),
        phrase_matching: Some(true),
        fuzzy_matching: Some(true),
        lowercase: Some(true),
        ..Default::default()
    };

    let dir0 = Builder::new().prefix("test_dir").tempdir().unwrap();
    let dir1 = Builder::new().prefix("test_dir").tempdir().unwrap();
    let dir2 = Builder::new().prefix("test_dir").tempdir().unwrap();

    let mut mutable = FullTextIndex::builder_gridstore(dir0.path().to_path_buf(), config.clone())
        .make_empty()
        .unwrap();

    let mut immutable_b =
        FullTextIndex::builder_mmap(dir1.path().to_path_buf(), config.clone(), false);
    immutable_b.init().unwrap();

    let mut mmap_b = FullTextIndex::builder_mmap(dir2.path().to_path_buf(), config, true);
    mmap_b.init().unwrap();

    for (id, text) in DOCS {
        let value = text.to_string();
        mutable
            .add_many(*id, vec![value.clone()], &hw_counter)
            .unwrap();
        immutable_b
            .add_many(*id, vec![value.clone()], &hw_counter)
            .unwrap();
        mmap_b.add_many(*id, vec![value], &hw_counter).unwrap();
    }

    let immutable = immutable_b.finalize().unwrap();
    let mmap = mmap_b.finalize().unwrap();

    let indices = [
        ("mutable", &mutable),
        ("immutable", &immutable),
        ("mmap", &mmap),
    ];

    let query = MatchFuzzy {
        fuzzy: vec![Fuzzy::Text {
            text: "éclair".into(),
            params: Some(FuzzyParams {
                max_edits: 1,
                prefix_length: 1,
                max_expansions: 50,
            }),
        }],
    };

    let expected: HashSet<u32> = [0, 2].into_iter().collect();

    for (iname, index) in &indices {
        let Some(parsed) = index.parse_fuzzy_query(&query, &hw_counter) else {
            panic!("[{iname}] parse returned None");
        };

        let actual: HashSet<u32> = index.filter_query(parsed, &hw_counter).collect();

        assert_eq!(
            actual, expected,
            "[{iname}] expected UTF-8 prefix guard to exclude terms whose first character differs"
        );
    }
}

#[test]
fn test_ascii_folding_in_full_text_index_word() {
    let hw_counter = HardwareCounterCell::default();

    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    let config_enabled = TextIndexParams {
        r#type: TextIndexType::Text,
        tokenizer: TokenizerType::Word,
        min_token_len: None,
        max_token_len: None,
        lowercase: None,
        on_disk: None,
        phrase_matching: None,
        fuzzy_matching: None,
        stopwords: None,
        stemmer: None,
        ascii_folding: Some(true),
        enable_hnsw: None,
    };
    let config_disabled = TextIndexParams {
        ascii_folding: Some(false),
        ..config_enabled.clone()
    };

    // Index with folding enabled
    let mut index_enabled =
        FullTextIndex::new_gridstore(temp_dir.path().to_path_buf(), config_enabled.clone(), true)
            .unwrap()
            .unwrap();

    // Index with folding disabled (separate storage path)
    let temp_dir2 = Builder::new().prefix("test_dir").tempdir().unwrap();
    let mut index_disabled = FullTextIndex::new_gridstore(
        temp_dir2.path().to_path_buf(),
        config_disabled.clone(),
        true,
    )
    .unwrap()
    .unwrap();

    // Documents containing accents
    let docs = vec![
        (0, "ação no coração".to_string()),
        (1, "café com leite".to_string()),
    ];

    for (id, text) in &docs {
        index_enabled
            .add_many(*id as PointOffsetType, vec![text.clone()], &hw_counter)
            .unwrap();
        index_disabled
            .add_many(*id as PointOffsetType, vec![text.clone()], &hw_counter)
            .unwrap();
    }

    // ASCII-only queries should match only when folding is enabled
    let query_enabled = index_enabled.parse_text_query("acao", &hw_counter).unwrap();
    assert!(index_enabled.check_match(&query_enabled, 0));

    let results_enabled: Vec<_> = index_enabled
        .filter_query(query_enabled, &hw_counter)
        .collect();
    assert!(results_enabled.contains(&0));

    let query_disabled_opt = index_disabled.parse_text_query("acao", &hw_counter);
    // Query might still parse, but should not match anything
    if let Some(query_disabled) = query_disabled_opt {
        let results_disabled: Vec<_> = index_disabled
            .filter_query(query_disabled, &hw_counter)
            .collect();
        assert!(!results_disabled.contains(&0));
    }

    // Non-folded query must work in both
    let query_acento = index_enabled.parse_text_query("ação", &hw_counter).unwrap();
    assert!(index_enabled.check_match(&query_acento, 0));
    let results_acento: Vec<_> = index_enabled
        .filter_query(query_acento, &hw_counter)
        .collect();
    assert!(results_acento.contains(&0));

    let query_acento2 = index_disabled
        .parse_text_query("ação", &hw_counter)
        .unwrap();
    let results_acento2: Vec<_> = index_disabled
        .filter_query(query_acento2, &hw_counter)
        .collect();
    assert!(results_acento2.contains(&0));
}
