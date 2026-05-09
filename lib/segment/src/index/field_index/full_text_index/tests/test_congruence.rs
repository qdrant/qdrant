use std::collections::HashSet;

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rstest::rstest;
use serde_json::Value;
use tempfile::{Builder, TempDir};

use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::fixtures::payload_fixtures::random_full_text_payload;
use crate::index::field_index::field_index_base::{PayloadFieldIndex, PayloadFieldIndexRead};
use crate::index::field_index::full_text_index::inverted_index::{
    ARRAY_BOUNDARY_SENTINEL, Document, ParsedQuery, TokenId, TokenSet,
};
use crate::index::field_index::full_text_index::mmap_text_index::FullTextMmapIndexBuilder;
use crate::index::field_index::full_text_index::mutable_text_index::MutableFullTextIndex;
use crate::index::field_index::full_text_index::text_index::{
    FullTextGridstoreIndexBuilder, FullTextIndex,
};
use crate::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};
use crate::json_path::JsonPath;
use crate::types::{FieldCondition, ValuesCount};

type Database = ();

const FIELD_NAME: &str = "test";
const TYPES: &[IndexType] = &[
    IndexType::MutableGridstore,
    IndexType::ImmMmap,
    IndexType::ImmRamMmap,
];

#[derive(Clone, Copy, PartialEq, Debug)]
enum IndexType {
    MutableGridstore,
    ImmMmap,
    ImmRamMmap,
}

enum IndexBuilder {
    MutableGridstore(FullTextGridstoreIndexBuilder),
    ImmMmap(FullTextMmapIndexBuilder),
    ImmRamMmap(FullTextMmapIndexBuilder),
}

impl IndexBuilder {
    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            IndexBuilder::MutableGridstore(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
            }
            IndexBuilder::ImmMmap(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
            }
            IndexBuilder::ImmRamMmap(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
            }
        }
    }

    fn finalize(self) -> OperationResult<FullTextIndex> {
        match self {
            IndexBuilder::MutableGridstore(builder) => builder.finalize(),
            IndexBuilder::ImmMmap(builder) => builder.finalize(),
            IndexBuilder::ImmRamMmap(builder) => builder.finalize(),
        }
    }
}

fn create_builder(
    index_type: IndexType,
    phrase_matching: bool,
) -> (IndexBuilder, TempDir, Database) {
    let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
    let db = ();

    let config = TextIndexParams {
        phrase_matching: Some(phrase_matching),
        ..TextIndexParams::default()
    };

    let empty_deleted = BitVec::new();
    let mut builder = match index_type {
        IndexType::MutableGridstore => IndexBuilder::MutableGridstore(
            FullTextIndex::builder_gridstore(temp_dir.path().to_path_buf(), config),
        ),
        IndexType::ImmMmap => IndexBuilder::ImmMmap(FullTextIndex::builder_mmap(
            temp_dir.path().to_path_buf(),
            config,
            true,
            &empty_deleted,
        )),
        IndexType::ImmRamMmap => IndexBuilder::ImmRamMmap(FullTextIndex::builder_mmap(
            temp_dir.path().to_path_buf(),
            config,
            false,
            &empty_deleted,
        )),
    };
    match &mut builder {
        IndexBuilder::MutableGridstore(builder) => builder.init().unwrap(),
        IndexBuilder::ImmMmap(builder) => builder.init().unwrap(),
        IndexBuilder::ImmRamMmap(builder) => builder.init().unwrap(),
    }
    (builder, temp_dir, db)
}

fn reopen_index(
    index: FullTextIndex,
    index_type: IndexType,
    temp_dir: &TempDir,
    #[allow(unused_variables)] db: &Database,
    phrase_matching: bool,
    num_points: usize,
) -> FullTextIndex {
    let config = TextIndexParams {
        phrase_matching: Some(phrase_matching),
        ..TextIndexParams::default()
    };

    // Capture the deletion state so we can re-supply it on reopen of the
    // mmap-backed variants. The mmap index no longer persists runtime
    // deletions: callers (id-tracker in production, this test in unit tests)
    // are responsible for the cumulative deletion mask.
    let mut deleted = BitVec::repeat(false, num_points);
    for point_id in 0..num_points as PointOffsetType {
        if index.values_is_empty(point_id) {
            deleted.set(point_id as usize, true);
        }
    }

    // Drop the original index to ensure files are flushed
    drop(index);

    // Reopen based on index type
    match index_type {
        IndexType::MutableGridstore => {
            FullTextIndex::new_gridstore(temp_dir.path().to_path_buf(), config, false)
                .unwrap()
                .expect("Failed to reopen MutableGridstore index")
        }
        IndexType::ImmMmap => {
            // Reopen with is_on_disk = true (mmap directly)
            FullTextIndex::new_mmap(temp_dir.path().to_path_buf(), config, true, &deleted)
                .unwrap()
                .expect("Failed to reopen ImmMmap index")
        }
        IndexType::ImmRamMmap => {
            // Reopen with is_on_disk = false (load into RAM)
            // This is the path that will call ImmutableFullTextIndex::open_mmap
            FullTextIndex::new_mmap(temp_dir.path().to_path_buf(), config, false, &deleted)
                .unwrap()
                .expect("Failed to reopen ImmRamMmap index")
        }
    }
}

fn build_random_index(
    num_points: usize,
    num_keywords: usize,
    keyword_len: usize,
    index_type: IndexType,
    phrase_matching: bool,
    deleted: bool,
    reopen: bool,
) -> (FullTextIndex, TempDir, Database) {
    let mut rnd = StdRng::seed_from_u64(42);
    let (mut builder, temp_dir, db) = create_builder(index_type, phrase_matching);

    for idx in 0..num_points {
        let keywords = random_full_text_payload(
            &mut rnd,
            num_keywords..=num_keywords,
            keyword_len..=keyword_len,
        );
        let array_payload = Value::Array(keywords);
        builder
            .add_point(
                idx as PointOffsetType,
                &[&array_payload],
                &HardwareCounterCell::new(),
            )
            .unwrap();
    }

    let mut index = builder.finalize().unwrap();
    assert_eq!(index.points_count(), num_points);

    // Delete some points before loading into a different format
    if deleted {
        index.remove_point(20).unwrap();
        index.remove_point(21).unwrap();
        index.remove_point(22).unwrap();
        index.remove_point(200).unwrap();
        index.remove_point(250).unwrap();

        index.flusher()().expect("failed to flush deletions");
    }

    // Reopen the index if requested
    let index = if reopen {
        reopen_index(
            index,
            index_type,
            &temp_dir,
            &db,
            phrase_matching,
            num_points,
        )
    } else {
        index
    };

    (index, temp_dir, db)
}

pub fn parse_query(query: &[String], is_phrase: bool, index: &FullTextIndex) -> ParsedQuery {
    let hw_counter = HardwareCounterCell::disposable();
    let tokens = resolve_tokens(index, query, &hw_counter).into_iter();
    match is_phrase {
        false => ParsedQuery::AllTokens(tokens.collect::<Option<TokenSet>>().unwrap()),
        true => ParsedQuery::Phrase(tokens.collect::<Option<Document>>().unwrap()),
    }
}

fn resolve_tokens<S: AsRef<str>>(
    index: &FullTextIndex,
    tokens: &[S],
    hw_counter: &HardwareCounterCell,
) -> Vec<Option<TokenId>> {
    let mut ids = vec![None; tokens.len()];
    index
        .for_each_token_id(
            tokens.iter().map(|s| s.as_ref()).enumerate(),
            hw_counter,
            |i, id| ids[i] = id,
        )
        .unwrap();
    ids
}

#[rstest]
fn test_congruence(
    #[values(false, true)] deleted: bool,
    #[values(false, true)] phrase_matching: bool,
    #[values(false, true)] reopen: bool,
) {
    const POINT_COUNT: usize = 500;
    const KEYWORD_COUNT: usize = 20;
    const KEYWORD_LEN: usize = 2;

    let hw_counter = HardwareCounterCell::disposable();

    let (mut indices, _data): (Vec<_>, Vec<_>) = TYPES
        .iter()
        .copied()
        .map(|index_type| {
            let (index, temp_dir, db) = build_random_index(
                POINT_COUNT,
                KEYWORD_COUNT,
                KEYWORD_LEN,
                index_type,
                phrase_matching,
                deleted,
                reopen,
            );
            ((index, index_type), (temp_dir, db))
        })
        .unzip();

    // Delete some points after loading
    if deleted {
        for (index, _type) in indices.iter_mut() {
            index.remove_point(10).unwrap();
            index.remove_point(11).unwrap();
            index.remove_point(12).unwrap();
            index.remove_point(100).unwrap();
            index.remove_point(150).unwrap();
        }
    }

    // Grab 10 keywords to use for querying
    let (FullTextIndex::Mutable(index), _) = &indices[0] else {
        panic!("Expects mutable full text index as first");
    };
    let mut keywords = index
        .inverted_index
        .vocab
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    keywords.sort_unstable();
    keywords.truncate(10);

    const EXISTING_IDS: &[PointOffsetType] = &[5, 19, 57, 223, 229, 499];
    let existing_phrases =
        check_phrase::<KEYWORD_COUNT>(EXISTING_IDS, index, &indices, phrase_matching);

    for i in 1..indices.len() {
        let ((index_a, type_a), (index_b, type_b)) = (&indices[0], &indices[i]);
        eprintln!("Testing index type {type_a:?} vs {type_b:?}");

        assert_eq!(index_a.points_count(), index_b.points_count());
        for point_id in 0..POINT_COUNT as PointOffsetType {
            assert_eq!(
                index_a.values_count(point_id),
                index_b.values_count(point_id),
            );
            assert_eq!(
                index_a.values_is_empty(point_id),
                index_b.values_is_empty(point_id),
            );
        }

        let probe_tokens = ["doesnotexist", keywords[0].as_str()];
        let probe_a = resolve_tokens(index_a, &probe_tokens, &hw_counter);
        let probe_b = resolve_tokens(index_b, &probe_tokens, &hw_counter);
        assert_eq!(probe_a[0], probe_b[0]);
        assert_eq!(probe_a[1].is_some(), probe_b[1].is_some());

        for query_range in [0..1, 2..4, 5..9, 0..10] {
            let keywords = &keywords[query_range];
            let parsed_query_a = parse_query(keywords, false, index_a);
            let parsed_query_b = parse_query(keywords, false, index_b);

            // Mutable index behaves different versus the others on point deletion
            // Mutable index updates postings, the others do not. Cardinality estimations are
            // not expected to match because of it.
            if !deleted {
                let field_condition = FieldCondition::new_values_count(
                    JsonPath::new(FIELD_NAME),
                    ValuesCount::from(0..10),
                );
                let cardinality_a = index_a.estimate_query_cardinality(
                    &parsed_query_a,
                    &field_condition,
                    &hw_counter,
                );
                let cardinality_b = index_b.estimate_query_cardinality(
                    &parsed_query_b,
                    &field_condition,
                    &hw_counter,
                );
                assert_eq!(cardinality_a, cardinality_b);
            }

            for point_id in 0..POINT_COUNT as PointOffsetType {
                assert_eq!(
                    index_a.check_match(&parsed_query_a, point_id).unwrap(),
                    index_b.check_match(&parsed_query_b, point_id).unwrap(),
                );
            }

            assert_eq!(
                index_a
                    .filter_query(parsed_query_a, &hw_counter)
                    .unwrap()
                    .collect::<HashSet<_>>(),
                index_b
                    .filter_query(parsed_query_b, &hw_counter)
                    .unwrap()
                    .collect::<HashSet<_>>(),
            );
        }

        if phrase_matching {
            for phrase in &existing_phrases {
                eprintln!("Phrase: {phrase:?}");

                let parsed_query_a = parse_query(phrase, true, index_a);
                let parsed_query_b = parse_query(phrase, true, index_b);

                // Mutable index removes from postings on deletion,
                // immutable does not — cardinality estimates can differ.
                if !deleted {
                    let field_condition = FieldCondition::new_values_count(
                        JsonPath::new(FIELD_NAME),
                        ValuesCount::from(0..10),
                    );
                    assert_eq!(
                        index_a.estimate_query_cardinality(
                            &parsed_query_a,
                            &field_condition,
                            &hw_counter
                        ),
                        index_b.estimate_query_cardinality(
                            &parsed_query_b,
                            &field_condition,
                            &hw_counter
                        ),
                    );
                }

                for point_id in 0..POINT_COUNT as PointOffsetType {
                    assert_eq!(
                        index_a.check_match(&parsed_query_a, point_id).unwrap(),
                        index_b.check_match(&parsed_query_b, point_id).unwrap(),
                    );
                }

                // Assert that both indices return the same results
                assert_eq!(
                    index_a
                        .filter_query(parsed_query_a, &hw_counter)
                        .unwrap()
                        .collect::<HashSet<_>>(),
                    index_b
                        .filter_query(parsed_query_b, &hw_counter)
                        .unwrap()
                        .collect::<HashSet<_>>(),
                );
            }
        }

        if !deleted {
            for threshold in 1..=10 {
                let mut count_a = 0;
                index_a
                    .for_each_payload_block(threshold, JsonPath::new(FIELD_NAME), &mut |_| {
                        count_a += 1;
                        Ok(())
                    })
                    .unwrap();
                let mut count_b = 0;
                index_b
                    .for_each_payload_block(threshold, JsonPath::new(FIELD_NAME), &mut |_| {
                        count_b += 1;
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(count_a, count_b);
            }
        }
    }
}

/// Checks that the ids can be found when filtering and matching a phrase.
///
/// Returns the phrases that were used
fn check_phrase<const KEYWORD_COUNT: usize>(
    existing_ids: &[PointOffsetType],
    mutable_index: &MutableFullTextIndex,
    check_indexes: &[(FullTextIndex, IndexType)],
    phrase_matching: bool,
) -> Vec<Vec<String>> {
    const PHRASE_LENGTH: usize = 4;
    let mut phrases = Vec::new();
    let rng = &mut StdRng::seed_from_u64(43);
    for id in existing_ids {
        let doc = mutable_index.get_doc(*id).unwrap();

        // Split stored tokens into per-element segments at boundary sentinels
        let segments: Vec<Vec<&str>> = doc
            .split(|t| t.as_str() == ARRAY_BOUNDARY_SENTINEL)
            .map(|seg| seg.iter().map(|s| s.as_str()).collect())
            .filter(|seg: &Vec<&str>| seg.len() >= PHRASE_LENGTH)
            .collect();

        let phrase = if segments.is_empty() {
            // All elements are single-word; fall back to a single-token phrase
            let real_tokens: Vec<&str> = doc
                .iter()
                .filter(|t| t.as_str() != ARRAY_BOUNDARY_SENTINEL)
                .map(|t| t.as_str())
                .collect();
            let rand_idx = rng.random_range(0..real_tokens.len());
            vec![real_tokens[rand_idx].to_string()]
        } else {
            let seg = &segments[rng.random_range(0..segments.len())];
            let rand_idx = rng.random_range(0..=seg.len() - PHRASE_LENGTH);
            seg[rand_idx..rand_idx + PHRASE_LENGTH]
                .iter()
                .map(|s| s.to_string())
                .collect()
        };

        phrases.push(phrase);
    }

    let hw_counter = HardwareCounterCell::disposable();

    for (index, index_type) in check_indexes {
        eprintln!("Checking index type: {index_type:?}");
        for (phrase, exp_id) in phrases.iter().zip(existing_ids) {
            eprintln!("Phrase: {phrase:?}");

            let parsed_query = parse_query(phrase, phrase_matching, index);

            assert!(index.check_match(&parsed_query, *exp_id).unwrap());

            let result = index
                .filter_query(parsed_query, &hw_counter)
                .unwrap()
                .collect::<HashSet<_>>();

            assert!(!result.is_empty());
            assert!(
                result.contains(exp_id),
                "Expected ID {exp_id} not found in other index result: {result:?}"
            );
        }
    }

    phrases
}

/// Reproduces the bug from <https://github.com/qdrant/qdrant/issues/8937>:
/// phrase matching must NOT cross string-array element boundaries when a
/// full-text index is enabled.
#[rstest]
fn test_phrase_matching_respects_array_boundaries(
    #[values(IndexType::MutableGridstore, IndexType::ImmMmap, IndexType::ImmRamMmap)]
    index_type: IndexType,
) {
    let hw = HardwareCounterCell::new();
    let (mut builder, _temp_dir, _db) = create_builder(index_type, true);

    // ID 1: ["quick", "brown"] — words in separate elements
    let p1 = serde_json::json!(["quick", "brown"]);
    // ID 2: ["quick brown"]   — phrase in a single element
    let p2 = serde_json::json!(["quick brown"]);
    // ID 3: ["quick", "blue"] — words in separate elements
    let p3 = serde_json::json!(["quick", "blue"]);
    // ID 4: "quick brown fox" — plain string
    let p4 = serde_json::json!("quick brown fox");
    // ID 5: ["quick blue"]    — phrase in a single element
    let p5 = serde_json::json!(["quick blue"]);

    builder.add_point(1, &[&p1], &hw).unwrap();
    builder.add_point(2, &[&p2], &hw).unwrap();
    builder.add_point(3, &[&p3], &hw).unwrap();
    builder.add_point(4, &[&p4], &hw).unwrap();
    builder.add_point(5, &[&p5], &hw).unwrap();

    let index = builder.finalize().unwrap();

    // "quick brown" should match only IDs 2 and 4 (phrase within one element)
    let qb = index.parse_phrase_query("quick brown", &hw).unwrap();
    assert!(qb.is_some(), "query tokens must exist");
    let qb = qb.unwrap();

    let mut results: Vec<_> = index.filter_query(qb.clone(), &hw).unwrap().collect();
    results.sort();
    assert_eq!(
        results,
        vec![2, 4],
        "\"quick brown\" must NOT match ID 1 (words in separate array elements)"
    );

    // Also verify via check_match
    assert!(!index.check_match(&qb, 1).unwrap());
    assert!(index.check_match(&qb, 2).unwrap());
    assert!(!index.check_match(&qb, 3).unwrap());
    assert!(index.check_match(&qb, 4).unwrap());
    assert!(!index.check_match(&qb, 5).unwrap());

    // "quick blue" should match only ID 5 (phrase within one element)
    let qbl = index.parse_phrase_query("quick blue", &hw).unwrap();
    assert!(qbl.is_some(), "query tokens must exist");
    let qbl = qbl.unwrap();

    let mut results: Vec<_> = index.filter_query(qbl.clone(), &hw).unwrap().collect();
    results.sort();
    assert_eq!(
        results,
        vec![5],
        "\"quick blue\" must NOT match ID 3 (words in separate array elements)"
    );

    assert!(!index.check_match(&qbl, 1).unwrap());
    assert!(!index.check_match(&qbl, 2).unwrap());
    assert!(!index.check_match(&qbl, 3).unwrap());
    assert!(!index.check_match(&qbl, 4).unwrap());
    assert!(index.check_match(&qbl, 5).unwrap());
}

/// Single-element arrays and plain strings should still work normally.
#[rstest]
fn test_phrase_matching_single_element_array(
    #[values(IndexType::MutableGridstore, IndexType::ImmMmap, IndexType::ImmRamMmap)]
    index_type: IndexType,
) {
    let hw = HardwareCounterCell::new();
    let (mut builder, _temp_dir, _db) = create_builder(index_type, true);

    let p1 = serde_json::json!(["the quick brown fox"]);
    let p2 = serde_json::json!("the quick brown fox");
    let p3 = serde_json::json!(["the", "quick brown fox"]);

    builder.add_point(1, &[&p1], &hw).unwrap();
    builder.add_point(2, &[&p2], &hw).unwrap();
    builder.add_point(3, &[&p3], &hw).unwrap();

    let index = builder.finalize().unwrap();

    let q = index
        .parse_phrase_query("quick brown", &hw)
        .unwrap()
        .unwrap();

    let mut results: Vec<_> = index.filter_query(q, &hw).unwrap().collect();
    results.sort();
    assert_eq!(results, vec![1, 2, 3]);
}
