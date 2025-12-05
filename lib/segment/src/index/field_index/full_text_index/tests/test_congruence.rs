use std::collections::HashSet;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use serde_json::Value;
use tempfile::{Builder, TempDir};

use crate::common::operation_error::OperationResult;
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
use crate::data_types::index::TextIndexParams;
use crate::fixtures::payload_fixtures::random_full_text_payload;
use crate::index::field_index::field_index_base::PayloadFieldIndex;
use crate::index::field_index::full_text_index::inverted_index::{
    Document, InvertedIndex, ParsedQuery, TokenId, TokenSet,
};
use crate::index::field_index::full_text_index::mmap_text_index::FullTextMmapIndexBuilder;
use crate::index::field_index::full_text_index::mutable_text_index::MutableFullTextIndex;
#[cfg(feature = "rocksdb")]
use crate::index::field_index::full_text_index::text_index::FullTextIndexRocksDbBuilder;
use crate::index::field_index::full_text_index::text_index::{
    FullTextGridstoreIndexBuilder, FullTextIndex,
};
use crate::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};
use crate::json_path::JsonPath;
use crate::types::{FieldCondition, ValuesCount};

#[cfg(feature = "rocksdb")]
type Database = std::sync::Arc<parking_lot::RwLock<rocksdb::DB>>;
#[cfg(not(feature = "rocksdb"))]
type Database = ();

const FIELD_NAME: &str = "test";
const TYPES: &[IndexType] = &[
    #[cfg(feature = "rocksdb")]
    IndexType::MutableRocksdb,
    IndexType::MutableGridstore,
    #[cfg(feature = "rocksdb")]
    IndexType::ImmRamRocksDb,
    IndexType::ImmMmap,
    IndexType::ImmRamMmap,
];

#[derive(Clone, Copy, PartialEq, Debug)]
enum IndexType {
    #[cfg(feature = "rocksdb")]
    MutableRocksdb,
    MutableGridstore,
    #[cfg(feature = "rocksdb")]
    ImmRamRocksDb,
    ImmMmap,
    ImmRamMmap,
}

enum IndexBuilder {
    #[cfg(feature = "rocksdb")]
    MutableRocksdb(FullTextIndexRocksDbBuilder),
    MutableGridstore(FullTextGridstoreIndexBuilder),
    #[cfg(feature = "rocksdb")]
    ImmRamRocksdb(FullTextIndexRocksDbBuilder),
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
            #[cfg(feature = "rocksdb")]
            IndexBuilder::MutableRocksdb(builder) => builder.add_point(id, payload, hw_counter),
            IndexBuilder::MutableGridstore(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
            }
            #[cfg(feature = "rocksdb")]
            IndexBuilder::ImmRamRocksdb(builder) => builder.add_point(id, payload, hw_counter),
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
            #[cfg(feature = "rocksdb")]
            IndexBuilder::MutableRocksdb(builder) => builder.finalize(),
            IndexBuilder::MutableGridstore(builder) => builder.finalize(),
            #[cfg(feature = "rocksdb")]
            IndexBuilder::ImmRamRocksdb(builder) => builder.finalize(),
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
    #[cfg(feature = "rocksdb")]
    let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
    #[cfg(not(feature = "rocksdb"))]
    let db = ();

    let config = TextIndexParams {
        phrase_matching: Some(phrase_matching),
        ..TextIndexParams::default()
    };

    let mut builder = match index_type {
        #[cfg(feature = "rocksdb")]
        IndexType::MutableRocksdb => IndexBuilder::MutableRocksdb(
            FullTextIndex::builder_rocksdb(db.clone(), config, FIELD_NAME, true).unwrap(),
        ),
        IndexType::MutableGridstore => IndexBuilder::MutableGridstore(
            FullTextIndex::builder_gridstore(temp_dir.path().to_path_buf(), config),
        ),
        #[cfg(feature = "rocksdb")]
        IndexType::ImmRamRocksDb => IndexBuilder::ImmRamRocksdb(
            FullTextIndex::builder_rocksdb(db.clone(), config, FIELD_NAME, false).unwrap(),
        ),
        IndexType::ImmMmap => IndexBuilder::ImmMmap(FullTextIndex::builder_mmap(
            temp_dir.path().to_path_buf(),
            config,
            true,
        )),
        IndexType::ImmRamMmap => IndexBuilder::ImmRamMmap(FullTextIndex::builder_mmap(
            temp_dir.path().to_path_buf(),
            config,
            false,
        )),
    };
    match &mut builder {
        #[cfg(feature = "rocksdb")]
        IndexBuilder::MutableRocksdb(builder) => builder.init().unwrap(),
        IndexBuilder::MutableGridstore(builder) => builder.init().unwrap(),
        #[cfg(feature = "rocksdb")]
        IndexBuilder::ImmRamRocksdb(builder) => builder.init().unwrap(),
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
) -> FullTextIndex {
    let config = TextIndexParams {
        phrase_matching: Some(phrase_matching),
        ..TextIndexParams::default()
    };

    // Drop the original index to ensure files are flushed
    drop(index);

    // Reopen based on index type
    match index_type {
        #[cfg(feature = "rocksdb")]
        IndexType::MutableRocksdb => {
            FullTextIndex::new_rocksdb(db.clone(), config, FIELD_NAME, true, false)
                .unwrap()
                .expect("Failed to reopen MutableRocksdb index")
        }
        IndexType::MutableGridstore => {
            FullTextIndex::new_gridstore(temp_dir.path().to_path_buf(), config, false)
                .unwrap()
                .expect("Failed to reopen MutableGridstore index")
        }
        #[cfg(feature = "rocksdb")]
        IndexType::ImmRamRocksDb => {
            FullTextIndex::new_rocksdb(db.clone(), config, FIELD_NAME, false, false)
                .unwrap()
                .expect("Failed to reopen ImmRamRocksDb index")
        }
        IndexType::ImmMmap => {
            // Reopen with is_on_disk = true (mmap directly)
            FullTextIndex::new_mmap(temp_dir.path().to_path_buf(), config, true)
                .unwrap()
                .expect("Failed to reopen ImmMmap index")
        }
        IndexType::ImmRamMmap => {
            // Reopen with is_on_disk = false (load into RAM)
            // This is the path that will call ImmutableFullTextIndex::open_mmap
            FullTextIndex::new_mmap(temp_dir.path().to_path_buf(), config, false)
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

        index.flush_all().expect("failed to flush deletions");
    }

    // Reopen the index if requested
    let index = if reopen {
        reopen_index(index, index_type, &temp_dir, &db, phrase_matching)
    } else {
        index
    };

    (index, temp_dir, db)
}

/// Tries to parse a query. If there is an unknown id to a token, returns `None`
pub fn to_parsed_query(
    query: &[String],
    is_phrase: bool,
    token_to_id: impl Fn(&str) -> Option<TokenId>,
) -> Option<ParsedQuery> {
    let tokens = query.iter().map(|token| token_to_id(token.as_str()));

    let parsed = match is_phrase {
        false => ParsedQuery::AllTokens(tokens.collect::<Option<TokenSet>>()?),
        true => ParsedQuery::Phrase(tokens.collect::<Option<Document>>()?),
    };

    Some(parsed)
}

pub fn parse_query(query: &[String], is_phrase: bool, index: &FullTextIndex) -> ParsedQuery {
    let hw_counter = HardwareCounterCell::disposable();
    match index {
        FullTextIndex::Mutable(index) => {
            let token_to_id = |token: &str| index.inverted_index.get_token_id(token, &hw_counter);
            to_parsed_query(query, is_phrase, token_to_id).unwrap()
        }
        FullTextIndex::Immutable(index) => {
            let token_to_id = |token: &str| index.inverted_index.get_token_id(token, &hw_counter);
            to_parsed_query(query, is_phrase, token_to_id).unwrap()
        }
        FullTextIndex::Mmap(index) => {
            let token_to_id = |token: &str| index.inverted_index.get_token_id(token, &hw_counter);
            to_parsed_query(query, is_phrase, token_to_id).unwrap()
        }
    }
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

        assert_eq!(
            index_a.get_token("doesnotexist", &hw_counter),
            index_b.get_token("doesnotexist", &hw_counter),
        );
        assert!(
            index_a.get_token(&keywords[0], &hw_counter).is_some()
                == index_b.get_token(&keywords[0], &hw_counter).is_some(),
        );

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
                    index_a.check_match(&parsed_query_a, point_id),
                    index_b.check_match(&parsed_query_b, point_id),
                );
            }

            assert_eq!(
                index_a
                    .filter_query(parsed_query_a, &hw_counter)
                    .collect::<HashSet<_>>(),
                index_b
                    .filter_query(parsed_query_b, &hw_counter)
                    .collect::<HashSet<_>>(),
            );
        }

        if phrase_matching {
            for phrase in &existing_phrases {
                eprintln!("Phrase: {phrase:?}");

                let parsed_query_a = parse_query(phrase, true, index_a);
                let parsed_query_b = parse_query(phrase, true, index_b);

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

                for point_id in 0..POINT_COUNT as PointOffsetType {
                    assert_eq!(
                        index_a.check_match(&parsed_query_a, point_id),
                        index_b.check_match(&parsed_query_b, point_id),
                    );
                }

                // Assert that both indices return the same results
                assert_eq!(
                    index_a
                        .filter_query(parsed_query_a, &hw_counter)
                        .collect::<HashSet<_>>(),
                    index_b
                        .filter_query(parsed_query_b, &hw_counter)
                        .collect::<HashSet<_>>(),
                );
            }
        }

        if !deleted {
            for threshold in 1..=10 {
                assert_eq!(
                    index_a
                        .payload_blocks(threshold, JsonPath::new(FIELD_NAME))
                        .count(),
                    index_b
                        .payload_blocks(threshold, JsonPath::new(FIELD_NAME))
                        .count(),
                );
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
    // From the ids, choose a random phrase of 4 words.
    const PHRASE_LENGTH: usize = 4;
    let mut phrases = Vec::new();
    let rng = &mut StdRng::seed_from_u64(43);
    for id in existing_ids {
        let doc = mutable_index.get_doc(*id).unwrap();
        let rand_idx = rng.random_range(0..=KEYWORD_COUNT - PHRASE_LENGTH);
        let phrase = doc[rand_idx..rand_idx + PHRASE_LENGTH].to_vec();

        phrases.push(phrase);
    }

    let hw_counter = HardwareCounterCell::disposable();

    for (index, index_type) in check_indexes {
        eprintln!("Checking index type: {index_type:?}");
        for (phrase, exp_id) in phrases.iter().zip(existing_ids) {
            eprintln!("Phrase: {phrase:?}");

            let parsed_query = parse_query(phrase, phrase_matching, index);

            assert!(index.check_match(&parsed_query, *exp_id));

            let result = index
                .filter_query(parsed_query, &hw_counter)
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
