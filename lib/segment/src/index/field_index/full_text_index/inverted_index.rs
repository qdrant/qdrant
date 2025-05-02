use std::collections::HashMap;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, PayloadKeyType};

pub type TokenId = u32;

/// Contains the set of tokens that are in a document.
///
/// Internally, it keeps them unique and sorted, so that we can binary-search over them
#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct TokenSet(Vec<TokenId>);

impl TokenSet {
    pub fn new(tokens: AHashSet<TokenId>) -> Self {
        let sorted_unique = tokens.into_iter().sorted_unstable().collect();

        Self(sorted_unique)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn tokens(&self) -> &[TokenId] {
        &self.0
    }

    pub fn contains(&self, token: &TokenId) -> bool {
        self.0.binary_search(token).is_ok()
    }
}

impl FromIterator<TokenId> for TokenSet {
    fn from_iter<T: IntoIterator<Item = TokenId>>(iter: T) -> Self {
        let tokens = iter
            .into_iter()
            .sorted_unstable()
            .dedup()
            .collect::<Vec<_>>();

        Self(tokens)
    }
}

/// Contains the token ids that make up a document, in the same order that appear in the document.
#[derive(Clone)]
pub struct Document(Vec<TokenId>);

impl Document {
    pub fn new(tokens: Vec<TokenId>) -> Self {
        Self(tokens)
    }
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub tokens: Vec<TokenId>,
}

impl ParsedQuery {
    pub fn check_match(&self, tokens: &TokenSet) -> bool {
        if self.tokens.is_empty() {
            return false;
        }

        // Check that all tokens are in document
        self.tokens
            .iter()
            .all(|query_token| tokens.contains(query_token))
    }
}

pub trait InvertedIndex {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId>;

    /// Translate the string tokens into token ids.
    /// If it is an unseen token, it is added to the vocabulary and a new token id is generated.
    ///
    /// The order of the tokens is preserved.
    fn token_ids<'a>(
        &mut self,
        str_tokens: impl IntoIterator<Item = &'a String> + 'a,
    ) -> Vec<TokenId> {
        let vocab = self.get_vocab_mut();
        let mut token_ids = vec![];
        for token in str_tokens {
            // check if in vocab
            let vocab_idx = match vocab.get(token) {
                Some(&idx) => idx,
                None => {
                    let next_token_id = vocab.len() as TokenId;
                    vocab.insert(token.to_string(), next_token_id);
                    next_token_id
                }
            };
            token_ids.push(vocab_idx);
        }

        token_ids
    }

    fn index_tokens(
        &mut self,
        idx: PointOffsetType,
        tokens: TokenSet,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn index_document(
        &mut self,
        idx: PointOffsetType,
        document: Document,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn remove(&mut self, idx: PointOffsetType) -> bool;

    fn filter<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a>;

    fn get_posting_len(&self, token_id: TokenId, hw_counter: &HardwareCounterCell)
    -> Option<usize>;

    fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let points_count = self.points_count();

        let posting_lengths: Option<Vec<usize>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| self.get_posting_len(vocab_idx, hw_counter))
            .collect();
        if posting_lengths.is_none() || points_count == 0 {
            // There are unseen tokens -> no matches
            return CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
                min: 0,
                exp: 0,
                max: 0,
            };
        }
        let postings = posting_lengths.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
                min: 0,
                exp: 0,
                max: 0,
            };
        }
        // Smallest posting is the largest possible cardinality
        let smallest_posting = postings.iter().min().copied().unwrap();

        if postings.len() == 1 {
            CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
                min: smallest_posting,
                exp: smallest_posting,
                max: smallest_posting,
            }
        } else {
            let expected_frac: f64 = postings
                .iter()
                .map(|posting| *posting as f64 / points_count as f64)
                .product();
            let exp = (expected_frac * points_count as f64) as usize;
            CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(Box::new(condition.clone()))],
                min: 0, // ToDo: make better estimation
                exp,
                max: smallest_posting,
            }
        }
    }

    fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_;

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> impl Iterator<Item = PayloadBlockCondition> + '_ {
        let map_filter_condition = move |(token, postings_len): (&str, usize)| {
            if postings_len >= threshold {
                Some(PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), Match::new_text(token)),
                    cardinality: postings_len,
                })
            } else {
                None
            }
        };

        // It might be very hard to predict possible combinations of conditions,
        // so we only build it for individual tokens
        self.vocab_with_postings_len_iter()
            .filter_map(map_filter_condition)
    }

    fn check_match(
        &self,
        parsed_query: &ParsedQuery,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> bool;

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool;

    fn values_count(&self, point_id: PointOffsetType) -> usize;

    fn points_count(&self) -> usize;

    fn get_token_id(&self, token: &str, hw_counter: &HardwareCounterCell) -> Option<TokenId>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use common::counter::hardware_counter::HardwareCounterCell;
    use rand::Rng;
    use rand::seq::SliceRandom;
    use rstest::rstest;

    use super::{InvertedIndex, ParsedQuery, TokenId, TokenSet};
    use crate::index::field_index::full_text_index::immutable_inverted_index::ImmutableInvertedIndex;
    use crate::index::field_index::full_text_index::mmap_inverted_index::MmapInvertedIndex;
    use crate::index::field_index::full_text_index::mutable_inverted_index::MutableInvertedIndex;

    fn generate_word() -> String {
        let mut rng = rand::rng();

        // Each word is 1 to 3 characters long
        let len = rng.random_range(1..=3);
        rng.sample_iter(rand::distr::Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    fn generate_query() -> Vec<String> {
        let mut rng = rand::rng();
        let len = rng.random_range(1..=2);
        (0..len).map(|_| generate_word()).collect()
    }

    /// Tries to parse a query. If there is an unknown id to a token, returns `None`
    fn to_parsed_query(
        query: Vec<String>,
        token_to_id: impl Fn(String) -> Option<TokenId>,
    ) -> Option<ParsedQuery> {
        let tokens = query
            .into_iter()
            .map(token_to_id)
            .collect::<Option<Vec<_>>>()?;
        Some(ParsedQuery { tokens })
    }

    fn mutable_inverted_index(indexed_count: u32, deleted_count: u32) -> MutableInvertedIndex {
        let mut index = MutableInvertedIndex::default();

        let hw_counter = HardwareCounterCell::new();

        for idx in 0..indexed_count {
            // Generate 10 tot 30-word documents
            let doc_len = rand::rng().random_range(10..=30);
            let tokens: BTreeSet<String> = (0..doc_len).map(|_| generate_word()).collect();
            let token_ids = index.token_ids(&tokens);
            let token_set = TokenSet::from_iter(token_ids);
            index.index_tokens(idx, token_set, &hw_counter).unwrap();
        }

        // Remove some points
        let mut points_to_delete = (0..indexed_count).collect::<Vec<_>>();
        points_to_delete.shuffle(&mut rand::rng());
        for idx in &points_to_delete[..deleted_count as usize] {
            index.remove(*idx);
        }

        index
    }

    #[test]
    fn test_mutable_to_immutable() {
        let mutable = mutable_inverted_index(2000, 400);

        let immutable = ImmutableInvertedIndex::from(mutable.clone());

        assert!(immutable.vocab.len() < mutable.vocab.len());
        assert!(immutable.postings.len() < mutable.postings.len());
        assert!(!immutable.vocab.is_empty());

        // Check that new vocabulary token ids leads to the same posting lists
        assert!({
            immutable.vocab.iter().all(|(key, new_token)| {
                let new_posting = immutable
                    .postings
                    .get(*new_token as usize)
                    .cloned()
                    .unwrap();

                let orig_token = mutable.vocab.get(key).unwrap();

                let orig_posting = mutable.postings.get(*orig_token as usize).cloned().unwrap();

                let mut posting_visitor = new_posting.visitor();
                let new_contains_orig = orig_posting
                    .iter()
                    .all(|point_id| posting_visitor.contains(point_id));

                let orig_contains_new = new_posting
                    .iter()
                    .map(|elem| elem.id)
                    .all(|point_id| orig_posting.contains(point_id));

                new_contains_orig && orig_contains_new
            })
        });
    }

    #[rstest]
    #[case(2000, 400)]
    #[case(2000, 2000)]
    #[case(1111, 1110)]
    #[case(1111, 0)]
    #[case(10, 2)]
    #[case(0, 0)]
    #[test]
    fn test_immutable_to_mmap_to_immutable(#[case] indexed_count: u32, #[case] deleted_count: u32) {
        use std::collections::HashSet;

        let mutable = mutable_inverted_index(indexed_count, deleted_count);
        let immutable = ImmutableInvertedIndex::from(mutable);

        let mmap_dir = tempfile::tempdir().unwrap();

        let hw_counter = HardwareCounterCell::new();

        MmapInvertedIndex::create(mmap_dir.path().into(), immutable.clone()).unwrap();
        let mmap = MmapInvertedIndex::open(mmap_dir.path().into(), false).unwrap();

        let imm_mmap = ImmutableInvertedIndex::from(&mmap);

        // Check same vocabulary
        for (token, token_id) in immutable.vocab.iter() {
            assert_eq!(mmap.get_token_id(token, &hw_counter), Some(*token_id));
            assert_eq!(imm_mmap.get_token_id(token, &hw_counter), Some(*token_id));
        }

        // Check same postings
        for (token_id, posting) in immutable.postings.iter().enumerate() {
            let mutable_elems = posting.iter().collect::<HashSet<_>>();

            // Check mutable vs mmap
            let mmap_elems = mmap
                .postings
                .get(token_id as u32, &hw_counter)
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(mutable_elems, mmap_elems);

            // Check mutable vs immutable mmap
            let imm_mmap_elems = imm_mmap.postings[token_id].iter().collect();
            assert_eq!(mutable_elems, imm_mmap_elems);
        }

        for (point_id, count) in immutable.point_to_tokens_count.iter().enumerate() {
            // Check same deleted points
            assert_eq!(
                mmap.deleted_points.get(point_id).unwrap(),
                count.is_none(),
                "point_id: {point_id}",
            );

            // Check same count
            assert_eq!(
                *mmap.point_to_tokens_count.get(point_id).unwrap(),
                count.unwrap_or(0)
            );
            assert_eq!(imm_mmap.point_to_tokens_count[point_id], *count);
        }

        // Check same points count
        assert_eq!(immutable.points_count, mmap.active_points_count);
        assert_eq!(immutable.points_count, imm_mmap.points_count);
    }

    #[test]
    fn test_mmap_index_congruence() {
        let indexed_count = 10000;
        let deleted_count = 500;

        let hw_counter = HardwareCounterCell::new();
        let mmap_dir = tempfile::tempdir().unwrap();

        let mut mut_index = mutable_inverted_index(indexed_count, deleted_count);

        let immutable = ImmutableInvertedIndex::from(mut_index.clone());
        MmapInvertedIndex::create(mmap_dir.path().into(), immutable).unwrap();
        let mut mmap_index = MmapInvertedIndex::open(mmap_dir.path().into(), false).unwrap();

        let mut imm_mmap_index = ImmutableInvertedIndex::from(&mmap_index);

        let queries: Vec<_> = (0..100).map(|_| generate_query()).collect();

        let mut_parsed_queries: Vec<_> = queries
            .iter()
            .cloned()
            .map(|query| to_parsed_query(query, |token| mut_index.vocab.get(&token).copied()))
            .collect();
        let mmap_parsed_queries: Vec<_> = queries
            .iter()
            .cloned()
            .map(|query| {
                to_parsed_query(query, |token| mmap_index.get_token_id(&token, &hw_counter))
            })
            .collect();
        let imm_mmap_parsed_queries: Vec<_> = queries
            .into_iter()
            .map(|query| {
                to_parsed_query(query, |token| {
                    imm_mmap_index.get_token_id(&token, &hw_counter)
                })
            })
            .collect();

        check_query_congruence(
            &mut_parsed_queries,
            &mmap_parsed_queries,
            &imm_mmap_parsed_queries,
            &mut_index,
            &mmap_index,
            &imm_mmap_index,
            &hw_counter,
        );

        // Delete random documents from both indexes
        let points_to_delete: Vec<_> = (0..deleted_count)
            .map(|_| rand::rng().random_range(0..indexed_count))
            .collect();
        for point_id in &points_to_delete {
            mut_index.remove(*point_id);
            mmap_index.remove(*point_id);
            imm_mmap_index.remove(*point_id);
        }

        // Check congruence after deletion
        check_query_congruence(
            &mut_parsed_queries,
            &mmap_parsed_queries,
            &imm_mmap_parsed_queries,
            &mut_index,
            &mmap_index,
            &imm_mmap_index,
            &hw_counter,
        );
    }

    fn check_query_congruence(
        mut_parsed_queries: &[Option<ParsedQuery>],
        mmap_parsed_queries: &[Option<ParsedQuery>],
        imm_mmap_parsed_queries: &[Option<ParsedQuery>],
        mut_index: &MutableInvertedIndex,
        mmap_index: &MmapInvertedIndex,
        imm_mmap_index: &ImmutableInvertedIndex,
        hw_counter: &HardwareCounterCell,
    ) {
        for queries in mut_parsed_queries.iter().cloned().zip(
            mmap_parsed_queries
                .iter()
                .cloned()
                .zip(imm_mmap_parsed_queries.iter().cloned()),
        ) {
            let (Some(mut_query), (Some(imm_query), Some(imm_mmap_query))) = queries else {
                // Immutable index can have a smaller vocabulary, since it only contains tokens that have
                // non-empty posting lists.
                // Since we removed some documents from the mutable index, it can happen that the immutable
                // index returns None when parsing the query, even if the mutable index returns Some.
                //
                // In this case both queries would filter to an empty set of documents.
                continue;
            };
            let mut_filtered = mut_index.filter(mut_query, hw_counter).collect::<Vec<_>>();
            let imm_filtered = mmap_index.filter(imm_query, hw_counter).collect::<Vec<_>>();
            let imm_mmap_filtered = imm_mmap_index
                .filter(imm_mmap_query, hw_counter)
                .collect::<Vec<_>>();

            assert_eq!(mut_filtered, imm_filtered);
            assert_eq!(imm_filtered, imm_mmap_filtered);
        }
    }
}
