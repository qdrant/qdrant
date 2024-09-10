use std::collections::{BTreeSet, HashMap};

use common::types::PointOffsetType;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::immutable_inverted_index::ImmutableInvertedIndex;
use crate::index::field_index::full_text_index::mutable_inverted_index::MutableInvertedIndex;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, PayloadKeyType};

pub type TokenId = u32;

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct Document {
    tokens: Vec<TokenId>,
}

impl Document {
    pub fn new(mut tokens: Vec<TokenId>) -> Self {
        tokens.sort_unstable();
        Self { tokens }
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    pub fn tokens(&self) -> &[TokenId] {
        &self.tokens
    }

    pub fn check(&self, token: TokenId) -> bool {
        self.tokens.binary_search(&token).is_ok()
    }
}

#[derive(Debug)]
pub struct ParsedQuery {
    pub tokens: Vec<Option<TokenId>>,
}

impl ParsedQuery {
    pub fn check_match(&self, document: &Document) -> bool {
        if self.tokens.contains(&None) {
            return false;
        }
        // Check that all tokens are in document
        self.tokens
            .iter()
            // unwrap crash safety: all tokens exist in the vocabulary if it passes the above check
            .all(|query_token| document.check(query_token.unwrap()))
    }
}

pub enum InvertedIndex {
    Mutable(MutableInvertedIndex),
    Immutable(ImmutableInvertedIndex),
}

impl InvertedIndex {
    pub fn new(is_appendable: bool) -> InvertedIndex {
        if is_appendable {
            InvertedIndex::Mutable(MutableInvertedIndex::default())
        } else {
            InvertedIndex::Immutable(ImmutableInvertedIndex::default())
        }
    }

    pub fn document_from_tokens(&mut self, tokens: &BTreeSet<String>) -> Document {
        let vocab = match self {
            InvertedIndex::Mutable(index) => &mut index.vocab,
            InvertedIndex::Immutable(index) => &mut index.vocab,
        };
        Self::document_from_tokens_impl(vocab, tokens)
    }

    pub fn document_from_tokens_impl(
        vocab: &mut HashMap<String, TokenId>,
        tokens: &BTreeSet<String>,
    ) -> Document {
        let mut document_tokens = vec![];
        for token in tokens {
            // check if in vocab
            let vocab_idx = match vocab.get(token) {
                Some(&idx) => idx,
                None => {
                    let next_token_id = vocab.len() as TokenId;
                    vocab.insert(token.to_string(), next_token_id);
                    next_token_id
                }
            };
            document_tokens.push(vocab_idx);
        }

        Document::new(document_tokens)
    }

    pub fn index_document(
        &mut self,
        idx: PointOffsetType,
        document: Document,
    ) -> OperationResult<()> {
        match self {
            InvertedIndex::Mutable(index) => {
                index.index_document(idx, document);
                Ok(())
            }
            InvertedIndex::Immutable(_index) => Err(OperationError::service_error(
                "Can't add values to immutable text index",
            )),
        }
    }

    pub fn remove_document(&mut self, idx: PointOffsetType) -> bool {
        match self {
            InvertedIndex::Mutable(index) => index.remove_document(idx),
            InvertedIndex::Immutable(index) => index.remove_document(idx),
        }
    }

    pub fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            InvertedIndex::Mutable(index) => index.filter(query),
            InvertedIndex::Immutable(index) => index.filter(query),
        }
    }

    pub fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> CardinalityEstimation {
        let points_count = match self {
            InvertedIndex::Mutable(index) => index.points_count,
            InvertedIndex::Immutable(index) => index.points_count,
        };
        let posting_lengths: Option<Vec<usize>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| match vocab_idx {
                None => None,
                // unwrap safety: same as in filter()
                Some(idx) => match &self {
                    Self::Mutable(index) => index
                        .postings
                        .get(idx as usize)
                        .unwrap()
                        .as_ref()
                        .map(|p| p.len()),
                    Self::Immutable(index) => index.postings.get(idx as usize).map(|p| p.len()),
                },
            })
            .collect();
        if posting_lengths.is_none() || points_count == 0 {
            // There are unseen tokens -> no matches
            return CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0,
                exp: 0,
                max: 0,
            };
        }
        let postings = posting_lengths.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0,
                exp: 0,
                max: 0,
            };
        }
        // Smallest posting is the largest possible cardinality
        let smallest_posting = postings.iter().min().copied().unwrap();

        return if postings.len() == 1 {
            CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
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
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0, // ToDo: make better estimation
                exp,
                max: smallest_posting,
            }
        };
    }

    pub fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
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
        match &self {
            InvertedIndex::Mutable(index) => Box::new(
                index
                    .vocab_with_postings_len_iter()
                    .filter_map(map_filter_condition),
            ),
            InvertedIndex::Immutable(index) => Box::new(
                index
                    .vocab_with_postings_len_iter()
                    .filter_map(map_filter_condition),
            ),
        }
    }

    pub fn build_index(
        &mut self,
        iter: impl Iterator<Item = OperationResult<(PointOffsetType, BTreeSet<String>)>>,
    ) -> OperationResult<()> {
        let mut index = MutableInvertedIndex::default();
        index.build_index(iter)?;

        match self {
            InvertedIndex::Mutable(i) => {
                *i = index;
            }
            InvertedIndex::Immutable(i) => {
                *i = index.into();
            }
        }

        Ok(())
    }

    pub fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        match self {
            InvertedIndex::Mutable(index) => index.check_match(parsed_query, point_id),
            InvertedIndex::Immutable(index) => index.check_match(parsed_query, point_id),
        }
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            InvertedIndex::Mutable(index) => index.values_is_empty(point_id),
            InvertedIndex::Immutable(index) => index.values_is_empty(point_id),
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            InvertedIndex::Mutable(index) => index.values_count(point_id),
            InvertedIndex::Immutable(index) => index.values_count(point_id),
        }
    }

    pub fn points_count(&self) -> usize {
        match self {
            InvertedIndex::Mutable(index) => index.points_count,
            InvertedIndex::Immutable(index) => index.points_count,
        }
    }

    pub fn get_token(&self, token: &str) -> Option<TokenId> {
        match self {
            InvertedIndex::Mutable(index) => index.vocab.get(token).copied(),
            InvertedIndex::Immutable(index) => index.vocab.get(token).copied(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rand::seq::SliceRandom;
    use rand::Rng;
    use rstest::rstest;

    use super::{InvertedIndex, ParsedQuery, TokenId};
    use crate::index::field_index::full_text_index::immutable_inverted_index::ImmutableInvertedIndex;
    use crate::index::field_index::full_text_index::mmap_inverted_index::MmapInvertedIndex;
    use crate::index::field_index::full_text_index::mutable_inverted_index::MutableInvertedIndex;

    fn generate_word() -> String {
        let mut rng = rand::thread_rng();

        // Each word is 1 to 3 characters long
        let len = rng.gen_range(1..=3);
        rng.sample_iter(rand::distributions::Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    fn generate_query() -> Vec<String> {
        let mut rng = rand::thread_rng();
        let len = rng.gen_range(1..=2);
        (0..len).map(|_| generate_word()).collect()
    }

    fn to_parsed_query(
        query: Vec<String>,
        token_to_id: impl Fn(String) -> Option<TokenId>,
    ) -> ParsedQuery {
        let tokens: Vec<_> = query.into_iter().map(token_to_id).collect();
        ParsedQuery { tokens }
    }

    fn mutable_inverted_index(indexed_count: u32, deleted_count: u32) -> MutableInvertedIndex {
        let mut index = InvertedIndex::Mutable(MutableInvertedIndex::default());

        for idx in 0..indexed_count {
            // Generate 10 tot 30-word documents
            let doc_len = rand::thread_rng().gen_range(10..=30);
            let tokens: BTreeSet<String> = (0..doc_len).map(|_| generate_word()).collect();
            let document = index.document_from_tokens(&tokens);
            index.index_document(idx, document).unwrap();
        }

        // Remove some points
        let mut points_to_delete = (0..indexed_count).collect::<Vec<_>>();
        points_to_delete.shuffle(&mut rand::thread_rng());
        for idx in &points_to_delete[..deleted_count as usize] {
            index.remove_document(*idx);
        }

        let InvertedIndex::Mutable(index) = index else {
            panic!("Expected mutable index");
        };

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

                let orig_posting = mutable
                    .postings
                    .get(*orig_token as usize)
                    .cloned()
                    .unwrap()
                    .unwrap();

                let new_contains_orig = orig_posting
                    .iter()
                    .all(|point_id| new_posting.contains(&point_id));

                let orig_contains_new = new_posting
                    .iter()
                    .all(|point_id| orig_posting.contains(&point_id));

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
    #[test]
    fn test_immutable_to_mmap(#[case] indexed_count: u32, #[case] deleted_count: u32) {
        let mutable = mutable_inverted_index(indexed_count, deleted_count);
        let immutable = ImmutableInvertedIndex::from(mutable);

        let path = tempfile::tempdir().unwrap().into_path();

        MmapInvertedIndex::create(path.clone(), immutable.clone()).unwrap();

        let mmap = MmapInvertedIndex::open(path).unwrap();

        // Check same vocabulary
        for (token, token_id) in immutable.vocab.iter() {
            assert_eq!(mmap.get_token_id(token).unwrap(), Some(*token_id));
        }

        // Check same postings
        for (token_id, posting) in immutable.postings.iter().enumerate() {
            let chunk_reader = mmap.postings.get(token_id as u32).unwrap();

            for point_id in posting.iter() {
                assert!(chunk_reader.contains(&point_id));
            }
        }

        for (point_id, count) in immutable.point_to_tokens_count.iter().enumerate() {
            // Check same deleted points
            assert_eq!(
                mmap.deleted_points.get(point_id).unwrap(),
                count.is_none(),
                "point_id: {point_id}"
            );

            // Check same count
            assert_eq!(
                *mmap.point_to_tokens_count.get(point_id).unwrap(),
                count.unwrap_or(0)
            );
        }

        // Check same points count
        assert_eq!(mmap.active_points_count, immutable.points_count);
    }

    #[test]
    fn test_mmap_index_congruence() {
        let indexed_count = 10000;
        let deleted_count = 500;

        let mut mutable = mutable_inverted_index(indexed_count, deleted_count);
        let immutable = ImmutableInvertedIndex::from(mutable.clone());

        let path = tempfile::tempdir().unwrap().into_path();

        MmapInvertedIndex::create(path.clone(), immutable.clone()).unwrap();

        let mut mmap_index = MmapInvertedIndex::open(path).unwrap();

        let queries: Vec<_> = (0..100).map(|_| generate_query()).collect();

        let mut_parsed_queries: Vec<_> = queries
            .clone()
            .into_iter()
            .map(|query| to_parsed_query(query, |token| mutable.vocab.get(&token).copied()))
            .collect();

        let imm_parsed_queries: Vec<_> = queries
            .into_iter()
            .map(|query| to_parsed_query(query, |token| mmap_index.get_token_id(&token).unwrap()))
            .collect();

        for (mut_query, imm_query) in mut_parsed_queries.iter().zip(imm_parsed_queries.iter()) {
            let mut_filtered = mutable.filter(mut_query).collect::<Vec<_>>();
            let imm_filtered = mmap_index.filter(imm_query).collect::<Vec<_>>();

            assert_eq!(mut_filtered, imm_filtered);
        }

        // Delete random documents from both indexes

        let points_to_delete: Vec<_> = (0..deleted_count)
            .map(|_| rand::thread_rng().gen_range(0..indexed_count))
            .collect();

        for point_id in &points_to_delete {
            mutable.remove_document(*point_id);
            mmap_index.remove_document(*point_id);
        }

        // Check congruence after deletion

        for (mut_query, imm_query) in mut_parsed_queries.iter().zip(imm_parsed_queries.iter()) {
            let mut_filtered = mutable.filter(mut_query).collect::<Vec<_>>();
            let imm_filtered = mmap_index.filter(imm_query).collect::<Vec<_>>();

            assert_eq!(mut_filtered, imm_filtered);
        }
    }
}
