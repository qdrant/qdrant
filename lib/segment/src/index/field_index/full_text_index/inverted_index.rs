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

    use rand::Rng;

    use super::InvertedIndex;
    use crate::index::field_index::full_text_index::immutable_inverted_index::ImmutableInvertedIndex;
    use crate::index::field_index::full_text_index::mutable_inverted_index::MutableInvertedIndex;

    fn mutable_inverted_index() -> InvertedIndex {
        let mut index = InvertedIndex::Mutable(MutableInvertedIndex::default());

        let indexed_points = 2000;

        // 20% of indexed points get removed
        let deleted_points = 400;

        for idx in 0..indexed_points {
            // Generate 10-word documents
            let tokens: BTreeSet<String> = (0..10)
                .map(|_| {
                    let mut rng = rand::thread_rng();
                    // Each word is 1 to 3 characters long
                    let len = rng.gen_range(1..=3);
                    rng.sample_iter(rand::distributions::Alphanumeric)
                        .take(len)
                        .map(char::from)
                        .collect()
                })
                .collect();
            let document = index.document_from_tokens(&tokens);
            index.index_document(idx, document).unwrap();
        }

        // Remove some points
        for idx in 0..deleted_points {
            index.remove_document(idx);
        }

        index
    }

    #[test]
    fn test_mutable_to_immutable() {
        let InvertedIndex::Mutable(index) = mutable_inverted_index() else {
            panic!("Expected mutable index");
        };

        let (orig_vocab, orig_postings) = (index.vocab.clone(), index.postings.clone());

        let index = ImmutableInvertedIndex::from(index);

        assert!(index.vocab.len() < orig_vocab.len());
        assert!(index.postings.len() < orig_postings.len());
        assert!(!index.vocab.is_empty());

        // Check that new vocabulary token ids leads to the same posting lists
        assert!({
            index.vocab.iter().all(|(key, new_token)| {
                let new_posting = index.postings.get(*new_token as usize).cloned().unwrap();

                let orig_token = orig_vocab.get(key).unwrap();

                let orig_posting = orig_postings
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
}
