use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use super::posting_list::PostingList;
use super::postings_iterator::intersect_postings_iterator;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, MatchText, PayloadKeyType, PointOffsetType};

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

pub(crate) trait InvertedIndex {
    type Document<'a>: Borrow<Document>
    where
        Self: 'a;
    fn document_from_tokens(&mut self, tokens: &BTreeSet<String>) -> OperationResult<Document>;
    fn index_document(&mut self, idx: PointOffsetType, document: Document) -> OperationResult<()>;
    fn remove_document(&mut self, idx: PointOffsetType) -> OperationResult<Option<()>>;
    fn filter(
        &self,
        query: &ParsedQuery,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>>;
    fn get_points_count(&self) -> usize;
    fn get_doc(&self, idx: PointOffsetType) -> Option<Self::Document<'_>>;
    fn get_token_id(&self, token: &str) -> OperationResult<Option<u32>>;
    fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation>;
}

#[derive(Default)]
pub struct InvertedIndexInMemory {
    postings: Vec<Option<PostingList>>,
    pub vocab: HashMap<String, TokenId>,
    pub point_to_docs: Vec<Option<Document>>,
    points_count: usize,
}

impl InvertedIndexInMemory {
    pub fn new() -> InvertedIndexInMemory {
        Default::default()
    }
    pub fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        // It might be very hard to predict possible combinations of conditions,
        // so we only build it for individual tokens
        Box::new(
            self.vocab
                .iter()
                .filter(|(_token, &posting_idx)| self.postings[posting_idx as usize].is_some())
                .filter(move |(_token, &posting_idx)| {
                    // unwrap crash safety: all tokens that passes the first filter should have postings
                    self.postings[posting_idx as usize].as_ref().unwrap().len() >= threshold
                })
                .map(|(token, &posting_idx)| {
                    (
                        token,
                        // same as the above case
                        self.postings[posting_idx as usize].as_ref().unwrap(),
                    )
                })
                .map(move |(token, posting)| PayloadBlockCondition {
                    condition: FieldCondition {
                        key: key.clone(),
                        r#match: Some(Match::Text(MatchText {
                            text: token.clone(),
                        })),
                        range: None,
                        geo_bounding_box: None,
                        geo_radius: None,
                        values_count: None,
                    },
                    cardinality: posting.len(),
                }),
        )
    }
}

impl InvertedIndex for InvertedIndexInMemory {
    type Document<'a> = &'a Document;
    fn document_from_tokens(&mut self, tokens: &BTreeSet<String>) -> OperationResult<Document> {
        let mut document_tokens = vec![];
        for token in tokens {
            // check if in vocab
            let vocab_idx = match self.vocab.get(token) {
                Some(&idx) => idx,
                None => {
                    let next_token_id = self.vocab.len() as TokenId;
                    self.vocab.insert(token.to_string(), next_token_id);
                    next_token_id
                }
            };
            document_tokens.push(vocab_idx);
        }

        Ok(Document::new(document_tokens))
    }

    fn index_document(&mut self, idx: PointOffsetType, document: Document) -> OperationResult<()> {
        self.points_count += 1;
        if self.point_to_docs.len() <= idx as usize {
            self.point_to_docs
                .resize(idx as usize + 1, Default::default());
        }

        for token_idx in document.tokens() {
            let token_idx_usize = *token_idx as usize;
            if self.postings.len() <= token_idx_usize {
                self.postings
                    .resize(token_idx_usize + 1, Default::default());
            }
            let posting = self
                .postings
                .get_mut(token_idx_usize)
                .expect("posting must exist even if with None");
            match posting {
                None => *posting = Some(PostingList::new(idx)),
                Some(vec) => vec.insert(idx),
            }
        }
        self.point_to_docs[idx as usize] = Some(document);
        Ok(())
    }

    fn remove_document(&mut self, idx: PointOffsetType) -> OperationResult<Option<()>> {
        if self.point_to_docs.len() <= idx as usize {
            return Ok(None); // Already removed or never actually existed
        }

        let removed_doc = match std::mem::take(&mut self.point_to_docs[idx as usize]) {
            Some(doc) => doc,
            None => return Ok(None),
        };

        self.points_count -= 1;

        for removed_token in removed_doc.tokens() {
            // unwrap safety: posting list exists and contains the document id
            let posting = self.postings.get_mut(*removed_token as usize).unwrap();
            if let Some(vec) = posting {
                vec.remove(idx);
            }
        }
        Ok(Some(()))
    }

    fn filter(
        &self,
        query: &ParsedQuery,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| match vocab_idx {
                None => None,
                // if a ParsedQuery token was given an index, then it must exist in the vocabulary
                // dictionary. Posting list entry can be None but it exists.
                Some(idx) => self.postings.get(idx as usize).unwrap().as_ref(),
            })
            .collect();
        if postings_opt.is_none() {
            // There are unseen tokens -> no matches
            return Ok(Box::new(vec![].into_iter()));
        }
        let postings = postings_opt.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return Ok(Box::new(vec![].into_iter()));
        }
        Ok(intersect_postings_iterator(postings))
    }

    fn get_points_count(&self) -> usize {
        self.points_count
    }

    fn get_doc(&self, idx: PointOffsetType) -> Option<Self::Document<'_>> {
        if let Some(doc) = self.point_to_docs.get(idx as usize) {
            doc.as_ref()
        } else {
            None
        }
    }

    fn get_token_id(&self, token: &str) -> OperationResult<Option<u32>> {
        Ok(self.vocab.get(token).copied())
    }

    fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| match vocab_idx {
                None => None,
                // unwrap safety: same as in filter()
                Some(idx) => self.postings.get(idx as usize).unwrap().as_ref(),
            })
            .collect();
        if postings_opt.is_none() {
            // There are unseen tokens -> no matches
            return Ok(CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0,
                exp: 0,
                max: 0,
            });
        }
        let postings = postings_opt.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return Ok(CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0,
                exp: 0,
                max: 0,
            });
        }
        // Smallest posting is the largest possible cardinality
        let smallest_posting = postings.iter().map(|posting| posting.len()).min().unwrap();

        return if postings.len() == 1 {
            Ok(CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: smallest_posting,
                exp: smallest_posting,
                max: smallest_posting,
            })
        } else {
            let expected_frac: f64 = postings
                .iter()
                .map(|posting| posting.len() as f64 / self.points_count as f64)
                .product();
            let exp = (expected_frac * self.points_count as f64) as usize;
            Ok(CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0, // ToDo: make better estimation
                exp,
                max: smallest_posting,
            })
        };
    }
}
