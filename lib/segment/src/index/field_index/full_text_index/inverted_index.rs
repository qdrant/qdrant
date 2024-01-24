use std::collections::{BTreeMap, BTreeSet, HashMap};

use common::types::PointOffsetType;
use serde::{Deserialize, Serialize};

use super::posting_list::PostingList;
use super::postings_iterator::intersect_postings_iterator;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, MatchText, PayloadKeyType};

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
            InvertedIndex::Mutable(MutableInvertedIndex::new())
        } else {
            InvertedIndex::Immutable(ImmutableInvertedIndex::new())
        }
    }

    pub fn index_tokens(
        &mut self,
        idx: PointOffsetType,
        tokens: &BTreeSet<String>,
    ) -> OperationResult<()> {
        match self {
            InvertedIndex::Mutable(index) => index.index_tokens(idx, tokens),
            InvertedIndex::Immutable(_index) => Err(OperationError::service_error(
                "Can't add values to immutable text index",
            )),
        }
    }

    pub fn build_index(
        &mut self,
        iter: impl Iterator<Item = OperationResult<(PointOffsetType, BTreeSet<String>)>>,
    ) -> OperationResult<()> {
        let mut index = MutableInvertedIndex::new();
        for i in iter {
            let (idx, tokens) = i?;
            index.index_tokens(idx, &tokens)?;
        }

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

    pub fn remove_document(&mut self, idx: PointOffsetType) -> Option<Document> {
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
        match self {
            InvertedIndex::Mutable(index) => index.estimate_cardinality(query, condition),
            InvertedIndex::Immutable(index) => index.estimate_cardinality(query, condition),
        }
    }

    pub fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        match self {
            InvertedIndex::Mutable(index) => index.payload_blocks(threshold, key),
            InvertedIndex::Immutable(index) => index.payload_blocks(threshold, key),
        }
    }

    pub fn get_points_count(&self) -> usize {
        match self {
            InvertedIndex::Mutable(index) => index.points_count,
            InvertedIndex::Immutable(index) => index.points_count,
        }
    }

    pub fn get_vocab(&self, token: &str) -> Option<TokenId> {
        match self {
            InvertedIndex::Mutable(index) => index.vocab.get(token).copied(),
            InvertedIndex::Immutable(index) => index.vocab.get_id(token),
        }
    }

    pub fn get_doc(&self, idx: PointOffsetType) -> Option<&Document> {
        match self {
            InvertedIndex::Mutable(index) => index.point_to_docs.get(idx as usize),
            InvertedIndex::Immutable(index) => index.point_to_docs.get(idx as usize),
        }?
        .as_ref()
    }
}

#[derive(Default)]
pub struct MutableInvertedIndex {
    postings: Vec<Option<PostingList>>,
    pub vocab: HashMap<String, TokenId>,
    pub point_to_docs: Vec<Option<Document>>,
    pub points_count: usize,
}

impl MutableInvertedIndex {
    pub fn new() -> MutableInvertedIndex {
        Default::default()
    }

    fn document_from_tokens(&mut self, tokens: &BTreeSet<String>) -> Document {
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

        Document::new(document_tokens)
    }

    pub fn index_tokens(
        &mut self,
        idx: PointOffsetType,
        tokens: &BTreeSet<String>,
    ) -> OperationResult<()> {
        let document = self.document_from_tokens(tokens);

        self.points_count += 1;
        if self.point_to_docs.len() <= idx as usize {
            self.point_to_docs
                .resize_with(idx as usize + 1, Default::default);
        }

        for token_idx in document.tokens() {
            let token_idx_usize = *token_idx as usize;
            if self.postings.len() <= token_idx_usize {
                self.postings
                    .resize_with(token_idx_usize + 1, Default::default);
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

    pub fn remove_document(&mut self, idx: PointOffsetType) -> Option<Document> {
        if self.point_to_docs.len() <= idx as usize {
            return None; // Already removed or never actually existed
        }

        let removed_doc = match std::mem::take(&mut self.point_to_docs[idx as usize]) {
            Some(doc) => doc,
            None => return None,
        };

        self.points_count -= 1;

        for removed_token in removed_doc.tokens() {
            // unwrap safety: posting list exists and contains the document id
            let posting = self.postings.get_mut(*removed_token as usize).unwrap();
            if let Some(vec) = posting {
                vec.remove(idx);
            }
        }
        Some(removed_doc)
    }

    pub fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
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
            return Box::new(vec![].into_iter());
        }
        let postings = postings_opt.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return Box::new(vec![].into_iter());
        }
        intersect_postings_iterator(postings)
    }

    pub fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> CardinalityEstimation {
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
            return CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0,
                exp: 0,
                max: 0,
            };
        }
        let postings = postings_opt.unwrap();
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
        let smallest_posting = postings.iter().map(|posting| posting.len()).min().unwrap();

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
                .map(|posting| posting.len() as f64 / self.points_count as f64)
                .product();
            let exp = (expected_frac * self.points_count as f64) as usize;
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
                        geo_polygon: None,
                        values_count: None,
                    },
                    cardinality: posting.len(),
                }),
        )
    }
}

#[derive(Default)]
pub struct ImmutableInvertedIndex {
    postings: Vec<Option<PostingList>>,
    vocab: FlattenedVocab,
    pub point_to_docs: Vec<Option<Document>>,
    pub points_count: usize,
}

#[derive(Default)]
struct FlattenedVocab {
    data: String,
    offsets: Vec<usize>,
}

impl FlattenedVocab {
    fn new(vocab: HashMap<String, TokenId>) -> (Self, Vec<TokenId>) {
        let vocab: BTreeMap<String, TokenId> = vocab.into_iter().collect();
        let mut offsets = vec![0];
        let mut data = String::new();
        let mut token_id_reindex = vec![0; vocab.len()];
        for (token, old_token_id) in vocab {
            token_id_reindex[old_token_id as usize] = offsets.len() as TokenId - 1;
            offsets.push(offsets.last().unwrap() + token.len());
            data.push_str(&token);
        }
        (Self { data, offsets }, token_id_reindex)
    }

    pub fn get_id(&self, token: &str) -> Option<TokenId> {
        let mut l = 0;
        let mut r = self.offsets.len() - 1;
        while l < r {
            let m = (l + r) / 2;
            let start = self.offsets[m];
            let end = self.offsets[m + 1];
            let token_in_vocab = &self.data[start..end];
            match token.cmp(token_in_vocab) {
                std::cmp::Ordering::Less => {
                    r = m - 1;
                }
                std::cmp::Ordering::Equal => return Some(m as TokenId),
                std::cmp::Ordering::Greater => {
                    l = m + 1;
                }
            }
        }
        let start = self.offsets[l];
        let end = self.offsets[l + 1];
        let token_in_vocab = &self.data[start..end];
        if token == token_in_vocab {
            Some(l as TokenId)
        } else {
            None
        }
    }

    pub fn iter_vocab(&self) -> impl Iterator<Item = (&str, TokenId)> + '_ {
        self.offsets.windows(2).map(|w| {
            let start = w[0];
            let end = w[1];
            let token = &self.data[start..end];
            (token, start as TokenId)
        })
    }
}

impl ImmutableInvertedIndex {
    pub fn new() -> ImmutableInvertedIndex {
        Default::default()
    }

    pub fn remove_document(&mut self, idx: PointOffsetType) -> Option<Document> {
        if self.point_to_docs.len() <= idx as usize {
            return None; // Already removed or never actually existed
        }

        let removed_doc = match std::mem::take(&mut self.point_to_docs[idx as usize]) {
            Some(doc) => doc,
            None => return None,
        };

        self.points_count -= 1;

        for removed_token in removed_doc.tokens() {
            // unwrap safety: posting list exists and contains the document id
            let posting = self.postings.get_mut(*removed_token as usize).unwrap();
            if let Some(vec) = posting {
                vec.remove(idx);
            }
        }
        Some(removed_doc)
    }

    pub fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
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
            return Box::new(vec![].into_iter());
        }
        let postings = postings_opt.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return Box::new(vec![].into_iter());
        }
        intersect_postings_iterator(postings)
    }

    pub fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> CardinalityEstimation {
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
            return CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0,
                exp: 0,
                max: 0,
            };
        }
        let postings = postings_opt.unwrap();
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
        let smallest_posting = postings.iter().map(|posting| posting.len()).min().unwrap();

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
                .map(|posting| posting.len() as f64 / self.points_count as f64)
                .product();
            let exp = (expected_frac * self.points_count as f64) as usize;
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
        // It might be very hard to predict possible combinations of conditions,
        // so we only build it for individual tokens
        Box::new(
            self.vocab
                .iter_vocab()
                .filter(|(_token, posting_idx)| self.postings[*posting_idx as usize].is_some())
                .filter(move |(_token, posting_idx)| {
                    // unwrap crash safety: all tokens that passes the first filter should have postings
                    self.postings[*posting_idx as usize].as_ref().unwrap().len() >= threshold
                })
                .map(|(token, posting_idx)| {
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
                            text: token.to_owned(),
                        })),
                        range: None,
                        geo_bounding_box: None,
                        geo_radius: None,
                        geo_polygon: None,
                        values_count: None,
                    },
                    cardinality: posting.len(),
                }),
        )
    }
}

impl From<MutableInvertedIndex> for ImmutableInvertedIndex {
    fn from(mut index: MutableInvertedIndex) -> Self {
        let (vocab, token_id_reindex) = FlattenedVocab::new(index.vocab);
        index.point_to_docs.iter_mut().for_each(|doc| {
            if let Some(doc) = doc {
                doc.tokens
                    .iter_mut()
                    .for_each(|token| *token = token_id_reindex[*token as usize]);
            }
        });

        let mut postings = vec![None; index.postings.len()];
        for (token_id, posting) in index.postings.into_iter().enumerate() {
            let new_token_id = token_id_reindex[token_id];
            postings[new_token_id as usize] = posting;
        }

        ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_docs: index.point_to_docs,
            points_count: index.points_count,
        }
    }
}
