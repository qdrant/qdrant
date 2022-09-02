use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::index::field_index::full_text_index::postings_iterator::intersect_btree_iterator;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, MatchText, PayloadKeyType, PointOffsetType};

type PostingList = BTreeSet<PointOffsetType>;

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct Document {
    pub tokens: BTreeSet<String>,
}

impl Document {
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

pub struct ParsedQuery {
    pub tokens: BTreeSet<String>,
}

impl ParsedQuery {
    pub fn check_match(&self, document: &Document) -> bool {
        // Check that all tokens are in document
        self.tokens
            .iter()
            .all(|query_token| document.tokens.contains(query_token))
    }
}

pub struct InvertedIndex {
    postings: BTreeMap<String, PostingList>,
    pub point_to_docs: Vec<Option<Document>>,
    pub points_count: usize,
}

impl InvertedIndex {
    pub fn new() -> InvertedIndex {
        InvertedIndex {
            postings: BTreeMap::new(),
            point_to_docs: Vec::new(),
            points_count: 0,
        }
    }

    pub fn index_document(&mut self, idx: PointOffsetType, document: Document) {
        for token in &document.tokens {
            let posting = self
                .postings
                .entry(token.to_owned())
                .or_insert_with(BTreeSet::new);
            posting.insert(idx);
        }
        self.points_count += 1;
        if self.point_to_docs.len() <= idx as usize {
            self.point_to_docs
                .resize(idx as usize + 1, Default::default());
        }

        self.point_to_docs[idx as usize] = Some(document);
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

        for removed_token in &removed_doc.tokens {
            let posting = self.postings.get_mut(removed_token);
            if let Some(posting) = posting {
                posting.remove(&idx);
                if posting.is_empty() {
                    self.postings.remove(removed_token);
                }
            }
        }
        Some(removed_doc)
    }

    pub fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|token| self.postings.get(token))
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
        intersect_btree_iterator(postings)
    }

    pub fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> CardinalityEstimation {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|token| self.postings.get(token))
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
            self.postings
                .iter()
                .filter(move |(_token, posting)| posting.len() >= threshold)
                .map(move |(token, posting)| PayloadBlockCondition {
                    condition: FieldCondition {
                        key: key.clone(),
                        r#match: Some(Match::Text(MatchText {
                            text: token.to_owned(),
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
