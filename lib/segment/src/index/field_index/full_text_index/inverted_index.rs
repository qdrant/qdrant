use patricia_tree::{PatriciaMap, PatriciaSet};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};

use crate::index::field_index::full_text_index::postings_iterator::intersect_btree_iterator;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, MatchText, PayloadKeyType, PointOffsetType};

type PostingList = Vec<PointOffsetType>;

#[derive(Default, Clone, Debug)]
pub struct Document {
    // TODO: use a radix trie to store tokens
    pub tokens: PatriciaSet,
}

impl Document {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

impl Serialize for Document {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_seq(Some(self.tokens.len()))?;
        for token in self.tokens.iter() {
            state.serialize_element(&token)?;
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for Document {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let tokens = Vec::<Vec<u8>>::deserialize(deserializer)?;
        let mut doc = Document::new();
        for token in tokens {
            doc.tokens.insert(token);
        }
        Ok(doc)
    }
}

pub struct ParsedQuery {
    pub tokens: PatriciaSet,
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
    // TODO: maybe use a adaptive radix tree
    // store strings and map them to a posting list
    postings: PatriciaMap<PostingList>,
    pub point_to_docs: Vec<Option<Document>>,
    pub points_count: usize,
}

impl InvertedIndex {
    pub fn new() -> InvertedIndex {
        InvertedIndex {
            postings: PatriciaMap::new(),
            point_to_docs: Vec::new(),
            points_count: 0,
        }
    }

    /// Insert a document into the inverted index
    pub fn index_document(&mut self, idx: PointOffsetType, document: Document) {
        for token in document.tokens.iter() {
            // check if the token is already in the inverted index
            // if not, add it
            let posting = self.postings.get_mut(&token);
            if let Some(posting) = posting {
                // check if the posting list already contains the current document
                // if not, add it and keep the posting list sorted
                match posting.binary_search(&idx) {
                    Ok(_) => {} // element already in vector @ `pos`
                    Err(pos) => posting.insert(pos, idx),
                }
            } else {
                let new_posting = vec![idx];
                self.postings.insert(token, new_posting);
            }
        }
        self.points_count += 1;
        if self.point_to_docs.len() <= idx as usize {
            self.point_to_docs.resize(idx as usize + 1, None);
        }

        self.point_to_docs[idx as usize] = Some(document);
    }

    /// Remove a document from the inverted index
    pub fn remove_document(&mut self, idx: PointOffsetType) -> Option<Document> {
        if self.point_to_docs.len() <= idx as usize {
            return None; // Already removed or never actually existed
        }

        let removed_doc = match std::mem::take(&mut self.point_to_docs[idx as usize]) {
            Some(doc) => doc,
            None => return None,
        };

        self.points_count -= 1;

        for removed_token in removed_doc.tokens.iter() {
            let posting = self.postings.remove(&removed_token);
            if let Some(mut posting) = posting {
                if let Ok(pos) = posting.binary_search(&idx) {
                    posting.remove(pos); // element already in vector @ `pos`
                } else {
                    continue;
                }

                if posting.is_empty() {
                    self.postings.remove(removed_token);
                }
            } else {
                continue;
            }
        }
        Some(removed_doc)
    }

    /// Query the inverted index
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

    /// Estimate the cardinality of a query
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
                .filter(move |(_token, _posting)| _posting.len() >= threshold)
                .map(move |(token, posting)| PayloadBlockCondition {
                    condition: FieldCondition {
                        key: key.clone(),
                        r#match: Some(Match::Text(MatchText {
                            text: format!("{:?}", &token),
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
