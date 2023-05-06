use std::collections::{BTreeSet, HashMap};
use std::ops::Index;

use bitvec::view::AsBits;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::inverted_index::{Document, TokenId, ParsedQuery};
use super::posting_list::PostingList;
use super::postings_iterator::intersect_postings_iterator;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, MatchText, PayloadKeyType, PointOffsetType};


pub fn db_encode_tokens(data: &[u32]) -> Vec<u8> {
    let mut res = Vec::with_capacity(data.len() * 4);
    for i in 0..data.len() {
        res[4*i..][..4].copy_from_slice(&data[i].to_be_bytes());
    }
    res
}

pub fn db_decode_tokens(data: &[u8]) -> Vec<u32> {
    if data.len() == 0 {
        return vec![];
    }
    let token_count = data.len() / 4 + 1;
    let mut res = Vec::<u32>::with_capacity(token_count);
    for (chunk, i) in data.windows(4).step_by(4).zip(0..token_count) {
        res[i] = u32::from_be_bytes(chunk.try_into().unwrap());
    }
    res
}

pub struct InvertedIndex {
    postings: DatabaseColumnWrapper,
    pub vocab: DatabaseColumnWrapper,
    pub point_to_docs: DatabaseColumnWrapper,
    pub points_count: usize,
}

impl InvertedIndex {
    fn store_key(id: &PointOffsetType) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    fn restore_key(data: &[u8]) -> PointOffsetType {
        bincode::deserialize(data).unwrap()
    }

    fn serialize_document_tokens(&self, tokens: BTreeSet<String>) -> OperationResult<Vec<u8>> {
        #[derive(Serialize)]
        struct StoredDocument {
            tokens: BTreeSet<String>,
        }
        let doc = StoredDocument { tokens };
        serde_cbor::to_vec(&doc).map_err(|e| {
            OperationError::service_error(format!("Failed to serialize document: {e}"))
        })
    }

    fn deserialize_document(&self, data: &[u8]) -> OperationResult<Document> {
        #[derive(Deserialize)]
        struct StoredDocument {
            tokens: BTreeSet<String>,
        }
        match serde_cbor::from_slice::<StoredDocument>(data)
        {
            Ok(doc) => self.document_from_tokens(&doc.tokens),
            Err(e) => Err(OperationError::service_error(format!("Failed to deserialize document: {e}")))
        } 
    }

    pub fn document_from_tokens(&self, tokens: &BTreeSet<String>) -> Result<Document, OperationError> {
        let mut document_tokens = vec![];
        for token in tokens {
            // check if in vocab
            let vocab_idx = match self.vocab.get_pinned(token.as_bytes(), |raw| serde_cbor::from_slice(raw))? {
                    Some(cbor_result) => cbor_result?,
                    None => {
                        let next_token_id = self.vocab.lock_db().iter()?.count() as TokenId;
                        self.vocab.put(token.as_bytes(), next_token_id.to_be_bytes());
                        next_token_id
                    }
            };
            document_tokens.push(vocab_idx);
        }

        Ok(Document::new(document_tokens))
    }

    pub fn index_document(&mut self, idx: PointOffsetType, document: Document) -> Result<(), OperationError> {
        self.points_count += 1;

        for token_idx in document.tokens() {
            let mut posting = self
                .postings
                .get_pinned(&Self::store_key(&token_idx), |raw| db_decode_tokens(raw))
                .expect("posting must exist even if with None");
            let new_posting = match posting {
                None => vec![idx],
                Some(vec) => {
                    vec.push(idx);
                    vec
                },
            };
            self.postings.put(Self::store_key(&token_idx), db_encode_tokens(&new_posting));
        }
        let db_document = db_encode_tokens(document.tokens());
        self.point_to_docs.put(Self::store_key(&idx), db_document);
        Ok(())
    }

    pub fn remove_document(&mut self, idx: PointOffsetType) -> OperationResult<Option<()>> {
        if self.point_to_docs.lock_db().iter()?.count() <= idx as usize {
            return Ok(None); // Already removed or never actually existed
        }
        let db_idx = Self::store_key(&idx);
        let tokens = self.point_to_docs.get_pinned(&db_idx, |raw| db_decode_tokens(raw))?.ok_or(OperationError::service_error(format!("Document to be deleted is empty {idx}")))?;
        self.point_to_docs.put(&db_idx, vec![])?;

        self.points_count -= 1;

        for removed_token in tokens {
            // unwrap safety: posting list exists and contains the document id
            let db_key = Self::store_key(&removed_token);
            let posting = self.postings.get_pinned(&db_key, |raw| db_decode_tokens(raw))?;
            if let Some(vec) = posting {
                if let Ok(removal_idx) = vec.binary_search(&idx) {
                    vec.remove(removal_idx);
                    self.postings.put(&db_key, db_encode_tokens(&vec))?;
                }
            }
        }
        Ok(Some(()))
    }

    pub fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| match vocab_idx {
                None => None,
                // if a ParsedQuery token was given an index, then it must exist in the vocabulary
                // dictionary. Posting list entry can be None but it exists.
                Some(idx) => Some(PostingList::from(self.postings.get_pinned(&Self::store_key(&idx), |raw| db_decode_tokens(raw)))),
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
        let postings = postings.iter().collect_vec();
        intersect_postings_iterator(postings)
    }

    // pub fn estimate_cardinality(
    //     &self,
    //     query: &ParsedQuery,
    //     condition: &FieldCondition,
    // ) -> CardinalityEstimation {
    //     let postings_opt: Option<Vec<_>> = query
    //         .tokens
    //         .iter()
    //         .map(|&vocab_idx| match vocab_idx {
    //             None => None,
    //             // unwrap safety: same as in filter()
    //             Some(idx) => self.postings.get(idx as usize).unwrap().as_ref(),
    //         })
    //         .collect();
    //     if postings_opt.is_none() {
    //         // There are unseen tokens -> no matches
    //         return CardinalityEstimation {
    //             primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
    //             min: 0,
    //             exp: 0,
    //             max: 0,
    //         };
    //     }
    //     let postings = postings_opt.unwrap();
    //     if postings.is_empty() {
    //         // Empty request -> no matches
    //         return CardinalityEstimation {
    //             primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
    //             min: 0,
    //             exp: 0,
    //             max: 0,
    //         };
    //     }
    //     // Smallest posting is the largest possible cardinality
    //     let smallest_posting = postings.iter().map(|posting| posting.len()).min().unwrap();

    //     return if postings.len() == 1 {
    //         CardinalityEstimation {
    //             primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
    //             min: smallest_posting,
    //             exp: smallest_posting,
    //             max: smallest_posting,
    //         }
    //     } else {
    //         let expected_frac: f64 = postings
    //             .iter()
    //             .map(|posting| posting.len() as f64 / self.points_count as f64)
    //             .product();
    //         let exp = (expected_frac * self.points_count as f64) as usize;
    //         CardinalityEstimation {
    //             primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
    //             min: 0, // ToDo: make better estimation
    //             exp,
    //             max: smallest_posting,
    //         }
    //     };
    // }

    // pub fn payload_blocks(
    //     &self,
    //     threshold: usize,
    //     key: PayloadKeyType,
    // ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
    //     // It might be very hard to predict possible combinations of conditions,
    //     // so we only build it for individual tokens
    //     Box::new(
    //         self.vocab
    //             .lock_db().iter().unwrap()
    //             .filter(|(_token, &posting_idx)| self.postings[posting_idx as usize].is_some())
    //             .filter(move |(_token, &posting_idx)| {
    //                 // unwrap crash safety: all tokens that passes the first filter should have postings
    //                 self.postings[posting_idx as usize].as_ref().unwrap().len() >= threshold
    //             })
    //             .map(|(token, &posting_idx)| {
    //                 (
    //                     token,
    //                     // same as the above case
    //                     self.postings[posting_idx as usize].as_ref().unwrap(),
    //                 )
    //             })
    //             .map(move |(token, posting)| PayloadBlockCondition {
    //                 condition: FieldCondition {
    //                     key: key.clone(),
    //                     r#match: Some(Match::Text(MatchText {
    //                         text: token.clone(),
    //                     })),
    //                     range: None,
    //                     geo_bounding_box: None,
    //                     geo_radius: None,
    //                     values_count: None,
    //                 },
    //                 cardinality: posting.len(),
    //             }),
    //     )
    // }
}
