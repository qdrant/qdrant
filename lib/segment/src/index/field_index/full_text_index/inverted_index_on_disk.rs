use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};

use super::inverted_index::{Document, ParsedQuery, TokenId};
use super::posting_list::PostingList;
use super::postings_iterator::intersect_postings_iterator_owned;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::PointOffsetType;

pub fn db_encode_tokens(data: &[u32]) -> Vec<u8> {
    if data.is_empty() {
        return vec![];
    }
    let mut res = vec![0; data.len() * 4];
    for i in 0..data.len() {
        res[4 * i..][..4].copy_from_slice(&data[i].to_be_bytes());
    }
    res
}

pub fn db_decode_tokens(data: &[u8]) -> Vec<u32> {
    if data.is_empty() {
        return vec![];
    }
    let token_count = data.len() / 4 + if data.len() % 4 == 0 { 0 } else { 1 };
    let mut res = vec![0; token_count];
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
    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let db_postings = DatabaseColumnWrapper::new(db.clone(), &format!("{field}_postings_iidx"));
        let db_vocab = DatabaseColumnWrapper::new(db.clone(), &format!("{field}_vocab_iidx"));
        let db_point_to_docs =
            DatabaseColumnWrapper::new(db, &format!("{field}_point_to_docs_iidx"));
        Self {
            postings: db_postings,
            vocab: db_vocab,
            point_to_docs: db_point_to_docs,
            points_count: Default::default(),
        }
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.postings.recreate_column_family()?;
        self.vocab.recreate_column_family()?;
        self.point_to_docs.recreate_column_family()
    }

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
        match serde_cbor::from_slice::<StoredDocument>(data) {
            Ok(doc) => self.document_from_tokens(&doc.tokens),
            Err(e) => Err(OperationError::service_error(format!(
                "Failed to deserialize document: {e}"
            ))),
        }
    }

    pub fn document_from_tokens(
        &self,
        tokens: &BTreeSet<String>,
    ) -> Result<Document, OperationError> {
        let mut document_tokens = vec![];
        for token in tokens {
            // check if in vocab
            let vocab_idx = match self.vocab.get_pinned(token.as_bytes(), db_decode_tokens)? {
                Some(cbor_result) => cbor_result
                    .first()
                    .ok_or(OperationError::service_error("No tokens to decode"))?
                    .clone(),
                None => {
                    let next_token_id = self.vocab.lock_db().iter()?.count() as TokenId;
                    self.vocab
                        .put(token.as_bytes(), db_encode_tokens(&[next_token_id]))?;
                    next_token_id
                }
            };
            document_tokens.push(vocab_idx);
        }

        Ok(Document::new(document_tokens))
    }

    pub fn index_document(
        &mut self,
        idx: PointOffsetType,
        document: Document,
    ) -> Result<(), OperationError> {
        self.points_count += 1;

        for token_idx in document.tokens() {
            let posting = self
                .postings
                .get_pinned(&Self::store_key(token_idx), db_decode_tokens)
                .expect("posting must exist even if with None");
            let new_posting = match posting {
                None => vec![idx],
                Some(mut vec) => {
                    vec.push(idx);
                    vec
                }
            };
            self.postings
                .put(Self::store_key(token_idx), db_encode_tokens(&new_posting))?;
        }
        let db_document = db_encode_tokens(document.tokens());
        self.point_to_docs.put(Self::store_key(&idx), db_document)?;
        Ok(())
    }

    pub fn remove_document(&mut self, idx: PointOffsetType) -> OperationResult<Option<()>> {
        if self.point_to_docs.lock_db().iter()?.count() <= idx as usize {
            return Ok(None); // Already removed or never actually existed
        }
        let db_idx = Self::store_key(&idx);
        let tokens = self
            .point_to_docs
            .get_pinned(&db_idx, db_decode_tokens)?
            .ok_or(OperationError::service_error(format!(
                "Document to be deleted is empty {idx}"
            )))?;
        self.point_to_docs.put(&db_idx, vec![])?;

        self.points_count -= 1;

        for removed_token in tokens {
            // unwrap safety: posting list exists and contains the document id
            let db_key = Self::store_key(&removed_token);
            let posting = self.postings.get_pinned(&db_key, db_decode_tokens)?;
            if let Some(mut vec) = posting {
                if let Ok(removal_idx) = vec.binary_search(&idx) {
                    vec.remove(removal_idx);
                    self.postings.put(&db_key, db_encode_tokens(&vec))?;
                }
            }
        }
        Ok(Some(()))
    }

    pub fn filter(
        &self,
        query: &ParsedQuery,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        let mut postings = vec![];
        for &vocab_idx in query.tokens.iter() {
            if let Some(idx) = vocab_idx {
                let res = self
                    .postings
                    .get_pinned(&Self::store_key(&idx), db_decode_tokens)?;
                if let Some(tokens) = res {
                    postings.push(PostingList::from(tokens));
                } else {
                    return Ok(Box::new(vec![].into_iter()));
                }
            } else {
                // There are unseen tokens -> no matches
                return Ok(Box::new(vec![].into_iter()));
            }
        }
        if postings.is_empty() {
            // Empty request -> no matches
            return Ok(Box::new(vec![].into_iter()));
        }
        Ok(intersect_postings_iterator_owned(postings))
    }

    fn flush(&self) -> OperationResult<()> {
        self.postings.flusher()()?;
        self.vocab.flusher()()?;
        self.point_to_docs.flusher()()
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use tempfile::Builder;

    use super::db_decode_tokens;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::data_types::text_index::{TextIndexParams, TextIndexType, TokenizerType};
    use crate::index::field_index::full_text_index::inverted_index::ParsedQuery;
    use crate::index::field_index::full_text_index::inverted_index_on_disk::{
        db_encode_tokens, InvertedIndex,
    };
    use crate::index::field_index::full_text_index::tokenizers::Tokenizer;
    fn parse_query(index: &InvertedIndex, config: &TextIndexParams, text: &str) -> ParsedQuery {
        let mut tokens = HashSet::new();
        Tokenizer::tokenize_query(text, &config, |token| {
            tokens.insert(
                index
                    .vocab
                    .get_pinned(token.as_bytes(), |raw| {
                        db_decode_tokens(raw).first().unwrap().clone()
                    })
                    .unwrap(),
            );
        });
        ParsedQuery {
            tokens: tokens.into_iter().collect(),
        }
    }

    #[test]
    fn test_codec_roundtrip() {
        let tokens = vec![102, 105, 114, 115, 116];
        assert_eq!(db_decode_tokens(&db_encode_tokens(&tokens)), tokens);

        let tokens = vec![0, 0, 0, 31];
        assert_eq!(db_encode_tokens(&db_decode_tokens(&tokens)), tokens);
    }

    #[test]
    fn test_inverted_index() {
        let payloads = vec![
            "The celebration had a long way to go and even in the silent depths of Multivac's underground chambers, it hung in the air.".to_string(),
            "If nothing else, there was the mere fact of isolation and silence.".to_string(),
            "For the first time in a decade, technicians were not scurrying about the vitals of the giant computer, the soft lights did not wink out their erratic patterns, the flow of information in and out had halted.".to_string(),
            "It would not be halted long, of course, for the needs of peace would be pressing.".to_string(),
            "Yet now, for a day, perhaps for a week, even Multivac might celebrate the great time, and rest.".to_string(),
        ];

        let tmp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let config = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: None,
        };

        {
            let db = open_db_with_existing_cf(&tmp_dir.path().join("test_db")).unwrap();

            let mut index = InvertedIndex::new(db, "text");

            index.recreate().unwrap();

            for (idx, payload) in payloads.iter().enumerate() {
                let mut tokens: BTreeSet<String> = BTreeSet::new();

                Tokenizer::tokenize_doc(&payload, &config, |token| {
                    tokens.insert(token.to_owned());
                });
                index
                    .index_document(idx as u32, index.document_from_tokens(&tokens).unwrap())
                    .unwrap();
            }

            assert_eq!(
                index.point_to_docs.lock_db().iter().unwrap().count(),
                payloads.len()
            );

            assert_eq!(index.points_count, payloads.len());

            let query = parse_query(&index, &config, "multivac");
            let search_res: Vec<_> = index.filter(&query).unwrap().collect();
            assert_eq!(search_res, vec![0, 4]);

            let query = parse_query(&index, &config, "giant computer");
            let search_res: Vec<_> = index.filter(&query).unwrap().collect();
            assert_eq!(search_res, vec![2]);

            let query = parse_query(&index, &config, "the great time");
            let search_res: Vec<_> = index.filter(&query).unwrap().collect();
            assert_eq!(search_res, vec![4]);

            index.remove_document(2).unwrap();
            index.remove_document(3).unwrap();

            let filter_condition = parse_query(&index, &config, "giant computer");
            assert!(index.filter(&filter_condition).unwrap().next().is_none());

            assert_eq!(index.points_count, payloads.len() - 2);

            // let payload = serde_json::json!([
            //     "The last question was asked for the first time, half in jest, on May 21, 2061,",
            //     "at a time when humanity first stepped into the light."
            // ]);
            // index.add_point(3, &MultiValue::one(&payload)).unwrap();

            // let payload = serde_json::json!([
            //     "The question came about as a result of a five dollar bet over highballs, and it happened this way: "
            // ]);
            // index.add_point(4, &MultiValue::one(&payload)).unwrap();

            // assert_eq!(index.count_indexed_points(), payloads.len() - 1);

            index.flush().unwrap();
        }
    }
}
