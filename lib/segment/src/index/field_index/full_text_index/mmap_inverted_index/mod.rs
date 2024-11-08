#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;

use bitvec::vec::BitVec;
use common::mmap_hashmap::MmapHashMap;
use common::types::PointOffsetType;
use memory::madvise::AdviceSetting;
use memory::mmap_ops;
use memory::mmap_type::{MmapBitSlice, MmapSlice};
use mmap_postings::MmapPostings;

use super::inverted_index::{InvertedIndex, ParsedQuery};
use super::postings_iterator::intersect_compressed_postings_iterator;
use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::immutable_inverted_index::ImmutableInvertedIndex;
use crate::index::field_index::full_text_index::inverted_index::TokenId;

mod mmap_postings;

const POSTINGS_FILE: &str = "postings.dat";
const VOCAB_FILE: &str = "vocab.dat";
const POINT_TO_TOKENS_COUNT_FILE: &str = "point_to_tokens_count.dat";
const DELETED_POINTS_FILE: &str = "deleted_points.dat";

pub struct MmapInvertedIndex {
    pub(in crate::index::field_index::full_text_index) path: PathBuf,
    pub(in crate::index::field_index::full_text_index) postings: MmapPostings,
    pub(in crate::index::field_index::full_text_index) vocab: MmapHashMap<str, TokenId>,
    pub(in crate::index::field_index::full_text_index) point_to_tokens_count: MmapSlice<usize>,
    pub(in crate::index::field_index::full_text_index) deleted_points:
        MmapBitSliceBufferedUpdateWrapper,
    /// Number of points which are not deleted
    pub(in crate::index::field_index::full_text_index) active_points_count: usize,
}

impl MmapInvertedIndex {
    pub fn create(path: PathBuf, inverted_index: ImmutableInvertedIndex) -> OperationResult<()> {
        let ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count,
            points_count: _,
        } = inverted_index;

        debug_assert_eq!(vocab.len(), postings.len());

        let postings_path = path.join(POSTINGS_FILE);
        let vocab_path = path.join(VOCAB_FILE);
        let point_to_tokens_count_path = path.join(POINT_TO_TOKENS_COUNT_FILE);
        let deleted_points_path = path.join(DELETED_POINTS_FILE);

        MmapPostings::create(postings_path, &postings)?;

        // Currently MmapHashMap maps str -> [u32], but we only need to map str -> u32.
        // TODO: Consider making another mmap structure for this case.
        MmapHashMap::<str, TokenId>::create(
            &vocab_path,
            vocab.iter().map(|(k, v)| (k.as_str(), std::iter::once(*v))),
        )?;

        // Save point_to_tokens_count, separated into a bitslice for None values and a slice for actual values
        //
        // None values are represented as deleted in the bitslice
        let deleted_bitslice: BitVec = point_to_tokens_count
            .iter()
            .map(|count| count.is_none())
            .collect();
        MmapBitSlice::create(&deleted_points_path, &deleted_bitslice)?;

        // The actual values go in the slice
        let point_to_tokens_count_iter = point_to_tokens_count
            .into_iter()
            .map(|count| count.unwrap_or(0));

        MmapSlice::create(&point_to_tokens_count_path, point_to_tokens_count_iter)?;

        Ok(())
    }

    pub fn open(path: PathBuf, populate: bool) -> OperationResult<Self> {
        let postings_path = path.join(POSTINGS_FILE);
        let vocab_path = path.join(VOCAB_FILE);
        let point_to_tokens_count_path = path.join(POINT_TO_TOKENS_COUNT_FILE);
        let deleted_points_path = path.join(DELETED_POINTS_FILE);

        let postings = MmapPostings::open(&postings_path, populate)?;
        let vocab = MmapHashMap::<str, TokenId>::open(&vocab_path)?;

        let point_to_tokens_count = unsafe {
            MmapSlice::try_from(mmap_ops::open_write_mmap(
                &point_to_tokens_count_path,
                AdviceSetting::Global,
                populate,
            )?)?
        };

        let deleted =
            mmap_ops::open_write_mmap(&deleted_points_path, AdviceSetting::Global, populate)?;
        let deleted = MmapBitSlice::from(deleted, 0);

        let num_deleted_points = deleted.count_ones();
        let deleted_points = MmapBitSliceBufferedUpdateWrapper::new(deleted);
        let points_count = point_to_tokens_count.len() - num_deleted_points;

        Ok(Self {
            path,
            postings,
            vocab,
            point_to_tokens_count,
            deleted_points,
            active_points_count: points_count,
        })
    }

    fn iter_vocab(&self) -> impl Iterator<Item = (&str, &TokenId)> {
        // unwrap safety: we know that each token points to a token id.
        self.vocab.iter().map(|(k, v)| (k, v.first().unwrap()))
    }

    /// Returns whether the point id is valid and active.
    fn is_active(&self, point_id: PointOffsetType) -> bool {
        let is_deleted = self.deleted_points.get(point_id as usize).unwrap_or(true);

        !is_deleted
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            self.path.join(POSTINGS_FILE),
            self.path.join(VOCAB_FILE),
            self.path.join(POINT_TO_TOKENS_COUNT_FILE),
            self.path.join(DELETED_POINTS_FILE),
        ]
    }
}

impl InvertedIndex for MmapInvertedIndex {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId> {
        unreachable!("MmapInvertedIndex does not support mutable operations")
    }

    fn index_document(
        &mut self,
        _idx: PointOffsetType,
        _document: super::inverted_index::Document,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Can't add values to mmap immutable text index",
        ))
    }

    fn remove_document(&mut self, idx: PointOffsetType) -> bool {
        let Some(is_deleted) = self.deleted_points.get(idx as usize) else {
            return false; // Never existed
        };

        if is_deleted {
            return false; // Already removed
        }

        self.deleted_points.set(idx as usize, true);
        self.point_to_tokens_count[idx as usize] = 0;
        self.active_points_count -= 1;
        true
    }

    fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&token_id| match token_id {
                None => None,
                // if a ParsedQuery token was given an index, then it must exist in the vocabulary
                Some(idx) => self.postings.get(idx),
            })
            .collect();
        let Some(posting_readers) = postings_opt else {
            // There are unseen tokens -> no matches
            return Box::new(vec![].into_iter());
        };

        if posting_readers.is_empty() {
            // Empty request -> no matches
            return Box::new(vec![].into_iter());
        }

        // in case of mmap immutable index, deleted points are still in the postings
        let filter = move |idx| self.is_active(idx);

        intersect_compressed_postings_iterator(posting_readers, filter)
    }

    fn get_posting_len(&self, token_id: TokenId) -> Option<usize> {
        self.postings.get(token_id).map(|p| p.len())
    }

    fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.iter_vocab().filter_map(|(token, &token_id)| {
            self.postings
                .get(token_id)
                .map(|posting| (token, posting.len()))
        })
    }

    fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        if parsed_query.tokens.contains(&None) {
            return false;
        }
        // check presence of the document
        if self.values_is_empty(point_id) {
            return false;
        }
        // Check that all tokens are in document
        parsed_query
            .tokens
            .iter()
            // unwrap safety: all tokens exist in the vocabulary if it passes the above check
            .all(|query_token| {
                self.postings
                    .get(query_token.unwrap())
                    .unwrap()
                    .contains(point_id)
            })
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        if self.deleted_points.get(point_id as usize).unwrap_or(true) {
            return true;
        }
        self.point_to_tokens_count
            .get(point_id as usize)
            .map(|count| *count == 0)
            // if the point does not exist, it is considered empty
            .unwrap_or(true)
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        if self.deleted_points.get(point_id as usize).unwrap_or(true) {
            return 0;
        }
        self.point_to_tokens_count
            .get(point_id as usize)
            .copied()
            // if the point does not exist, it is considered empty
            .unwrap_or(0)
    }

    fn points_count(&self) -> usize {
        self.active_points_count
    }

    fn get_token_id(&self, token: &str) -> Option<TokenId> {
        self.vocab
            .get(token)
            .ok()
            .flatten()
            .and_then(<[TokenId]>::first)
            .copied()
    }
}
