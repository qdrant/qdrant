use std::sync::atomic::AtomicBool;

use common::types::{PointOffsetType, ScoredPointOffset};
use common::universal_io::{UniversalRead, UserData};

use super::super::full_text_index_read::{FullTextIndexRead, default_check_match_batch};
use super::super::inverted_index::{InvertedIndex, ParsedQuery, TokenId};
use super::super::tokenizers::Tokenizer;
use super::ImmutableFullTextIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::inverted_index::bm25::Bm25Query;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::{ImmutableLayout, StorageType};
use crate::types::{FieldCondition, PayloadKeyType};

impl<S: UniversalRead> FullTextIndexRead for ImmutableFullTextIndex<S> {
    fn tokenizer(&self) -> &Tokenizer {
        &self.storage.tokenizer
    }

    fn telemetry_index_type(&self) -> &'static str {
        "immutable_full_text"
    }

    fn points_count(&self) -> usize {
        self.inverted_index.points_count()
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.inverted_index.values_count(point_id)
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.inverted_index.values_is_empty(point_id)
    }

    fn doc_len_batch(
        &self,
        point_ids: &[PointOffsetType],
        f: impl FnMut(usize, Option<u32>),
    ) -> OperationResult<()> {
        self.inverted_index.doc_len_batch(point_ids, f)
    }

    fn posting_len(&self, token_id: TokenId) -> OperationResult<Option<usize>> {
        self.inverted_index.get_posting_len(token_id)
    }

    fn score_bm25(
        &self,
        query: &Bm25Query,
        accept: &dyn Fn(PointOffsetType) -> bool,
        limit: usize,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        self.inverted_index
            .score_bm25(query, accept, limit, is_stopped)
    }

    fn total_tokens(&self) -> Option<u64> {
        self.inverted_index.total_tokens()
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        iter: impl Iterator<Item = (U, &'a str)>,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        self.inverted_index.for_each_token_id(iter, f)
    }

    fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        self.inverted_index.filter(query)
    }

    fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        self.inverted_index.estimate_cardinality(query, condition)
    }

    fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> OperationResult<bool> {
        self.inverted_index.check_match(query, point_id)
    }

    fn check_match_batch<U: UserData>(
        &self,
        query: &ParsedQuery,
        items: impl Iterator<Item = (U, PointOffsetType)>,
        on_match: impl FnMut(U, bool),
    ) -> OperationResult<()> {
        default_check_match_batch(self, query, items, on_match)
    }

    fn for_each_payload_block_inner(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inverted_index
            .for_each_payload_block(threshold, key, f)
    }

    fn get_storage_type(&self) -> StorageType {
        StorageType::Mmap {
            layout: ImmutableLayout::Heap,
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }

    fn is_cold(&self) -> bool {
        false
    }
}
