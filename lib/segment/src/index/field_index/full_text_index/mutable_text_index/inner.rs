use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UserData;

use super::super::full_text_index_read::{FullTextIndexRead, default_check_match_batch};
use super::super::inverted_index::mutable_inverted_index::MutableInvertedIndex;
use super::super::inverted_index::{InvertedIndex, ParsedQuery, TokenId};
use super::super::tokenizers::Tokenizer;
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::StorageType;
use crate::types::{FieldCondition, PayloadKeyType};

/// In-memory state shared by [`MutableFullTextIndex`] and
/// [`ReadOnlyAppendableFullTextIndex`].
///
/// Both wrappers add a different backing storage (`Blobstore` vs
/// `BlobstoreReader`); the in-memory layout that serves every
/// [`FullTextIndexRead`] method is the same, so it lives here once.
///
/// [`MutableFullTextIndex`]: super::MutableFullTextIndex
/// [`ReadOnlyAppendableFullTextIndex`]: super::read_only::ReadOnlyAppendableFullTextIndex
pub(in crate::index::field_index::full_text_index) struct MutableFullTextIndexInner {
    pub(in crate::index::field_index::full_text_index) inverted_index: MutableInvertedIndex,
    pub(in crate::index::field_index::full_text_index) config: TextIndexParams,
    pub(in crate::index::field_index::full_text_index) tokenizer: Tokenizer,
}

impl FullTextIndexRead for MutableFullTextIndexInner {
    fn tokenizer(&self) -> &Tokenizer {
        &self.tokenizer
    }

    /// Placeholder telemetry tag — the inner is never queried directly; both
    /// wrappers override this on their own [`FullTextIndexRead`] impls.
    fn telemetry_index_type(&self) -> &'static str {
        "mutable_full_text"
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

    fn for_each_token_id<'a, U: UserData>(
        &self,
        iter: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        self.inverted_index.for_each_token_id(iter, hw_counter, f)
    }

    fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        self.inverted_index.filter(query, hw_counter)
    }

    fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        self.inverted_index
            .estimate_cardinality(query, condition, hw_counter)
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

    /// Placeholder — both wrappers override `get_storage_type()` on their own
    /// [`FullTextIndexRead`] impls to report the concrete storage.
    fn get_storage_type(&self) -> StorageType {
        StorageType::Gridstore
    }

    fn ram_usage_bytes(&self) -> usize {
        self.inverted_index.ram_usage_bytes()
    }

    fn is_on_disk(&self) -> bool {
        false
    }
}
