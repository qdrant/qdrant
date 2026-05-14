use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UserData;

use super::super::full_text_index_read::FullTextIndexRead;
use super::super::inverted_index::{ParsedQuery, TokenId};
use super::super::tokenizers::Tokenizer;
use super::MutableFullTextIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::StorageType;
use crate::types::{FieldCondition, PayloadKeyType};

impl FullTextIndexRead for MutableFullTextIndex {
    fn tokenizer(&self) -> &Tokenizer {
        self.inner.tokenizer()
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mutable_full_text"
    }

    fn points_count(&self) -> usize {
        self.inner.points_count()
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.inner.values_count(point_id)
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.inner.values_is_empty(point_id)
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        iter: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        self.inner.for_each_token_id(iter, hw_counter, f)
    }

    fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        self.inner.filter_query(query, hw_counter)
    }

    fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        self.inner
            .estimate_query_cardinality(query, condition, hw_counter)
    }

    fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> OperationResult<bool> {
        self.inner.check_match(query, point_id)
    }

    fn for_each_payload_block_inner(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner.for_each_payload_block_inner(threshold, key, f)
    }

    fn get_storage_type(&self) -> StorageType {
        StorageType::Gridstore
    }

    fn ram_usage_bytes(&self) -> usize {
        self.inner.ram_usage_bytes()
    }

    fn is_on_disk(&self) -> bool {
        self.inner.is_on_disk()
    }
}
