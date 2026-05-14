use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UserData;

use super::super::inverted_index::{InvertedIndex, ParsedQuery, TokenId};
use super::super::read_ops::FullTextIndexRead;
use super::super::tokenizers::Tokenizer;
use super::ImmutableFullTextIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::StorageType;
use crate::types::{FieldCondition, PayloadKeyType};

impl FullTextIndexRead for ImmutableFullTextIndex {
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
            is_on_disk: self.storage.is_on_disk(),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn populate(&self) -> OperationResult<()> {
        Ok(())
    }

    fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear immutable full text index gridstore cache: {err}"
            ))
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.storage.immutable_files()
    }
}
