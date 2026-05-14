use std::borrow::Cow;
use std::path::PathBuf;

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UserData;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::immutable_text_index::ImmutableFullTextIndex;
use super::inverted_index::{ParsedQuery, TokenId};
use super::mmap_text_index::{FullTextMmapIndexBuilder, MmapFullTextIndex};
use super::mutable_text_index::MutableFullTextIndex;
use super::read_ops::{self, FullTextIndexRead};
use super::tokenizers::Tokenizer;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexRead, ValueIndexer,
};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

/// Selects how a text query is parsed and matched against the payload.
pub enum PayloadMatchQueryType {
    /// All query tokens must be present in the document (any order).
    Text,
    /// All query tokens must be present in exact order.
    Phrase,
    /// At least one query token must be present.
    TextAny,
}

#[allow(clippy::large_enum_variant)]
pub enum FullTextIndex {
    Mutable(MutableFullTextIndex),
    Immutable(ImmutableFullTextIndex),
    Mmap(Box<MmapFullTextIndex<common::universal_io::MmapFile>>),
}

impl FullTextIndex {
    pub fn new_mmap(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode downgrades the in-RAM `Immutable` wrapper to the
        // pure-mmap variant at load time. Files are shared between variants;
        // the persisted `is_on_disk` flag in `mmap_index` is untouched.
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let Some(mmap_index) =
            MmapFullTextIndex::open(path, config, effective_is_on_disk, deleted_points)?
        else {
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            // Use on mmap directly
            Some(Self::Mmap(Box::new(mmap_index)))
        } else {
            // Load into RAM, use mmap as backing storage
            Some(Self::Immutable(ImmutableFullTextIndex::open_mmap(
                mmap_index,
            )?))
        };
        Ok(index)
    }

    pub fn new_gridstore(
        dir: PathBuf,
        config: TextIndexParams,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let index = MutableFullTextIndex::open_gridstore(dir, config, create_if_missing)?;
        Ok(index.map(Self::Mutable))
    }

    pub fn init(&mut self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.init(),
            Self::Immutable(_) => {
                debug_assert!(false, "Immutable index should be initialized before use");
                Ok(())
            }
            Self::Mmap(_) => {
                debug_assert!(false, "Mmap index should be initialized before use");
                Ok(())
            }
        }
    }

    pub fn builder_mmap(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> FullTextMmapIndexBuilder {
        FullTextMmapIndexBuilder::new(path, config, is_on_disk, deleted_points)
    }

    pub fn builder_gridstore(
        dir: PathBuf,
        config: TextIndexParams,
    ) -> FullTextGridstoreIndexBuilder {
        FullTextGridstoreIndexBuilder::new(dir, config)
    }

    pub(super) fn serialize_document(tokens: Vec<Cow<str>>) -> OperationResult<Vec<u8>> {
        #[derive(Serialize)]
        struct StoredDocument<'a> {
            tokens: Vec<Cow<'a, str>>,
        }
        let doc = StoredDocument { tokens };
        serde_cbor::to_vec(&doc).map_err(|e| {
            OperationError::service_error(format!("Failed to serialize document: {e}"))
        })
    }

    pub(super) fn deserialize_document(data: &[u8]) -> OperationResult<Vec<String>> {
        #[derive(Deserialize)]
        struct StoredDocument {
            tokens: Vec<String>,
        }
        serde_cbor::from_slice::<StoredDocument>(data)
            .map_err(|e| {
                OperationError::service_error(format!("Failed to deserialize document: {e}"))
            })
            .map(|doc| doc.tokens)
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            index_type: self.telemetry_index_type(),
            points_values_count: FullTextIndexRead::points_count(self),
            points_count: FullTextIndexRead::points_count(self),
            histogram_bucket_size: None,
        }
    }

    #[cfg(test)]
    pub fn query<'a>(
        &'a self,
        query: &'a str,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let Some(parsed_query) = self.parse_text_query(query, hw_counter)? else {
            return Ok(Box::new(std::iter::empty()));
        };
        self.filter_query(parsed_query, hw_counter)
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            FullTextIndex::Mutable(_) => IndexMutability::Mutable,
            FullTextIndex::Immutable(_) => IndexMutability::Immutable,
            FullTextIndex::Mmap(_) => IndexMutability::Immutable,
        }
    }
}

impl ValueIndexer for FullTextIndex {
    type ValueType = String;

    fn add_many(
        &mut self,
        idx: PointOffsetType,
        values: Vec<String>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.add_many(idx, values, hw_counter),
            Self::Immutable(_) => Err(OperationError::service_error(
                "Cannot add values to immutable text index",
            )),
            Self::Mmap(_) => Err(OperationError::service_error(
                "Cannot add values to mmap text index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<String> {
        value.as_str().map(ToOwned::to_owned)
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            FullTextIndex::Mutable(index) => index.remove_point(id)?,
            FullTextIndex::Immutable(index) => index.remove_point(id),
            FullTextIndex::Mmap(index) => index.remove_point(id),
        }
        Ok(())
    }
}

impl PayloadFieldIndex for FullTextIndex {
    fn wipe(self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.wipe(),
            Self::Immutable(index) => index.wipe(),
            Self::Mmap(index) => index.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            Self::Mutable(index) => index.flusher(),
            Self::Immutable(index) => index.flusher(),
            Self::Mmap(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        FullTextIndexRead::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        FullTextIndexRead::immutable_files(self)
    }
}

impl FullTextIndexRead for FullTextIndex {
    fn tokenizer(&self) -> &Tokenizer {
        match self {
            Self::Mutable(index) => index.tokenizer(),
            Self::Immutable(index) => index.tokenizer(),
            Self::Mmap(index) => index.tokenizer(),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            Self::Mutable(index) => index.telemetry_index_type(),
            Self::Immutable(index) => index.telemetry_index_type(),
            Self::Mmap(index) => index.telemetry_index_type(),
        }
    }

    fn points_count(&self) -> usize {
        match self {
            Self::Mutable(index) => index.points_count(),
            Self::Immutable(index) => index.points_count(),
            Self::Mmap(index) => index.points_count(),
        }
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            Self::Mutable(index) => index.values_count(point_id),
            Self::Immutable(index) => index.values_count(point_id),
            Self::Mmap(index) => index.values_count(point_id),
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            Self::Mutable(index) => index.values_is_empty(point_id),
            Self::Immutable(index) => index.values_is_empty(point_id),
            Self::Mmap(index) => index.values_is_empty(point_id),
        }
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        iter: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.for_each_token_id(iter, hw_counter, f),
            Self::Immutable(index) => index.for_each_token_id(iter, hw_counter, f),
            Self::Mmap(index) => index.for_each_token_id(iter, hw_counter, f),
        }
    }

    fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match self {
            Self::Mutable(index) => index.filter_query(query, hw_counter),
            Self::Immutable(index) => index.filter_query(query, hw_counter),
            Self::Mmap(index) => index.filter_query(query, hw_counter),
        }
    }

    fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        match self {
            Self::Mutable(index) => index.estimate_query_cardinality(query, condition, hw_counter),
            Self::Immutable(index) => {
                index.estimate_query_cardinality(query, condition, hw_counter)
            }
            Self::Mmap(index) => index.estimate_query_cardinality(query, condition, hw_counter),
        }
    }

    fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> OperationResult<bool> {
        match self {
            Self::Mutable(index) => index.check_match(query, point_id),
            Self::Immutable(index) => index.check_match(query, point_id),
            Self::Mmap(index) => index.check_match(query, point_id),
        }
    }

    fn for_each_payload_block_inner(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.for_each_payload_block_inner(threshold, key, f),
            Self::Immutable(index) => index.for_each_payload_block_inner(threshold, key, f),
            Self::Mmap(index) => index.for_each_payload_block_inner(threshold, key, f),
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            Self::Mutable(index) => FullTextIndexRead::get_storage_type(index),
            Self::Immutable(index) => FullTextIndexRead::get_storage_type(index),
            Self::Mmap(index) => FullTextIndexRead::get_storage_type(index.as_ref()),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            Self::Mutable(index) => FullTextIndexRead::ram_usage_bytes(index),
            Self::Immutable(index) => FullTextIndexRead::ram_usage_bytes(index),
            Self::Mmap(index) => FullTextIndexRead::ram_usage_bytes(index.as_ref()),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            Self::Mutable(index) => FullTextIndexRead::is_on_disk(index),
            Self::Immutable(index) => FullTextIndexRead::is_on_disk(index),
            Self::Mmap(index) => FullTextIndexRead::is_on_disk(index.as_ref()),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => FullTextIndexRead::populate(index),
            Self::Immutable(index) => FullTextIndexRead::populate(index),
            Self::Mmap(index) => FullTextIndexRead::populate(index.as_ref()),
        }
    }

    fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => FullTextIndexRead::clear_cache(index),
            Self::Immutable(index) => FullTextIndexRead::clear_cache(index),
            Self::Mmap(index) => FullTextIndexRead::clear_cache(index.as_ref()),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(index) => FullTextIndexRead::files(index),
            Self::Immutable(index) => FullTextIndexRead::files(index),
            Self::Mmap(index) => FullTextIndexRead::files(index.as_ref()),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(_) => Vec::new(),
            Self::Immutable(index) => FullTextIndexRead::immutable_files(index),
            Self::Mmap(index) => FullTextIndexRead::immutable_files(index.as_ref()),
        }
    }
}

impl PayloadFieldIndexRead for FullTextIndex {
    fn count_indexed_points(&self) -> usize {
        FullTextIndexRead::points_count(self)
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        read_ops::filter(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        read_ops::estimate_cardinality(self, condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        read_ops::for_each_payload_block(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>> {
        read_ops::special_check_condition(self, condition, payload_value, hw_counter)
    }
}

pub struct FullTextGridstoreIndexBuilder {
    dir: PathBuf,
    config: TextIndexParams,
    index: Option<FullTextIndex>,
}

impl FullTextGridstoreIndexBuilder {
    pub fn new(dir: PathBuf, config: TextIndexParams) -> Self {
        Self {
            dir,
            config,
            index: None,
        }
    }
}

impl ValueIndexer for FullTextGridstoreIndexBuilder {
    type ValueType = String;

    fn get_value(value: &Value) -> Option<String> {
        FullTextIndex::get_value(value)
    }

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let values: Vec<Value> = values.into_iter().map(Value::String).collect();
        let values: Vec<&Value> = values.iter().collect();
        FieldIndexBuilderTrait::add_point(self, id, &values, hw_counter)
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        let Some(index) = &mut self.index else {
            return Err(OperationError::service_error(
                "FullTextIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.remove_point(id)
    }
}

impl FieldIndexBuilderTrait for FullTextGridstoreIndexBuilder {
    type FieldIndexType = FullTextIndex;

    fn init(&mut self) -> OperationResult<()> {
        assert!(
            self.index.is_none(),
            "index must be initialized exactly once",
        );
        self.index.replace(
            FullTextIndex::new_gridstore(self.dir.clone(), self.config.clone(), true)?.ok_or_else(
                || {
                    OperationError::service_error(
                        "Failed to create and open mutable full text index on gridstore",
                    )
                },
            )?,
        );
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(index) = &mut self.index else {
            return Err(OperationError::service_error(
                "FullTextIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.add_point(id, payload, hw_counter)
    }

    fn finalize(mut self) -> OperationResult<Self::FieldIndexType> {
        let Some(index) = self.index.take() else {
            return Err(OperationError::service_error(
                "FullTextIndexGridstoreBuilder: index must be initialized to finalize",
            ));
        };
        index.flusher()()?;
        Ok(index)
    }
}
