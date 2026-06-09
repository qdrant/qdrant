use std::borrow::Cow;
use std::path::PathBuf;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::immutable_text_index::ImmutableFullTextIndex;
use super::mutable_text_index::MutableFullTextIndex;
use super::on_disk_text_index::{FullTextMmapIndexBuilder, OnDiskFullTextIndex};
use super::{FullTextGridstoreIndexBuilder, FullTextIndex};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex, ValueIndexer};
use crate::index::payload_config::IndexMutability;

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

        let populate = Populate::from(!effective_is_on_disk);
        let Some(on_disk_index) =
            OnDiskFullTextIndex::open(&MmapFs, path, config, populate, deleted_points)?
        else {
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            // Use on-disk directly
            Self::OnDisk(on_disk_index)
        } else {
            // Load into RAM, use mmap as backing storage
            Self::Immutable(ImmutableFullTextIndex::load_from_on_disk(on_disk_index)?)
        };
        Ok(Some(index))
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
            Self::OnDisk(_) => {
                debug_assert!(false, "On-disk index should be initialized before use");
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

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            FullTextIndex::Mutable(_) => IndexMutability::Mutable,
            FullTextIndex::Immutable(_) => IndexMutability::Immutable,
            FullTextIndex::OnDisk(_) => IndexMutability::Immutable,
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            // Mutable / Immutable keep their inverted index fully in RAM —
            // there is nothing to populate.
            Self::Mutable(_) | Self::Immutable(_) => Ok(()),
            Self::OnDisk(index) => index.populate(),
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.clear_cache(),
            Self::Immutable(index) => index.clear_cache(),
            Self::OnDisk(index) => index.clear_cache(),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(index) => index.files(),
            Self::Immutable(index) => index.files(),
            Self::OnDisk(index) => index.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(_) => Vec::new(),
            Self::Immutable(index) => index.immutable_files(),
            Self::OnDisk(index) => index.immutable_files(),
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
            Self::OnDisk(_) => Err(OperationError::service_error(
                "Cannot add values to on-disk text index",
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
            FullTextIndex::OnDisk(index) => index.remove_point(id),
        }
        Ok(())
    }
}

impl PayloadFieldIndex for FullTextIndex {
    fn wipe(self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.wipe(),
            Self::Immutable(index) => index.wipe(),
            Self::OnDisk(index) => index.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            Self::Mutable(index) => index.flusher(),
            Self::Immutable(index) => index.flusher(),
            Self::OnDisk(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        FullTextIndex::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        FullTextIndex::immutable_files(self)
    }
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
