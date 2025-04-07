use std::collections::BTreeSet;
use std::fs::{create_dir_all, remove_dir};
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::immutable_inverted_index::ImmutableInvertedIndex;
use super::inverted_index::InvertedIndex;
use super::mmap_inverted_index::MmapInvertedIndex;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::text_index::FullTextIndex;
use super::tokenizers::Tokenizer;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};

pub struct MmapFullTextIndex {
    pub(super) inverted_index: MmapInvertedIndex,
    pub(super) config: TextIndexParams,
}

impl MmapFullTextIndex {
    pub fn open(path: PathBuf, config: TextIndexParams, is_on_disk: bool) -> OperationResult<Self> {
        let populate = !is_on_disk;
        let inverted_index = MmapInvertedIndex::open(path, populate)?;

        Ok(Self {
            inverted_index,
            config,
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.inverted_index.files()
    }

    fn path(&self) -> &PathBuf {
        &self.inverted_index.path
    }

    pub fn clear(self) -> OperationResult<()> {
        let files = self.files();
        let path = self.path();
        for file in files {
            std::fs::remove_file(file)?;
        }
        let _ = remove_dir(path);
        Ok(())
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.inverted_index.remove_document(id);

        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        self.inverted_index.deleted_points.flusher()
    }

    pub fn is_on_disk(&self) -> bool {
        self.inverted_index.is_on_disk()
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.inverted_index.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.inverted_index.clear_cache()?;
        Ok(())
    }
}

pub struct FullTextMmapIndexBuilder {
    path: PathBuf,
    mutable_index: MutableInvertedIndex,
    config: TextIndexParams,
    is_on_disk: bool,
}

impl FullTextMmapIndexBuilder {
    pub fn new(path: PathBuf, config: TextIndexParams, is_on_disk: bool) -> Self {
        Self {
            path,
            mutable_index: MutableInvertedIndex::default(),
            config,
            is_on_disk,
        }
    }
}

impl ValueIndexer for FullTextMmapIndexBuilder {
    type ValueType = String;

    fn get_value(value: &Value) -> Option<String> {
        match value {
            Value::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let mut tokens: BTreeSet<String> = BTreeSet::new();

        for value in values {
            Tokenizer::tokenize_doc(&value, &self.config, |token| {
                tokens.insert(token.to_owned());
            });
        }

        let document = self.mutable_index.document_from_tokens(&tokens);
        self.mutable_index
            .index_document(id, document, hw_counter)?;

        Ok(())
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.mutable_index.remove_document(id);

        Ok(())
    }
}

impl FieldIndexBuilderTrait for FullTextMmapIndexBuilder {
    type FieldIndexType = FullTextIndex;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        ValueIndexer::add_point(self, id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        let Self {
            path,
            mutable_index,
            config,
            is_on_disk,
        } = self;

        let immutable = ImmutableInvertedIndex::from(mutable_index);

        create_dir_all(path.as_path())?;

        MmapInvertedIndex::create(path.clone(), immutable)?;

        let populate = !is_on_disk;
        let inverted_index = MmapInvertedIndex::open(path, populate)?;

        let mmap_index = MmapFullTextIndex {
            inverted_index,
            config,
        };

        Ok(FullTextIndex::Mmap(Box::new(mmap_index)))
    }
}
