use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use fs_err as fs;
use serde_json::Value;

use super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::inverted_index::mmap_inverted_index::MmapInvertedIndex;
use super::inverted_index::mutable_inverted_index::MutableInvertedIndex;
use super::inverted_index::{Document, InvertedIndex, TokenSet};
use super::text_index::FullTextIndex;
use super::tokenizers::Tokenizer;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::full_text_index::immutable_text_index::{
    ImmutableFullTextIndex, Storage,
};
use crate::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};

pub struct MmapFullTextIndex {
    pub(super) inverted_index: MmapInvertedIndex,
    pub(super) tokenizer: Tokenizer,
}

impl MmapFullTextIndex {
    pub fn open(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
    ) -> OperationResult<Option<Self>> {
        let populate = !is_on_disk;

        let has_positions = config.phrase_matching == Some(true);
        let tokenizer = Tokenizer::new_from_text_index_params(&config);

        let inverted_index = MmapInvertedIndex::open(path, populate, has_positions)?;
        Ok(inverted_index.map(|inverted_index| Self {
            inverted_index,
            tokenizer,
        }))
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.inverted_index.files()
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.inverted_index.immutable_files()
    }

    fn path(&self) -> &PathBuf {
        &self.inverted_index.path
    }

    pub fn wipe(self) -> OperationResult<()> {
        let files = self.files();
        let path = self.path();
        for file in files {
            fs::remove_file(file)?;
        }
        let _ = fs::remove_dir(path);
        Ok(())
    }

    pub fn remove_point(&mut self, id: PointOffsetType) {
        self.inverted_index.remove(id);
    }

    pub fn flusher(&self) -> (Flusher, Flusher) {
        self.inverted_index.flusher()
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
    tokenizer: Tokenizer,
}

impl FullTextMmapIndexBuilder {
    pub fn new(path: PathBuf, config: TextIndexParams, is_on_disk: bool) -> Self {
        let with_positions = config.phrase_matching.unwrap_or_default();
        let tokenizer = Tokenizer::new_from_text_index_params(&config);
        Self {
            path,
            mutable_index: MutableInvertedIndex::new(with_positions),
            config,
            is_on_disk,
            tokenizer,
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

        let mut str_tokens = Vec::new();

        for value in &values {
            self.tokenizer.tokenize_doc(value, |token| {
                str_tokens.push(token);
            });
        }

        let tokens = self.mutable_index.register_tokens(&str_tokens);

        if self.mutable_index.point_to_doc.is_some() {
            let document = Document::new(tokens.clone());
            self.mutable_index
                .index_document(id, document, hw_counter)?;
        }

        let token_set = TokenSet::from_iter(tokens);
        self.mutable_index.index_tokens(id, token_set, hw_counter)?;

        Ok(())
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.mutable_index.remove(id);

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
            tokenizer,
        } = self;

        let immutable = ImmutableInvertedIndex::from(mutable_index);

        fs::create_dir_all(path.as_path())?;

        MmapInvertedIndex::create(path.clone(), &immutable)?;

        let populate = !is_on_disk;
        let has_positions = config.phrase_matching.unwrap_or_default();
        let inverted_index =
            MmapInvertedIndex::open(path, populate, has_positions)?.ok_or_else(|| {
                OperationError::service_error(
                    "Failed to open MmapInvertedIndex that was just created",
                )
            })?;

        let mmap_index = MmapFullTextIndex {
            inverted_index,
            tokenizer: tokenizer.clone(),
        };

        let text_index = if is_on_disk {
            FullTextIndex::Mmap(Box::new(mmap_index))
        } else {
            FullTextIndex::Immutable(ImmutableFullTextIndex {
                inverted_index: immutable,
                tokenizer,
                storage: Storage::Mmap(Box::new(mmap_index)),
            })
        };

        Ok(text_index)
    }
}
