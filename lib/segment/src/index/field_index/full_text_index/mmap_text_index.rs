use std::collections::BTreeSet;
use std::fs::{create_dir_all, remove_dir};
use std::path::PathBuf;

use common::types::PointOffsetType;
use serde_json::Value;

use super::immutable_inverted_index::ImmutableInvertedIndex;
use super::inverted_index::InvertedIndex;
use super::mmap_inverted_index::MmapInvertedIndex;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::text_index::FullTextIndex;
use super::tokenizers::Tokenizer;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};

pub struct MmapFullTextIndex {
    pub(super) inverted_index: MmapInvertedIndex,
    pub(super) config: TextIndexParams,
}

impl MmapFullTextIndex {
    pub fn open(path: PathBuf, config: TextIndexParams) -> OperationResult<Self> {
        let inverted_index = MmapInvertedIndex::open(path, false)?;

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
}

pub struct FullTextMmapIndexBuilder {
    path: PathBuf,
    mutable_index: MutableInvertedIndex,
    config: TextIndexParams,
}

impl FullTextMmapIndexBuilder {
    pub fn new(path: PathBuf, config: TextIndexParams) -> Self {
        Self {
            path,
            mutable_index: MutableInvertedIndex::default(),
            config,
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
        self.mutable_index.index_document(id, document)?;

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

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        ValueIndexer::add_point(self, id, payload)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        let Self {
            path,
            mutable_index,
            config,
        } = self;

        let immutable = ImmutableInvertedIndex::from(mutable_index);

        create_dir_all(path.as_path())?;

        MmapInvertedIndex::create(path.clone(), immutable)?;

        let inverted_index = MmapInvertedIndex::open(path, false)?;

        let mmap_index = MmapFullTextIndex {
            inverted_index,
            config,
        };

        Ok(FullTextIndex::Mmap(Box::new(mmap_index)))
    }
}
