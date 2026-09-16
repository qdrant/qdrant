use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, MmapFs, Populate, UniversalRead, UniversalReadFs};
use fs_err as fs;
use serde_json::Value;

use super::super::FullTextIndex;
use super::super::immutable_text_index::ImmutableFullTextIndex;
use super::super::inverted_index::InvertedIndex;
use super::super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::super::inverted_index::mutable_inverted_index::MutableInvertedIndex;
use super::super::inverted_index::on_disk_inverted_index::OnDiskInvertedIndex;
use super::super::tokenizers::Tokenizer;
use super::{FullTextMmapIndexBuilder, OnDiskFullTextIndex};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::{FieldIndexBuilderTrait, ValueIndexer};

impl<S: UniversalRead> OnDiskFullTextIndex<S> {
    /// Schedule background prefetch of every file [`open`](Self::open) will read.
    ///
    /// Returns `false` when the index is not in the on-disk format.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        // Inverted index
        OnDiskInvertedIndex::<S>::preopen(fs, path, populate)
    }

    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        config: TextIndexParams,
        populate: Populate,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let has_positions = config.phrase_matching == Some(true);
        let tokenizer = Tokenizer::new_from_text_index_params(&config);

        let inverted_index =
            OnDiskInvertedIndex::<S>::open(fs, path, populate, has_positions, deleted_points)?;
        Ok(inverted_index.map(|inverted_index| Self {
            inverted_index,
            tokenizer,
        }))
    }

    /// Whether this index has document lengths on disk.
    pub fn records_doc_len(&self) -> bool {
        self.inverted_index.records_doc_len()
    }

    pub fn wipe(self) -> OperationResult<()> {
        let path = self.inverted_index.path.clone();
        // drop mmap handles before deleting files
        drop(self);
        // Remove the directory rather than the files `files()` reports. That
        // list carries the `doc_len` sidecar only when the index loaded it, so
        // a truncated one is omitted, and deleting file by file would leave it
        // behind and keep the directory alive.
        match fs::remove_dir_all(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn remove_point(&mut self, id: PointOffsetType) {
        self.inverted_index.remove(id);
    }

    pub fn flusher(&self) -> Flusher {
        self.inverted_index.flusher()
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.inverted_index.populate()?;
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.inverted_index.clear_cache()?;
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.inverted_index.files()
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.inverted_index.immutable_files()
    }
}

impl FullTextMmapIndexBuilder {
    pub fn new(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
        deleted_points: &BitSlice,
        scoring: bool,
    ) -> Self {
        let with_positions = config.phrase_matching.unwrap_or_default();
        let tokenizer = Tokenizer::new_from_text_index_params(&config);
        Self {
            path,
            mutable_index: MutableInvertedIndex::new(with_positions, scoring),
            config,
            is_on_disk,
            tokenizer,
            deleted_points: deleted_points.to_owned(),
        }
    }
}

impl ValueIndexer for FullTextMmapIndexBuilder {
    type ValueType = String;

    fn get_value(value: &Value) -> Option<String> {
        match value {
            Value::String(s) => Some(s.clone()),
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::Array(_)
            | Value::Object(_) => None,
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

        // Through the shared helper: `document_length` subtracts the sentinels
        // it inserts, so the two rules have to agree. Read from the config like
        // the other two callers, rather than from whether a container happens
        // to be allocated, so that the two halves cannot drift apart.
        let phrase_matching = self.config.phrase_matching.unwrap_or_default();
        let str_tokens =
            FullTextIndex::tokenize_document(&self.tokenizer, phrase_matching, &values);

        // Measured here, the last point at which every token exists.
        let doc_len = self
            .mutable_index
            .records_doc_len()
            .then(|| FullTextIndex::document_length(&str_tokens, phrase_matching, &values));

        self.mutable_index
            .index_str_tokens(id, &str_tokens, doc_len, hw_counter)
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
            deleted_points,
        } = self;

        let immutable = ImmutableInvertedIndex::from(mutable_index);

        fs::create_dir_all(path.as_path())?;

        OnDiskInvertedIndex::create(path.clone(), &immutable)?;

        let populate = Populate::from(!is_on_disk);
        let has_positions = config.phrase_matching.unwrap_or_default();
        let inverted_index =
            OnDiskInvertedIndex::open(&MmapFs, path, populate, has_positions, &deleted_points)?
                .ok_or_else(|| {
                    OperationError::service_error(
                        "Failed to open OnDiskInvertedIndex that was just created",
                    )
                })?;

        let on_disk_index = OnDiskFullTextIndex {
            inverted_index,
            tokenizer,
        };

        let text_index = if is_on_disk {
            FullTextIndex::OnDisk(on_disk_index)
        } else {
            FullTextIndex::Immutable(ImmutableFullTextIndex::load_from_on_disk(on_disk_index)?)
        };

        Ok(text_index)
    }
}
