use std::marker::PhantomData;
use std::path::PathBuf;

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{Populate, UniversalRead, UniversalReadFs};

use super::super::inverted_index::InvertedIndex;
use super::super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::super::tokenizers::Tokenizer;
use super::ImmutableFullTextIndex;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;

impl<S: UniversalRead> ImmutableFullTextIndex<S> {
    /// Read the on-disk index at `path` into RAM, or `None` when there is no
    /// index at `path`.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        config: TextIndexParams,
        populate: Populate,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let has_positions = config.phrase_matching == Some(true);
        let Some((inverted_index, files)) =
            ImmutableInvertedIndex::open(fs, &path, populate, has_positions, deleted_points)?
        else {
            return Ok(None);
        };
        let cached_ram_usage_bytes = inverted_index.ram_usage_bytes();
        Ok(Some(Self {
            inverted_index,
            tokenizer: Tokenizer::new_from_text_index_params(&config),
            path,
            files,
            cached_ram_usage_bytes,
            _storage: PhantomData,
        }))
    }

    /// Whether this index has document lengths on disk.
    pub fn records_doc_len(&self) -> bool {
        self.inverted_index.total_tokens().is_some()
    }

    /// Not persisted: the id-tracker re-supplies deletions on reload.
    pub fn remove_point(&mut self, id: PointOffsetType) {
        self.inverted_index.remove(id);
    }

    /// Remove the directory rather than the files `files()` reports, for the
    /// same reason
    /// [`OnDiskFullTextIndex::wipe`](super::super::on_disk_text_index::OnDiskFullTextIndex::wipe)
    /// does.
    pub fn wipe(self) -> OperationResult<()> {
        fs_err::remove_dir_all(&self.path)?;
        Ok(())
    }

    /// No-op flusher: the on-disk files are build-time only.
    #[allow(clippy::unused_self)]
    pub fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.files.clone()
    }

    /// Every file is written once at build time, as for the on-disk index.
    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.files()
    }
}
