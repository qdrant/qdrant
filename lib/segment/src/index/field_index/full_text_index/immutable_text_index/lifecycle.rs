use std::path::PathBuf;

use common::types::PointOffsetType;
use common::universal_io::MmapFile;

use super::super::inverted_index::InvertedIndex;
use super::super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::super::mmap_text_index::MmapFullTextIndex;
use super::ImmutableFullTextIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

impl ImmutableFullTextIndex {
    /// Open and load immutable full text index from mmap storage
    pub fn open_mmap(index: MmapFullTextIndex<MmapFile>) -> OperationResult<Self> {
        Self::try_open_mmap(Box::new(index)).map_err(|(_, err)| err)
    }

    /// Like [`Self::open_mmap`] but returns the (unconsumed) mmap alongside
    /// the error on failure, so an in-place swap can restore it instead of
    /// aborting.
    pub fn try_open_mmap(
        index: Box<MmapFullTextIndex<MmapFile>>,
    ) -> Result<Self, (Box<MmapFullTextIndex<MmapFile>>, OperationError)> {
        let inverted_index = match ImmutableInvertedIndex::try_from(&index.inverted_index) {
            Ok(inverted_index) => inverted_index,
            Err(err) => return Err((index, err)),
        };

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.inverted_index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap full text index: {err}");
        }

        let mut result = Self {
            inverted_index,
            storage: index,
            cached_ram_usage_bytes: 0,
        };
        result.cached_ram_usage_bytes = result.inverted_index.ram_usage_bytes();
        Ok(result)
    }

    /// Apply the deletion to both `inverted_index` (the in-RAM cache used
    /// by queries) and `storage` (keeps the mmap's `points_count()` in
    /// sync; not persisted — id-tracker re-supplies on reload).
    pub fn remove_point(&mut self, id: PointOffsetType) {
        if self.inverted_index.remove(id) {
            self.storage.remove_point(id);
        }
    }

    pub fn wipe(self) -> OperationResult<()> {
        self.storage.wipe()
    }

    pub fn flusher(&self) -> Flusher {
        self.storage.flusher()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear immutable full text index gridstore cache: {err}"
            ))
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.storage.immutable_files()
    }
}
