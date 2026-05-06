use std::path::PathBuf;

use common::types::PointOffsetType;

use super::inverted_index::InvertedIndex;
use super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::mmap_text_index::MmapFullTextIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::payload_config::StorageType;

pub struct ImmutableFullTextIndex {
    pub(super) inverted_index: ImmutableInvertedIndex,
    // Backing storage, source of state, persists deletions
    pub(super) storage: Storage,
    /// Snapshot of approximate RAM usage at construction time.
    /// Not refreshed on `remove_point`.
    cached_ram_usage_bytes: usize,
}

pub(super) enum Storage {
    Mmap(Box<MmapFullTextIndex>),
}

impl ImmutableFullTextIndex {
    /// Open and load immutable full text index from mmap storage
    pub fn open_mmap(index: MmapFullTextIndex) -> OperationResult<Self> {
        let inverted_index = ImmutableInvertedIndex::try_from(&index.inverted_index)?;

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap full text index: {err}");
        }

        let mut result = Self {
            inverted_index,
            storage: Storage::Mmap(Box::new(index)),
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
            match self.storage {
                Storage::Mmap(ref mut index) => index.remove_point(id),
            }
        }
    }

    pub fn wipe(self) -> OperationResult<()> {
        match self.storage {
            Storage::Mmap(index) => index.wipe(),
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of mmap storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match &self.storage {
            Storage::Mmap(index) => index.clear_cache().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear immutable full text index gridstore cache: {err}"
                ))
            }),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self.storage {
            Storage::Mmap(ref index) => index.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self.storage {
            Storage::Mmap(ref index) => index.immutable_files(),
        }
    }

    pub fn flusher(&self) -> Flusher {
        match self.storage {
            Storage::Mmap(ref index) => index.flusher(),
        }
    }

    pub fn storage_type(&self) -> StorageType {
        match &self.storage {
            Storage::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }

    /// Approximate RAM usage in bytes (cached at construction).
    pub fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }
}
