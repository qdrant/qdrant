use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
#[cfg(feature = "testing")]
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::storage_version::StorageVersion as _;
use common::types::PointOffsetType;
use fs_err as fs;
use semver::Version;
use sparse::common::scores_memory_pool::ScoresMemoryPool;
use sparse::common::sparse_vector::SparseVector;
use sparse::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
use sparse::index::inverted_index::{INDEX_FILE_NAME, InvertedIndex, OLD_INDEX_FILE_NAME};

use super::indices_tracker::IndicesTracker;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::sparse_index::sparse_index_config::SparseIndexConfig;
use crate::index::sparse_index::sparse_search_telemetry::SparseSearchesTelemetry;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

mod search;
mod vector_index_impl;

/// Whether to use the new compressed format.
pub const USE_COMPRESSED: bool = true;

#[derive(Debug)]
pub struct SparseVectorIndex<TInvertedIndex: InvertedIndex> {
    config: SparseIndexConfig,
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    path: PathBuf,
    inverted_index: TInvertedIndex,
    searches_telemetry: SparseSearchesTelemetry,
    indices_tracker: IndicesTracker,
    scores_memory_pool: ScoresMemoryPool,
    deferred_internal_id: Option<PointOffsetType>,
}

/// Getters for internals, used for testing.
#[cfg(feature = "testing")]
impl<TInvertedIndex: InvertedIndex> SparseVectorIndex<TInvertedIndex> {
    pub fn config(&self) -> SparseIndexConfig {
        self.config
    }

    pub fn id_tracker(&self) -> &Arc<AtomicRefCell<IdTrackerEnum>> {
        &self.id_tracker
    }

    pub fn vector_storage(&self) -> &Arc<AtomicRefCell<VectorStorageEnum>> {
        &self.vector_storage
    }

    pub fn payload_index(&self) -> &Arc<AtomicRefCell<StructPayloadIndex>> {
        &self.payload_index
    }

    pub fn indices_tracker(&self) -> &IndicesTracker {
        &self.indices_tracker
    }
}

pub struct SparseVectorIndexOpenArgs<'a, F: FnMut()> {
    pub config: SparseIndexConfig,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub path: &'a Path,
    pub stopped: &'a AtomicBool,
    pub tick_progress: F,
    pub deferred_internal_id: Option<PointOffsetType>,
}

impl<TInvertedIndex: InvertedIndex> SparseVectorIndex<TInvertedIndex> {
    /// Open a sparse vector index at a given path
    pub fn open<F: FnMut()>(args: SparseVectorIndexOpenArgs<F>) -> OperationResult<Self> {
        let SparseVectorIndexOpenArgs {
            config,
            id_tracker,
            vector_storage,
            payload_index,
            path,
            stopped,
            tick_progress,
            deferred_internal_id,
        } = args;

        let config_path = SparseIndexConfig::get_config_path(path);

        let (config, inverted_index, indices_tracker) = if !config.index_type.is_persisted() {
            // RAM mutable case - build inverted index from scratch and use provided config
            fs::create_dir_all(path)?;
            let (inverted_index, indices_tracker) = Self::build_inverted_index(
                &id_tracker,
                &vector_storage,
                path,
                stopped,
                tick_progress,
            )?;
            (config, inverted_index, indices_tracker)
        } else {
            Self::try_load(path).or_else(|e| {
                if fs::exists(path).unwrap_or(true) {
                    log::warn!("Failed to load {path:?}, rebuilding: {e}");

                    // Drop index completely.
                    fs::remove_dir_all(path)?;
                }

                fs::create_dir_all(path)?;

                let (inverted_index, indices_tracker) = Self::build_inverted_index(
                    &id_tracker,
                    &vector_storage,
                    path,
                    stopped,
                    tick_progress,
                )?;

                config.save(&config_path)?;
                inverted_index.save(path)?;
                indices_tracker.save(path)?;

                // Save the version as the last step to mark a successful rebuild.
                // NOTE: index in the original format (Qdrant <=v1.9 / sparse <=v0.1.0) lacks of the
                // version file. To distinguish between index in original format and partially
                // written index in the current format, the index file name is changed from
                // `inverted_index.data` to `inverted_index.dat`.
                TInvertedIndex::Version::save(path)?;

                OperationResult::Ok((config, inverted_index, indices_tracker))
            })?
        };

        let searches_telemetry = SparseSearchesTelemetry::new();
        let path = path.to_path_buf();
        let scores_memory_pool = ScoresMemoryPool::new();
        Ok(Self {
            config,
            id_tracker,
            vector_storage,
            payload_index,
            path,
            inverted_index,
            searches_telemetry,
            indices_tracker,
            scores_memory_pool,
            deferred_internal_id,
        })
    }

    fn try_load(
        path: &Path,
    ) -> OperationResult<(SparseIndexConfig, TInvertedIndex, IndicesTracker)> {
        let mut stored_version = TInvertedIndex::Version::load(path)?;

        // Simple migration mechanism for 0.1.0.
        let old_path = path.join(OLD_INDEX_FILE_NAME);
        if TInvertedIndex::Version::current() == Version::new(0, 1, 0) && old_path.exists() {
            // Didn't have a version file, but uses 0.1.0 index. Create a version file.
            fs::rename(old_path, path.join(INDEX_FILE_NAME))?;
            TInvertedIndex::Version::save(path)?;
            stored_version = Some(TInvertedIndex::Version::current());
        }

        if stored_version != Some(TInvertedIndex::Version::current()) {
            return Err(OperationError::service_error_light(format!(
                "Index version mismatch, expected {}, found {}",
                TInvertedIndex::Version::current(),
                stored_version.map_or_else(|| "none".to_string(), |v| v.to_string()),
            )));
        }

        let loaded_config = SparseIndexConfig::load(&SparseIndexConfig::get_config_path(path))?;
        let inverted_index = TInvertedIndex::open(path)?;
        let indices_tracker = IndicesTracker::open(path)?;
        Ok((loaded_config, inverted_index, indices_tracker))
    }

    fn build_inverted_index(
        id_tracker: &AtomicRefCell<IdTrackerEnum>,
        vector_storage: &AtomicRefCell<VectorStorageEnum>,
        path: &Path,
        stopped: &AtomicBool,
        mut tick_progress: impl FnMut(),
    ) -> OperationResult<(TInvertedIndex, IndicesTracker)> {
        let borrowed_vector_storage = vector_storage.borrow();
        let borrowed_id_tracker = id_tracker.borrow();
        let deleted_bitslice = borrowed_vector_storage.deleted_vector_bitslice();

        let mut ram_index_builder = InvertedIndexBuilder::new();
        let mut indices_tracker = IndicesTracker::default();
        for id in borrowed_id_tracker
            .point_mappings()
            .iter_internal_excluding(deleted_bitslice)
        {
            check_process_stopped(stopped)?;
            // It is possible that the vector is not present in the storage in case of crash.
            // Because:
            // - the `id_tracker` is flushed before the `vector_storage`
            // - the sparse index is built *before* recovering the WAL when loading a segment
            match borrowed_vector_storage.get_vector_opt::<Random>(id) {
                None => {
                    // the vector was lost in a crash but will be recovered by the WAL
                    let point_id = borrowed_id_tracker.external_id(id);
                    let point_version = borrowed_id_tracker.internal_version(id);
                    log::debug!(
                        "Sparse vector with id {id} is not found, external_id: {point_id:?}, version: {point_version:?}",
                    )
                }
                Some(vector) => {
                    let vector: &SparseVector = vector.as_vec_ref().try_into()?;
                    // do not index empty vectors
                    if vector.is_empty() {
                        continue;
                    }
                    indices_tracker.register_indices(vector);
                    let vector = indices_tracker.remap_vector(vector.to_owned());
                    ram_index_builder.add(id, vector);
                }
            }
            tick_progress();
        }
        Ok((
            TInvertedIndex::from_ram_index(Cow::Owned(ram_index_builder.build()), path)?,
            indices_tracker,
        ))
    }

    pub fn inverted_index(&self) -> &TInvertedIndex {
        &self.inverted_index
    }

    /// Returns the maximum number of results that can be returned by the index for a given sparse vector
    /// Warning: the cost of this function grows with the number of dimensions in the query vector
    #[cfg(feature = "testing")]
    pub fn max_result_count(&self, query_vector: &SparseVector) -> usize {
        use sparse::index::posting_list_common::PostingListIter as _;

        // For tests only
        let hw_counter = HardwareCounterCell::disposable();

        let mut unique_record_ids = std::collections::HashSet::new();
        for dim_id in &query_vector.indices {
            if let Some(dim_id) = self.indices_tracker.remap_index(*dim_id)
                && let Some(posting_list_iter) = self.inverted_index.get(dim_id, &hw_counter)
            {
                for element in posting_list_iter.into_std_iter() {
                    unique_record_ids.insert(element.record_id);
                }
            }
        }
        unique_record_ids.len()
    }
}
