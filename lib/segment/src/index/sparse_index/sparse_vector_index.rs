use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
#[cfg(feature = "testing")]
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::storage_version::StorageVersion as _;
use common::universal_io::{MmapFile, MmapFs, UniversalReadFs};
use fs_err as fs;
use sparse::SearchScratchPool;
use sparse::common::sparse_vector::SparseVector;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use sparse::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
use sparse::index::inverted_index::{InvertedIndex, InvertedIndexReadWrite};

use self::read_view::{SparseVectorIndexReadView, SparseVectorIndexReadViewEnum};
use super::indices_tracker::IndicesTracker;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::sparse_index::sparse_index_config::SparseIndexConfig;
use crate::index::sparse_index::sparse_search_telemetry::SparseSearchesTelemetry;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

mod read_view;
mod vector_index_impl;

pub mod read_only;

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
    search_scratch_pool: SearchScratchPool,
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

pub struct SparseVectorIndexOpenArgs<'a, Fs: UniversalReadFs, F: FnMut()> {
    pub fs: &'a Fs,
    pub config: SparseIndexConfig,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub path: &'a Path,
    pub stopped: &'a AtomicBool,
    pub tick_progress: F,
}

/// Outcome of [`SparseVectorIndex::plan`]: whether the persisted index can be
/// loaded as-is, or must be (re)built from a freshly assembled RAM index.
///
/// The RAM index is type-agnostic, so the caller performs the type-specific
/// construction (`open` / `from_ram_index`) and then [`SparseVectorIndex::finish`].
pub enum SparseOpenPlan {
    /// The persisted index is current; load the inverted index from the path.
    Load {
        config: SparseIndexConfig,
        indices_tracker: IndicesTracker,
    },
    /// (Re)build: convert `ram_index` into the inverted index. Persist the
    /// result when `persist` is set (mutable RAM indexes are not persisted).
    Build {
        config: SparseIndexConfig,
        ram_index: InvertedIndexRam,
        indices_tracker: IndicesTracker,
        persist: bool,
    },
}

impl<TInvertedIndex: InvertedIndex> SparseVectorIndex<TInvertedIndex> {
    /// Open a sparse vector index at a given path
    pub fn open<F: FnMut()>(args: SparseVectorIndexOpenArgs<'_, MmapFs, F>) -> OperationResult<Self>
    where
        TInvertedIndex: InvertedIndexReadWrite<MmapFile>,
    {
        let SparseVectorIndexOpenArgs {
            fs,
            config,
            id_tracker,
            vector_storage,
            payload_index,
            path,
            stopped,
            tick_progress,
        } = args;

        let plan = Self::plan(
            config,
            &id_tracker,
            &vector_storage,
            path,
            stopped,
            tick_progress,
        )?;
        let (inverted_index, config, indices_tracker, persist) = match plan {
            SparseOpenPlan::Load {
                config,
                indices_tracker,
            } => (
                TInvertedIndex::open_rw(fs, path)?,
                config,
                indices_tracker,
                false,
            ),
            SparseOpenPlan::Build {
                config,
                ram_index,
                indices_tracker,
                persist,
            } => (
                TInvertedIndex::from_ram_index(fs, Cow::Owned(ram_index), path)?,
                config,
                indices_tracker,
                persist,
            ),
        };

        if persist {
            config.save(&SparseIndexConfig::get_config_path(path))?;
            inverted_index.save(path)?;
            indices_tracker.save(path)?;
            // Save the version last to mark a successful (re)build.
            TInvertedIndex::Version::save(path)?;
        }

        Ok(Self {
            config,
            id_tracker,
            vector_storage,
            payload_index,
            path: path.to_path_buf(),
            inverted_index,
            searches_telemetry: SparseSearchesTelemetry::new(),
            indices_tracker,
            search_scratch_pool: SearchScratchPool::new(),
        })
    }

    /// Decide whether the on-disk index can be loaded or must be (re)built.
    ///
    /// A rebuild assembles the RAM index here (it is type-agnostic); the caller
    /// then constructs the concrete inverted index from the returned plan and
    /// hands everything to [`Self::finish`]. This keeps `SparseVectorIndex` free
    /// of any inverted-index construction callbacks.
    ///
    /// A persisted index is reloaded only when its version file (written last as
    /// a "build completed" marker) matches the current version; otherwise it is
    /// rebuilt.
    pub fn plan(
        config: SparseIndexConfig,
        id_tracker: &AtomicRefCell<IdTrackerEnum>,
        vector_storage: &AtomicRefCell<VectorStorageEnum>,
        path: &Path,
        stopped: &AtomicBool,
        tick_progress: impl FnMut(),
    ) -> OperationResult<SparseOpenPlan> {
        if !config.index_type.is_persisted() {
            // RAM mutable case - build from scratch, keep the provided config, do not persist.
            fs::create_dir_all(path)?;
            let (ram_index, indices_tracker) =
                Self::build_ram_index(id_tracker, vector_storage, stopped, tick_progress)?;
            return Ok(SparseOpenPlan::Build {
                config,
                ram_index,
                indices_tracker,
                persist: false,
            });
        }

        let stored_version = TInvertedIndex::Version::load_universal(&MmapFs, path)?;
        if stored_version == Some(TInvertedIndex::Version::current()) {
            let config = SparseIndexConfig::load(&SparseIndexConfig::get_config_path(path))?;
            let indices_tracker = IndicesTracker::open(path)?;
            return Ok(SparseOpenPlan::Load {
                config,
                indices_tracker,
            });
        }

        if fs::exists(path).unwrap_or(true) {
            log::warn!(
                "Sparse index at {path:?} is missing or outdated (found {stored_version:?}, expected {}), rebuilding",
                TInvertedIndex::Version::current(),
            );
            // Drop index completely.
            fs::remove_dir_all(path)?;
        }
        fs::create_dir_all(path)?;
        let (ram_index, indices_tracker) =
            Self::build_ram_index(id_tracker, vector_storage, stopped, tick_progress)?;
        Ok(SparseOpenPlan::Build {
            config,
            ram_index,
            indices_tracker,
            persist: true,
        })
    }

    /// Build the (type-agnostic) RAM inverted index and indices tracker from the
    /// vector storage.
    fn build_ram_index(
        id_tracker: &AtomicRefCell<IdTrackerEnum>,
        vector_storage: &AtomicRefCell<VectorStorageEnum>,
        stopped: &AtomicBool,
        mut tick_progress: impl FnMut(),
    ) -> OperationResult<(InvertedIndexRam, IndicesTracker)> {
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
        Ok((ram_index_builder.build(), indices_tracker))
    }

    pub fn inverted_index(&self) -> &TInvertedIndex {
        &self.inverted_index
    }

    /// Borrow all backing storages and hand a read view to `f`.
    ///
    /// Mirrors the dense indexes: the shared search logic lives on
    /// [`SparseVectorIndexReadView`], so both this mutable index and the
    /// read-only counterpart drive the exact same code.
    pub fn with_view<R>(
        &self,
        f: impl FnOnce(SparseVectorIndexReadViewEnum<'_, TInvertedIndex>) -> R,
    ) -> R {
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let payload_index = self.payload_index.borrow();

        payload_index.with_view(|payload_index_view| {
            let read_view = SparseVectorIndexReadView {
                config: self.config,
                id_tracker: &*id_tracker,
                vector_storage: &*vector_storage,
                payload_index: payload_index_view,
                inverted_index: &self.inverted_index,
                searches_telemetry: &self.searches_telemetry,
                indices_tracker: &self.indices_tracker,
                search_scratch_pool: &self.search_scratch_pool,
            };
            f(read_view)
        })
    }

    /// Returns the maximum number of results that can be returned by the index for a given sparse vector
    /// Warning: the cost of this function grows with the number of dimensions in the query vector
    #[cfg(feature = "testing")]
    pub fn max_result_count(&self, query_vector: &SparseVector) -> OperationResult<usize> {
        use sparse::index::posting_list_common::PostingListIter as _;

        // For tests only
        let hw_counter = HardwareCounterCell::disposable();

        let mut unique_record_ids = std::collections::HashSet::new();
        let arena = blink_alloc::Blink::new();
        let ids = query_vector
            .indices
            .iter()
            .filter_map(|dim_id| Some(((), self.indices_tracker.remap_index(*dim_id)?)));
        self.inverted_index
            .get_batch(ids, &arena, &hw_counter, |(), posting_list_iter| {
                for element in posting_list_iter.into_std_iter() {
                    unique_record_ids.insert(element.record_id);
                }
                Ok(())
            })?;
        Ok(unique_record_ids.len())
    }
}
