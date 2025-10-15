use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use fs_err as fs;
use io::storage_version::{StorageVersion as _, VERSION_FILE};
use itertools::Itertools;
use semver::Version;
use sparse::common::scores_memory_pool::ScoresMemoryPool;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::DimId;
use sparse::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
use sparse::index::inverted_index::{INDEX_FILE_NAME, InvertedIndex, OLD_INDEX_FILE_NAME};
use sparse::index::search_context::SearchContext;

use super::indices_tracker::IndicesTracker;
use super::sparse_index_config::SparseIndexType;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::common::operation_time_statistics::ScopeDurationMeasurer;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal, VectorRef};
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::CardinalityEstimation;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::query_estimator::adjust_to_available_vectors;
use crate::index::sparse_index::sparse_index_config::SparseIndexConfig;
use crate::index::sparse_index::sparse_search_telemetry::SparseSearchesTelemetry;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndex};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{DEFAULT_SPARSE_FULL_SCAN_THRESHOLD, Filter, SearchParams};
use crate::vector_storage::query::TransformInto;
use crate::vector_storage::{Random, VectorStorage, VectorStorageEnum, check_deleted_condition};

/// Whether to use the new compressed format.
pub const USE_COMPRESSED: bool = true;

#[derive(Debug)]
pub struct SparseVectorIndex<TInvertedIndex: InvertedIndex> {
    config: SparseIndexConfig,
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    path: PathBuf,
    inverted_index: TInvertedIndex,
    searches_telemetry: SparseSearchesTelemetry,
    indices_tracker: IndicesTracker,
    scores_memory_pool: ScoresMemoryPool,
}

/// Getters for internals, used for testing.
#[cfg(feature = "testing")]
impl<TInvertedIndex: InvertedIndex> SparseVectorIndex<TInvertedIndex> {
    pub fn config(&self) -> SparseIndexConfig {
        self.config
    }

    pub fn id_tracker(&self) -> &Arc<AtomicRefCell<IdTrackerSS>> {
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
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub path: &'a Path,
    pub stopped: &'a AtomicBool,
    pub tick_progress: F,
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
        id_tracker: &AtomicRefCell<IdTrackerSS>,
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
        for id in borrowed_id_tracker.iter_ids_excluding(deleted_bitslice) {
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
        for dim_id in query_vector.indices.iter() {
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

    fn get_query_cardinality(
        &self,
        filter: &Filter,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let vector_storage = self.vector_storage.borrow();
        let id_tracker = self.id_tracker.borrow();
        let payload_index = self.payload_index.borrow();
        let available_vector_count = vector_storage.available_vector_count();
        let query_point_cardinality = payload_index.estimate_cardinality(filter, hw_counter);
        adjust_to_available_vectors(
            query_point_cardinality,
            available_vector_count,
            id_tracker.available_point_count(),
        )
    }

    // Search using raw scorer
    fn search_scored(
        &self,
        query_vector: &QueryVector,
        filter: Option<&Filter>,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let vector_storage = self.vector_storage.borrow();
        let id_tracker = self.id_tracker.borrow();
        let deleted_point_bitslice = vector_query_context
            .deleted_points()
            .unwrap_or(id_tracker.deleted_point_bitslice());

        let is_stopped = vector_query_context.is_stopped();

        let scorer = FilteredScorer::new(
            query_vector.clone(),
            &vector_storage,
            None,
            None,
            deleted_point_bitslice,
            vector_query_context.hardware_counter(),
        )?;
        let hw_counter = vector_query_context.hardware_counter();
        match filter {
            Some(filter) => {
                let payload_index = self.payload_index.borrow();
                let mut filtered_points = match prefiltered_points {
                    Some(filtered_points) => filtered_points.iter().copied(),
                    None => {
                        let filtered_points = payload_index.query_points(filter, &hw_counter);
                        *prefiltered_points = Some(filtered_points);
                        prefiltered_points.as_ref().unwrap().iter().copied()
                    }
                };
                let res = scorer.peek_top_iter(&mut filtered_points, top, &is_stopped)?;
                Ok(res)
            }
            None => {
                let res = scorer.peek_top_all(top, &is_stopped)?;
                Ok(res)
            }
        }
    }

    pub fn search_plain(
        &self,
        sparse_vector: &SparseVector,
        filter: &Filter,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let vector_storage = self.vector_storage.borrow();
        let id_tracker = self.id_tracker.borrow();
        let payload_index = self.payload_index.borrow();

        let is_stopped = vector_query_context.is_stopped();

        let deleted_point_bitslice = vector_query_context
            .deleted_points()
            .unwrap_or(id_tracker.deleted_point_bitslice());
        let deleted_vectors = vector_storage.deleted_vector_bitslice();

        let hw_counter = vector_query_context.hardware_counter();

        let ids = match prefiltered_points {
            Some(filtered_points) => filtered_points.iter(),
            None => {
                let filtered_points = payload_index.query_points(filter, &hw_counter);
                *prefiltered_points = Some(filtered_points);
                prefiltered_points.as_ref().unwrap().iter()
            }
        }
        .copied()
        .filter(|&idx| check_deleted_condition(idx, deleted_vectors, deleted_point_bitslice))
        .collect_vec();

        let sparse_vector = self.indices_tracker.remap_vector(sparse_vector.clone());
        let memory_handle = self.scores_memory_pool.get();
        let mut hw_counter = vector_query_context.hardware_counter();
        let is_index_on_disk = self.config.index_type.is_on_disk();
        if is_index_on_disk {
            hw_counter.set_vector_io_read_multiplier(1);
        } else {
            hw_counter.set_vector_io_read_multiplier(0);
        }

        let mut search_context = SearchContext::new(
            sparse_vector,
            top,
            &self.inverted_index,
            memory_handle,
            &is_stopped,
            &hw_counter,
        );
        let search_result = search_context.plain_search(&ids);
        Ok(search_result)
    }

    // search using sparse vector inverted index
    fn search_sparse(
        &self,
        sparse_vector: &SparseVector,
        filter: Option<&Filter>,
        top: usize,
        vector_query_context: &VectorQueryContext,
    ) -> Vec<ScoredPointOffset> {
        let vector_storage = self.vector_storage.borrow();
        let id_tracker = self.id_tracker.borrow();
        let deleted_point_bitslice = vector_query_context
            .deleted_points()
            .unwrap_or(id_tracker.deleted_point_bitslice());
        let deleted_vectors = vector_storage.deleted_vector_bitslice();

        let not_deleted_condition = |idx: PointOffsetType| -> bool {
            check_deleted_condition(idx, deleted_vectors, deleted_point_bitslice)
        };

        let is_stopped = vector_query_context.is_stopped();

        let sparse_vector = self.indices_tracker.remap_vector(sparse_vector.clone());
        let memory_handle = self.scores_memory_pool.get();
        let mut hw_counter = vector_query_context.hardware_counter();
        let is_index_on_disk = self.config.index_type.is_on_disk();
        if is_index_on_disk {
            hw_counter.set_vector_io_read_multiplier(1);
        } else {
            hw_counter.set_vector_io_read_multiplier(0);
        }

        let mut search_context = SearchContext::new(
            sparse_vector,
            top,
            &self.inverted_index,
            memory_handle,
            &is_stopped,
            &hw_counter,
        );

        match filter {
            Some(filter) => {
                let payload_index = self.payload_index.borrow();
                let filter_context = payload_index.filter_context(filter, &hw_counter);
                let matches_filter_condition = |idx: PointOffsetType| -> bool {
                    not_deleted_condition(idx) && filter_context.check(idx)
                };
                search_context.search(&matches_filter_condition)
            }
            None => search_context.search(&not_deleted_condition),
        }
    }

    fn search_nearest_query(
        &self,
        vector: &SparseVector,
        filter: Option<&Filter>,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        if vector.is_empty() {
            return Ok(vec![]);
        }

        match filter {
            Some(filter) => {
                // if cardinality is small - use plain search
                let query_cardinality =
                    self.get_query_cardinality(filter, &vector_query_context.hardware_counter());
                let threshold = self
                    .config
                    .full_scan_threshold
                    .unwrap_or(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD);
                if query_cardinality.max < threshold {
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.small_cardinality);
                    self.search_plain(
                        vector,
                        filter,
                        top,
                        prefiltered_points,
                        vector_query_context,
                    )
                } else {
                    let _timer =
                        ScopeDurationMeasurer::new(&self.searches_telemetry.filtered_sparse);
                    Ok(self.search_sparse(vector, Some(filter), top, vector_query_context))
                }
            }
            None => {
                let _timer = ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_sparse);
                Ok(self.search_sparse(vector, filter, top, vector_query_context))
            }
        }
    }

    pub fn search_query(
        &self,
        query_vector: &QueryVector,
        filter: Option<&Filter>,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        if top == 0 {
            return Ok(vec![]);
        }

        match query_vector {
            QueryVector::Nearest(vector) => self.search_nearest_query(
                vector.try_into()?,
                filter,
                top,
                prefiltered_points,
                vector_query_context,
            ),
            QueryVector::RecommendBestScore(_)
            | QueryVector::RecommendSumScores(_)
            | QueryVector::Discovery(_)
            | QueryVector::Context(_)
            | QueryVector::FeedbackSimple(_) => {
                let _timer = if filter.is_some() {
                    ScopeDurationMeasurer::new(&self.searches_telemetry.filtered_plain)
                } else {
                    ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_plain)
                };
                self.search_scored(
                    query_vector,
                    filter,
                    top,
                    prefiltered_points,
                    vector_query_context,
                )
            }
        }
    }

    // Update statistics for idf-dot similarity
    pub fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        hw_counter: &HardwareCounterCell,
    ) {
        for (dim_id, count) in idf.iter_mut() {
            if let Some(remapped_dim_id) = self.indices_tracker.remap_index(*dim_id)
                && let Some(posting_list_len) = self
                    .inverted_index
                    .posting_list_len(&remapped_dim_id, hw_counter)
            {
                *count += posting_list_len
            }
        }
    }
}

impl<TInvertedIndex: InvertedIndex> VectorIndex for SparseVectorIndex<TInvertedIndex> {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let mut results = Vec::with_capacity(vectors.len());
        let mut prefiltered_points = None;
        for vector in vectors {
            check_process_stopped(&query_context.is_stopped())?;

            let search_results = if query_context.is_require_idf() {
                let vector = (*vector).clone().transform(|mut vector| {
                    match &mut vector {
                        VectorInternal::Dense(_) | VectorInternal::MultiDense(_) => {
                            return Err(OperationError::WrongSparse);
                        }
                        VectorInternal::Sparse(sparse) => {
                            query_context.remap_idf_weights(&sparse.indices, &mut sparse.values)
                        }
                    }

                    Ok(vector)
                })?;

                self.search_query(&vector, filter, top, &mut prefiltered_points, query_context)?
            } else {
                self.search_query(vector, filter, top, &mut prefiltered_points, query_context)?
            };

            results.push(search_results);
        }
        Ok(results)
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        self.searches_telemetry.get_telemetry_data(detail)
    }

    fn files(&self) -> Vec<PathBuf> {
        let config_file = SparseIndexConfig::get_config_path(&self.path);
        if !config_file.exists() {
            return vec![];
        }

        let mut all_files = vec![
            IndicesTracker::file_path(&self.path),
            self.path.join(VERSION_FILE),
        ];
        all_files.retain(|f| f.exists());

        all_files.push(config_file);
        all_files.extend_from_slice(&TInvertedIndex::files(&self.path));
        all_files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let config_file = SparseIndexConfig::get_config_path(&self.path);
        if !config_file.exists() {
            return vec![];
        }

        let mut immutable_files = vec![
            self.path.join(VERSION_FILE), // TODO: Is version file immutable?
        ];
        immutable_files.retain(|f| f.exists());

        immutable_files.push(config_file);
        immutable_files.extend_from_slice(&TInvertedIndex::immutable_files(&self.path));
        immutable_files
    }

    fn indexed_vector_count(&self) -> usize {
        self.inverted_index.vector_count()
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.inverted_index.total_sparse_vectors_size()
    }

    fn update_vector(
        &mut self,
        id: PointOffsetType,
        vector: Option<VectorRef>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let (old_vector, new_vector) = {
            let mut vector_storage = self.vector_storage.borrow_mut();
            let old_vector = vector_storage
                .get_vector_opt::<Random>(id)
                .map(CowVector::to_owned);
            let new_vector = if let Some(vector) = vector {
                vector_storage.insert_vector(id, vector, hw_counter)?;
                vector.to_owned()
            } else {
                let default_vector = vector_storage.default_vector();
                if id as usize >= vector_storage.total_vector_count() {
                    // Vector doesn't exist in the storage
                    // Insert default vector to keep the sequence
                    vector_storage.insert_vector(
                        id,
                        VectorRef::from(&default_vector),
                        hw_counter,
                    )?;
                }
                vector_storage.delete_vector(id)?;
                default_vector
            };
            (old_vector, new_vector)
        };

        if self.config.index_type != SparseIndexType::MutableRam {
            return Err(OperationError::service_error(
                "Cannot update vector in non-appendable index",
            ));
        }

        let vector = SparseVector::try_from(new_vector)?;
        let old_vector: Option<SparseVector> =
            old_vector.map(SparseVector::try_from).transpose()?;

        // do not upsert empty vectors into the index
        if !vector.is_empty() {
            self.indices_tracker.register_indices(&vector);
            let vector = self.indices_tracker.remap_vector(vector);
            let old_vector = old_vector.map(|v| self.indices_tracker.remap_vector(v));
            self.inverted_index.upsert(id, vector, old_vector);
        } else if let Some(old_vector) = old_vector {
            // Make sure empty vectors do not interfere with the index
            if !old_vector.is_empty() {
                let old_vector = self.indices_tracker.remap_vector(old_vector);
                self.inverted_index.remove(id, old_vector);
            }
        }
        Ok(())
    }
}
