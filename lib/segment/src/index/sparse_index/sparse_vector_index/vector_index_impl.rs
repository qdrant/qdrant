use std::collections::HashMap;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::storage_version::VERSION_FILE;
use common::types::{PointOffsetType, ScoredPointOffset, TelemetryDetail};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::DimId;
use sparse::index::inverted_index::InvertedIndex;

use super::SparseVectorIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::id_tracker::IdTrackerRead;
use crate::index::sparse_index::indices_tracker::IndicesTracker;
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::index::{VectorIndex, VectorIndexRead};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::sparse::StoredSparseVector;
use crate::vector_storage::{VectorStorage, VectorStorageRead};

impl<TInvertedIndex: InvertedIndex> SparseVectorIndex<TInvertedIndex> {
    /// Plain (full-scan) sparse search over a set of pre-filtered points.
    ///
    /// Thin wrapper that drives the shared [`SparseVectorIndexReadView`]; kept on
    /// the index for benches and other direct callers.
    ///
    /// [`SparseVectorIndexReadView`]: super::read_view::SparseVectorIndexReadView
    pub fn search_plain(
        &self,
        sparse_vector: &SparseVector,
        filter: &Filter,
        top: usize,
        prefiltered_points: &mut Option<Vec<PointOffsetType>>,
        vector_query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        self.with_view(|view| {
            view.search_plain(
                sparse_vector,
                filter,
                top,
                prefiltered_points,
                vector_query_context,
            )
        })
    }
}

impl<TInvertedIndex: InvertedIndex> VectorIndexRead for SparseVectorIndex<TInvertedIndex> {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        self.with_view(|view| view.search(vectors, filter, top, query_context))
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        self.with_view(|view| view.get_telemetry_data(detail))
    }

    fn indexed_vector_count(&self) -> usize {
        self.with_view(|view| view.indexed_vector_count())
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.with_view(|view| view.size_of_searchable_vectors_in_bytes())
    }

    fn fill_idf_statistics(
        &self,
        idf: &mut HashMap<DimId, usize>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.with_view(|view| view.fill_idf_statistics(idf, hw_counter))
    }

    fn is_index(&self) -> bool {
        true
    }
}

impl<TInvertedIndex: InvertedIndex> VectorIndex for SparseVectorIndex<TInvertedIndex> {
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

        let point_is_deferred = self
            .id_tracker
            .borrow()
            .deferred_internal_id()
            .is_some_and(|deferred| id >= deferred);

        if point_is_deferred {
            return Ok(());
        }

        let vector = SparseVector::try_from(new_vector)?;
        let old_vector: Option<SparseVector> =
            old_vector.map(SparseVector::try_from).transpose()?;

        // do not upsert empty or deferred vectors into the index
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

    fn update_vector_raw(
        &mut self,
        id: PointOffsetType,
        vector: Option<&[u8]>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // The raw form is the lossless `StoredSparseVector` encoding; the
        // inverted index needs the decoded values anyway, so decode and
        // delegate to the regular path.
        let sparse = vector
            .map(|bytes| SparseVector::try_from(StoredSparseVector::try_from_bytes(bytes)?))
            .transpose()?;
        self.update_vector(id, sparse.as_ref().map(VectorRef::from), hw_counter)
    }
}
