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
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal, VectorRef};
use crate::index::sparse_index::indices_tracker::IndicesTracker;
use crate::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use crate::index::{VectorIndex, VectorIndexRead};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::query::TransformInto;
use crate::vector_storage::{VectorStorage, VectorStorageRead};

impl<TInvertedIndex: InvertedIndex> VectorIndexRead for SparseVectorIndex<TInvertedIndex> {
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

        debug_assert_eq!(
            self.deferred_internal_id,
            query_context.deferred_internal_id(),
            "SparseIndex and VectorQueryContext deferred_internal_id consistency violated."
        );

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

    fn indexed_vector_count(&self) -> usize {
        self.inverted_index.vector_count()
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        self.inverted_index.total_sparse_vectors_size()
    }

    /// Update statistics for idf-dot similarity.
    fn fill_idf_statistics(
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
            .deferred_internal_id
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
}
