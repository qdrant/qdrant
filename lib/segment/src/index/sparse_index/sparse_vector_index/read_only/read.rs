use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{ScoredPointOffset, TelemetryDetail};
use common::universal_io::UniversalRead;
use sparse::common::types::DimId;
use sparse::index::inverted_index::InvertedIndex;

use super::ReadOnlySparseVectorIndex;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::index::VectorIndexRead;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::query::TransformInto;

impl<S: UniversalRead, TInvertedIndex: InvertedIndex> VectorIndexRead
    for ReadOnlySparseVectorIndex<S, TInvertedIndex>
{
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
        query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        self.with_view(|view| {
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

                    view.search_query(&vector, filter, top, &mut prefiltered_points, query_context)?
                } else {
                    view.search_query(vector, filter, top, &mut prefiltered_points, query_context)?
                };

                results.push(search_results);
            }
            Ok(results)
        })
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
    ) -> OperationResult<()> {
        let iter = idf.iter_mut().filter_map(|(dim_id, count)| {
            let offset = self.indices_tracker.remap_index(*dim_id)?;
            Some((count, offset))
        });
        self.inverted_index
            .posting_list_len_batch(iter, hw_counter, |count, len| {
                *count += len;
                Ok(())
            })?;
        Ok(())
    }

    fn is_index(&self) -> bool {
        true
    }
}
