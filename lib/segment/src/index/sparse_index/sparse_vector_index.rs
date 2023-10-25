use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::{PointOffsetType, ScoredPointOffset};
use sparse::common::sparse_vector::SparseVector;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexBuilder;
use sparse::index::inverted_index::InvertedIndex;
use sparse::index::posting_list::PostingList;
use sparse::index::search_context::SearchContext;

use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::data_types::vectors::QueryVector;
use crate::id_tracker::IdTrackerSS;
use crate::index::hnsw_index::hnsw::SearchesTelemetry;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::{PayloadIndex, VectorIndex};
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub struct SparseVectorIndex<TInvertedIndex: InvertedIndex> {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    path: PathBuf,
    inverted_index: TInvertedIndex,
    searches_telemetry: SearchesTelemetry,
}

impl<TInvertedIndex: InvertedIndex> SparseVectorIndex<TInvertedIndex> {
    /// Create new sparse vector index
    pub fn new(
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
        path: PathBuf,
    ) -> OperationResult<Self> {
        let mut builder = InvertedIndexBuilder::new();
        {
            // TODO(ivan) load from disk instead of building
            let borrowed_vector_storage = vector_storage.borrow();
            let points_count = borrowed_vector_storage.total_vector_count();
            for id in 0..points_count as PointOffsetType {
                let vector: &SparseVector = borrowed_vector_storage.get_vector(id).try_into()?;
                let posting_list = PostingList::from(vector.to_owned());
                builder.add(id, posting_list);
            }
        }
        let inverted_index = TInvertedIndex::from_builder(builder, &path)?;

        Ok(Self {
            id_tracker,
            vector_storage,
            payload_index,
            path,
            inverted_index,
            searches_telemetry: SearchesTelemetry {
                unfiltered_hnsw: OperationDurationsAggregator::new(),
                unfiltered_plain: OperationDurationsAggregator::new(),
                small_cardinality: OperationDurationsAggregator::new(),
                large_cardinality: OperationDurationsAggregator::new(),
                exact_filtered: OperationDurationsAggregator::new(),
                exact_unfiltered: OperationDurationsAggregator::new(),
            },
        })
    }

    /// Search index using sparse vector query
    pub fn search_sparse(
        &self,
        vectors: &[&QueryVector],
        top: usize,
        is_stopped: &AtomicBool,
        condition: impl Fn(PointOffsetType) -> bool,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        let mut result = vec![];

        for vector in vectors {
            check_process_stopped(is_stopped)?;
            let _timer = ScopeDurationMeasurer::new(&self.searches_telemetry.unfiltered_hnsw);

            let vector = match vector {
                QueryVector::Nearest(vector) => vector,
                QueryVector::Recommend(_) => {
                    return Err(OperationError::ValidationError {
                        description: "Cannot recommend sparse vectors".to_string(),
                    })
                }
                QueryVector::Discovery(_) => {
                    return Err(OperationError::ValidationError {
                        description: "Cannot discovery sparse vectors".to_string(),
                    })
                }
                QueryVector::Context(_) => {
                    return Err(OperationError::ValidationError {
                        description: "Cannot context query sparse vectors".to_string(),
                    })
                }
            };
            let sparse_vector: &SparseVector = vector.try_into()?;
            let mut search_context = SearchContext::new(
                sparse_vector.to_owned(),
                top,
                &self.inverted_index,
                is_stopped,
            );
            result.push(search_context.search(&condition));
        }

        Ok(result)
    }
}

impl<TInvertedIndex: InvertedIndex> VectorIndex for SparseVectorIndex<TInvertedIndex> {
    fn search(
        &self,
        vectors: &[&QueryVector],
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        // TODO(ivan) small cardinality filter
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let deleted_point_bitslice = id_tracker.deleted_point_bitslice();
        let deleted_vectors = vector_storage.deleted_vector_bitslice();
        let delete_condition = |idx: PointOffsetType| -> bool {
            deleted_point_bitslice
                .get(idx as usize)
                .map(|x| *x)
                .unwrap_or(false)
                && deleted_vectors
                    .get(idx as usize)
                    .map(|x| *x)
                    .unwrap_or(true)
        };
        match filter {
            Some(filter) => {
                let payload_index = self.payload_index.borrow();
                let filter_context = payload_index.filter_context(filter);
                let filter_condition = |idx: PointOffsetType| -> bool { filter_context.check(idx) };
                self.search_sparse(vectors, top, is_stopped, |idx| {
                    delete_condition(idx) && filter_condition(idx)
                })
            }
            None => self.search_sparse(vectors, top, is_stopped, delete_condition),
        }
    }

    fn build_index(&mut self, _stopped: &AtomicBool) -> OperationResult<()> {
        let borrowed_vector_storage = self.vector_storage.borrow();
        let points_count = borrowed_vector_storage.total_vector_count();

        let mut builder = InvertedIndexBuilder::new();
        for id in 0..points_count as PointOffsetType {
            let vector: &SparseVector = borrowed_vector_storage.get_vector(id).try_into()?;
            let posting_list = PostingList::from(vector.to_owned());
            builder.add(id, posting_list);
        }

        self.inverted_index = TInvertedIndex::from_builder(builder, &self.path)?;
        Ok(())
    }

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry {
        let tm = &self.searches_telemetry;
        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: tm.unfiltered_plain.lock().get_statistics(),
            filtered_plain: Default::default(),
            unfiltered_hnsw: tm.unfiltered_hnsw.lock().get_statistics(),
            filtered_small_cardinality: tm.small_cardinality.lock().get_statistics(),
            filtered_large_cardinality: tm.large_cardinality.lock().get_statistics(),
            filtered_exact: tm.exact_filtered.lock().get_statistics(),
            unfiltered_exact: tm.exact_unfiltered.lock().get_statistics(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn indexed_vector_count(&self) -> usize {
        self.vector_storage.borrow().total_vector_count()
    }

    fn update(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        let vector_storage = self.vector_storage.borrow();
        let vector: &SparseVector = vector_storage.get_vector(idx).try_into()?;
        self.inverted_index.upsert(idx, vector.clone());
        Ok(())
    }
}
