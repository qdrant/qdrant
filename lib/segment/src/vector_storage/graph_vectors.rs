use std::borrow::Cow;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::PathBuf;

use common::generic_consts::AccessPattern;
use common::mmap::MmapFlusher;
use common::types::{PointOffsetType, ScoreType};
use common::universal_io::UniversalRead;
use quantization::turboquant::EncodedQueryTQ;
use quantization::turboquant::quantization::TurboQuantizer;

use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::index::hnsw_index::HnswGraph;
use crate::types::Distance;
use crate::vector_storage::dense::dense_vectors::DenseVectorBlob;
use crate::vector_storage::turbo::shared::score_query_bytes;
use crate::vector_storage::turbo::turbo_vectors::TurboVectorBlob;

#[derive(Debug)]
pub struct GraphVectors<T: PrimitiveVectorElement, S: UniversalRead> {
    graph: HnswGraph<S>,
    len: usize,
    element: PhantomData<T>,
}

impl<T: PrimitiveVectorElement, S: UniversalRead> GraphVectors<T, S> {
    pub fn new(graph: HnswGraph<S>, len: usize) -> OperationResult<Self> {
        graph.check_base_vector_layout_compatibility(size_of::<T>() * len, align_of::<T>())?;
        Ok(Self {
            graph,
            len,
            element: PhantomData,
        })
    }

    pub fn graph(&self) -> &HnswGraph<S> {
        &self.graph
    }

    pub fn populate(&self) {
        if let Err(err) = self.graph.populate() {
            log::error!("Failed to populate vector storage: {err}");
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.graph.clear_cache()
    }

    fn base_vector(&self, key: PointOffsetType) -> Cow<'_, [T]> {
        self.graph
            .base_vector(key)
            .unwrap_or_else(|err| panic!("Failed to read base vector {key} from graph: {err}"))
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> DenseVectorBlob for GraphVectors<T, S> {
    type Element = T;
    type File = S;

    fn dim(&self) -> usize {
        self.len
    }

    fn num_vectors(&self) -> usize {
        self.graph.num_points()
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<Cow<'_, [T]>> {
        ((key as usize) < self.graph.num_points()).then(|| self.base_vector(key))
    }

    fn for_each_in_batch<F: FnMut(usize, &[T])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        self.graph.for_each_base_vector_in_batch(keys, f)
    }
}

impl<S: UniversalRead> TurboVectorBlob for GraphVectors<u8, S> {
    type File = S;

    fn vectors_count(&self) -> usize {
        self.graph.num_points()
    }

    fn get_vector_data(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.base_vector(key)
    }

    fn for_each_in_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        self.graph.for_each_base_vector_in_batch(keys, f)
    }

    fn populate(&self) {
        GraphVectors::populate(self);
    }

    fn clear_cache(&self) -> OperationResult<()> {
        GraphVectors::clear_cache(self)
    }

    fn get_vector_data_opt(&self, key: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        ((key as usize) < self.graph.num_points()).then(|| self.base_vector(key))
    }

    fn files(&self) -> Vec<PathBuf> {
        // Files are shared with the graph. Not reporting there, assuming the
        // graph reports them.
        Vec::new()
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }

    fn score_query_batch(
        &self,
        quantizer: &TurboQuantizer,
        distance: Distance,
        query: &EncodedQueryTQ,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) {
        debug_assert_eq!(ids.len(), scores.len());
        self.graph
            .for_each_base_vector_in_batch(ids, |idx, bytes| {
                scores[idx] = score_query_bytes(quantizer, distance, query, bytes);
            })
            .expect("score TQ vectors");
    }
}
