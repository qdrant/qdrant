use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{ScoredPointOffset, TelemetryDetail};
use sparse::common::types::DimId;

use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::VectorQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::index::vector_index_base::VectorIndexRead;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{Filter, SearchParams};

/// Read-only counterpart of [`super::VectorIndexEnum`].
///
/// Wraps each read-capable index type with its read-only newtype. The mutable
/// RAM sparse index is intentionally absent because it has no persisted
/// read-only representation.
#[derive(Debug)]
pub enum VectorIndexReadEnum {
    // Plain(Box<ReadOnlyPlainVectorIndex>),
    // Hnsw(Box<ReadOnlyHNSWIndex>),
    // SparseImmutableRam(Box<ReadOnlySparseVectorIndex<InvertedIndexImmutableRam>>),
    // SparseMmap(Box<ReadOnlySparseVectorIndex<InvertedIndexMmap>>),
    // SparseCompressedImmutableRamF32(
    //     Box<ReadOnlySparseVectorIndex<InvertedIndexCompressedImmutableRam<f32>>>,
    // ),
    // SparseCompressedImmutableRamF16(
    //     Box<ReadOnlySparseVectorIndex<InvertedIndexCompressedImmutableRam<f16>>>,
    // ),
    // SparseCompressedImmutableRamU8(
    //     Box<ReadOnlySparseVectorIndex<InvertedIndexCompressedImmutableRam<QuantizedU8>>>,
    // ),
    // SparseCompressedMmapF32(Box<ReadOnlySparseVectorIndex<InvertedIndexCompressedMmap<f32>>>),
    // SparseCompressedMmapF16(Box<ReadOnlySparseVectorIndex<InvertedIndexCompressedMmap<f16>>>),
    // SparseCompressedMmapU8(
    //     Box<ReadOnlySparseVectorIndex<InvertedIndexCompressedMmap<QuantizedU8>>>,
    // ),
}

impl VectorIndexReadEnum {
    /// Returns true if underlying index files are configured to stay on disk.
    pub fn is_on_disk(&self) -> bool {
        // match self {
        //     Self::Plain(_) => false,
        //     Self::Hnsw(index) => index.is_on_disk(),
        //     Self::SparseImmutableRam(index) => index.is_on_disk(),
        //     Self::SparseMmap(index) => index.is_on_disk(),
        //     Self::SparseCompressedImmutableRamF32(index) => index.is_on_disk(),
        //     Self::SparseCompressedImmutableRamF16(index) => index.is_on_disk(),
        //     Self::SparseCompressedImmutableRamU8(index) => index.is_on_disk(),
        //     Self::SparseCompressedMmapF32(index) => index.is_on_disk(),
        //     Self::SparseCompressedMmapF16(index) => index.is_on_disk(),
        //     Self::SparseCompressedMmapU8(index) => index.is_on_disk(),
        // }

        todo!()
    }

    pub fn populate(&self) -> OperationResult<()> {
        // match self {
        //     Self::Plain(_) => {}
        //     Self::Hnsw(index) => index.populate()?,
        //     Self::SparseImmutableRam(_) => {}
        //     Self::SparseMmap(index) => index.inner().inverted_index().populate()?,
        //     Self::SparseCompressedImmutableRamF32(_) => {}
        //     Self::SparseCompressedImmutableRamF16(_) => {}
        //     Self::SparseCompressedImmutableRamU8(_) => {}
        //     Self::SparseCompressedMmapF32(index) => index.inner().inverted_index().populate()?,
        //     Self::SparseCompressedMmapF16(index) => index.inner().inverted_index().populate()?,
        //     Self::SparseCompressedMmapU8(index) => index.inner().inverted_index().populate()?,
        // };
        // Ok(())

        todo!()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        // match self {
        //     Self::Plain(_) => {}
        //     Self::Hnsw(index) => index.clear_cache()?,
        //     Self::SparseImmutableRam(_) => {}
        //     Self::SparseMmap(index) => index.inner().inverted_index().clear_cache()?,
        //     Self::SparseCompressedImmutableRamF32(_) => {}
        //     Self::SparseCompressedImmutableRamF16(_) => {}
        //     Self::SparseCompressedImmutableRamU8(_) => {}
        //     Self::SparseCompressedMmapF32(index) => index.inner().inverted_index().clear_cache()?,
        //     Self::SparseCompressedMmapF16(index) => index.inner().inverted_index().clear_cache()?,
        //     Self::SparseCompressedMmapU8(index) => index.inner().inverted_index().clear_cache()?,
        // };
        // Ok(())

        todo!()
    }
}

impl VectorIndexRead for VectorIndexReadEnum {
    fn search(
        &self,
        _vectors: &[&QueryVector],
        _filter: Option<&Filter>,
        _top: usize,
        _params: Option<&SearchParams>,
        _query_context: &VectorQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        // match self {
        //     Self::Plain(index) => index.search(vectors, filter, top, params, query_context),
        //     Self::Hnsw(index) => index.search(vectors, filter, top, params, query_context),
        //     Self::SparseImmutableRam(index) => {
        //         index.search(vectors, filter, top, params, query_context)
        //     }
        //     Self::SparseMmap(index) => index.search(vectors, filter, top, params, query_context),
        //     Self::SparseCompressedImmutableRamF32(index) => {
        //         index.search(vectors, filter, top, params, query_context)
        //     }
        //     Self::SparseCompressedImmutableRamF16(index) => {
        //         index.search(vectors, filter, top, params, query_context)
        //     }
        //     Self::SparseCompressedImmutableRamU8(index) => {
        //         index.search(vectors, filter, top, params, query_context)
        //     }
        //     Self::SparseCompressedMmapF32(index) => {
        //         index.search(vectors, filter, top, params, query_context)
        //     }
        //     Self::SparseCompressedMmapF16(index) => {
        //         index.search(vectors, filter, top, params, query_context)
        //     }
        //     Self::SparseCompressedMmapU8(index) => {
        //         index.search(vectors, filter, top, params, query_context)
        //     }
        // }

        todo!()
    }

    fn get_telemetry_data(&self, _detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        // match self {
        //     Self::Plain(index) => index.get_telemetry_data(detail),
        //     Self::Hnsw(index) => index.get_telemetry_data(detail),
        //     Self::SparseImmutableRam(index) => index.get_telemetry_data(detail),
        //     Self::SparseMmap(index) => index.get_telemetry_data(detail),
        //     Self::SparseCompressedImmutableRamF32(index) => index.get_telemetry_data(detail),
        //     Self::SparseCompressedImmutableRamF16(index) => index.get_telemetry_data(detail),
        //     Self::SparseCompressedImmutableRamU8(index) => index.get_telemetry_data(detail),
        //     Self::SparseCompressedMmapF32(index) => index.get_telemetry_data(detail),
        //     Self::SparseCompressedMmapF16(index) => index.get_telemetry_data(detail),
        //     Self::SparseCompressedMmapU8(index) => index.get_telemetry_data(detail),
        // }

        todo!()
    }

    fn indexed_vector_count(&self) -> usize {
        // match self {
        //     Self::Plain(index) => index.indexed_vector_count(),
        //     Self::Hnsw(index) => index.indexed_vector_count(),
        //     Self::SparseImmutableRam(index) => index.indexed_vector_count(),
        //     Self::SparseMmap(index) => index.indexed_vector_count(),
        //     Self::SparseCompressedImmutableRamF32(index) => index.indexed_vector_count(),
        //     Self::SparseCompressedImmutableRamF16(index) => index.indexed_vector_count(),
        //     Self::SparseCompressedImmutableRamU8(index) => index.indexed_vector_count(),
        //     Self::SparseCompressedMmapF32(index) => index.indexed_vector_count(),
        //     Self::SparseCompressedMmapF16(index) => index.indexed_vector_count(),
        //     Self::SparseCompressedMmapU8(index) => index.indexed_vector_count(),
        // }

        todo!()
    }

    fn size_of_searchable_vectors_in_bytes(&self) -> usize {
        // match self {
        //     Self::Plain(index) => index.size_of_searchable_vectors_in_bytes(),
        //     Self::Hnsw(index) => index.size_of_searchable_vectors_in_bytes(),
        //     Self::SparseImmutableRam(index) => index.size_of_searchable_vectors_in_bytes(),
        //     Self::SparseMmap(index) => index.size_of_searchable_vectors_in_bytes(),
        //     Self::SparseCompressedImmutableRamF32(index) => {
        //         index.size_of_searchable_vectors_in_bytes()
        //     }
        //     Self::SparseCompressedImmutableRamF16(index) => {
        //         index.size_of_searchable_vectors_in_bytes()
        //     }
        //     Self::SparseCompressedImmutableRamU8(index) => {
        //         index.size_of_searchable_vectors_in_bytes()
        //     }
        //     Self::SparseCompressedMmapF32(index) => index.size_of_searchable_vectors_in_bytes(),
        //     Self::SparseCompressedMmapF16(index) => index.size_of_searchable_vectors_in_bytes(),
        //     Self::SparseCompressedMmapU8(index) => index.size_of_searchable_vectors_in_bytes(),
        // }

        todo!()
    }

    fn fill_idf_statistics(
        &self,
        _idf: &mut HashMap<DimId, usize>,
        _hw_counter: &HardwareCounterCell,
    ) {
        // match self {
        //     Self::Plain(index) => index.fill_idf_statistics(idf, hw_counter),
        //     Self::Hnsw(index) => index.fill_idf_statistics(idf, hw_counter),
        //     Self::SparseImmutableRam(index) => index.fill_idf_statistics(idf, hw_counter),
        //     Self::SparseMmap(index) => index.fill_idf_statistics(idf, hw_counter),
        //     Self::SparseCompressedImmutableRamF32(index) => {
        //         index.fill_idf_statistics(idf, hw_counter)
        //     }
        //     Self::SparseCompressedImmutableRamF16(index) => {
        //         index.fill_idf_statistics(idf, hw_counter)
        //     }
        //     Self::SparseCompressedImmutableRamU8(index) => {
        //         index.fill_idf_statistics(idf, hw_counter)
        //     }
        //     Self::SparseCompressedMmapF32(index) => index.fill_idf_statistics(idf, hw_counter),
        //     Self::SparseCompressedMmapF16(index) => index.fill_idf_statistics(idf, hw_counter),
        //     Self::SparseCompressedMmapU8(index) => index.fill_idf_statistics(idf, hw_counter),
        // }

        todo!()
    }

    fn is_index(&self) -> bool {
        // match self {
        //     Self::Plain(index) => index.is_index(),
        //     Self::Hnsw(index) => index.is_index(),
        //     Self::SparseImmutableRam(index) => index.is_index(),
        //     Self::SparseMmap(index) => index.is_index(),
        //     Self::SparseCompressedImmutableRamF32(index) => index.is_index(),
        //     Self::SparseCompressedImmutableRamF16(index) => index.is_index(),
        //     Self::SparseCompressedImmutableRamU8(index) => index.is_index(),
        //     Self::SparseCompressedMmapF32(index) => index.is_index(),
        //     Self::SparseCompressedMmapF16(index) => index.is_index(),
        //     Self::SparseCompressedMmapU8(index) => index.is_index(),
        // }

        todo!()
    }
}
