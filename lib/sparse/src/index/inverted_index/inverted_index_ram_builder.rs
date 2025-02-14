use std::cmp::max;

use common::types::PointOffsetType;
use log::debug;

use crate::common::sparse_vector::RemappedSparseVector;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::posting_list::PostingBuilder;
use crate::index::posting_list_common::PostingElementEx;

/// Builder for InvertedIndexRam
pub struct InvertedIndexBuilder {
    pub posting_builders: Vec<PostingBuilder>,
    pub vector_count: usize,
    pub total_sparse_size: usize,
}

impl Default for InvertedIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndexBuilder {
    pub fn new() -> InvertedIndexBuilder {
        InvertedIndexBuilder {
            posting_builders: Vec::new(),
            vector_count: 0,
            total_sparse_size: 0,
        }
    }

    /// Add a vector to the inverted index builder
    pub fn add(&mut self, id: PointOffsetType, vector: RemappedSparseVector) {
        let sparse_size = vector.len() * size_of::<PostingElementEx>();
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            self.posting_builders.resize_with(
                max(dim_id + 1, self.posting_builders.len()),
                PostingBuilder::new,
            );
            self.posting_builders[dim_id].add(id, weight);
        }
        self.vector_count += 1;
        self.total_sparse_size = self.total_sparse_size.saturating_add(sparse_size);
    }

    /// Consumes the builder and returns an InvertedIndexRam
    pub fn build(self) -> InvertedIndexRam {
        let num_posting_lists = self.posting_builders.len();
        debug!(
            "building inverted index with {} posting lists",
            num_posting_lists
        );
        let mut new_vectors_count = 0;

        let mut postings = Vec::with_capacity(self.posting_builders.len());
        for posting_builder in self.posting_builders {
            let posting_list = posting_builder.build();
            new_vectors_count += posting_list.elements.len();
            postings.push(posting_list);
        }

        if new_vectors_count > 0 {
            debug!(
                "built inverted index for {} new sparse vectors and {} posting lists",
                new_vectors_count, num_posting_lists
            );
        }

        let vector_count = self.vector_count;
        let total_sparse_size = self.total_sparse_size;
        InvertedIndexRam {
            postings,
            vector_count,
            total_sparse_size,
        }
    }

    /// Creates an [InvertedIndexRam] from an iterator of (id, vector) pairs.
    pub fn build_from_iterator(
        iter: impl Iterator<Item = (PointOffsetType, RemappedSparseVector)>,
    ) -> InvertedIndexRam {
        let mut builder = InvertedIndexBuilder::new();
        for (id, vector) in iter {
            builder.add(id, vector);
        }
        builder.build()
    }
}
