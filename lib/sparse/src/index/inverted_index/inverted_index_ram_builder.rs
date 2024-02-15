use common::types::PointOffsetType;

use crate::common::sparse_vector::SparseVector;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::posting_list::PostingBuilder;

/// Builder for InvertedIndexRam
pub struct InvertedIndexBuilder {
    pub posting_builders: Vec<PostingBuilder>,
    pub vector_count: usize,
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
        }
    }

    /// Add a vector to the inverted index builder
    pub fn add(&mut self, id: PointOffsetType, vector: SparseVector) {
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            match self.posting_builders.get_mut(dim_id) {
                Some(posting) => {
                    // update existing posting list
                    posting.add(id, weight);
                }
                None => {
                    // resize postings vector (fill gaps with empty posting lists)
                    self.posting_builders
                        .resize_with(dim_id + 1, PostingBuilder::new);
                    // initialize new posting for dimension
                    let mut new_posting_builder = PostingBuilder::new();
                    new_posting_builder.add(id, weight);
                    self.posting_builders[dim_id] = new_posting_builder
                }
            }
        }
        self.vector_count += 1;
    }

    /// Consumes the builder and returns an InvertedIndexRam
    pub fn build(self) -> InvertedIndexRam {
        let mut postings = Vec::with_capacity(self.posting_builders.len());
        for posting_builder in self.posting_builders {
            postings.push(posting_builder.build());
        }

        let vector_count = self.vector_count;
        InvertedIndexRam {
            postings,
            vector_count,
        }
    }
}
