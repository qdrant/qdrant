use std::mem;

use common::types::PointOffsetType;

use crate::common::sparse_vector::SparseVector;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::posting_list::{PostingElement, PostingList};

/// Builder for InvertedIndexRam
pub struct InvertedIndexBuilder {
    /// The PostingList are not sorted by record id until 'build' is called
    pub postings: Vec<PostingList>,
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
            postings: Vec::new(),
            vector_count: 0,
        }
    }

    /// Add a vector to the inverted index builder
    pub fn add(&mut self, id: PointOffsetType, vector: SparseVector) -> &mut Self {
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            match self.postings.get_mut(dim_id) {
                Some(posting) => {
                    // update existing posting list
                    let posting_element = PostingElement::new(id, weight);
                    posting.append(posting_element);
                }
                None => {
                    // resize postings vector (fill gaps with empty posting lists)
                    self.postings.resize_with(dim_id + 1, PostingList::default);
                    // initialize new posting for dimension
                    self.postings[dim_id] = PostingList::new_one(id, weight);
                }
            }
        }
        self.vector_count += 1;
        self
    }

    /// Build the inverted index
    pub fn build(&mut self) -> InvertedIndexRam {
        for posting in &mut self.postings {
            // Sort posting list by record id
            posting.elements.sort_by_key(|e| e.record_id);
            // Compute the max next weights for each element
            posting.compute_max_next_weights();
        }

        // Take ownership of the postings
        let postings = mem::take(&mut self.postings);
        let vector_count = self.vector_count;
        InvertedIndexRam {
            postings,
            vector_count,
        }
    }
}
