use common::types::PointOffsetType;

use crate::posting_builder::PostingBuilder;
use crate::value_handler::{FixedSizeHandler, VarSizeHandler};
use crate::{CHUNK_SIZE, CompressedPostingList, FixedSizedValue, VarSizedValue};

/// V is the value we are interested to store along with the id.
/// S is the type of value we store within the chunk, should be small like an int. For
/// variable-sized values, this acts as a pointer into var_sized_data
pub struct PostingList<V, S = V> {
    pub(crate) id_data: Vec<u8>,
    pub(crate) chunks: Vec<PostingChunk<S>>,
    pub(crate) remainders: Vec<PostingElement<S>>,
    pub(crate) var_sized_data: Vec<u8>,
    pub(crate) _phantom: std::marker::PhantomData<V>,
}

#[derive(Clone)]
pub(crate) struct PostingElement<V> {
    pub(crate) id: PointOffsetType,
    pub(crate) value: V,
}

#[repr(C)]
pub struct PostingChunk<S: Sized> {
    /// Initial data point id. Used for decompression.
    pub initial: PointOffsetType,

    /// An offset within id_data
    pub offset: u32,

    /// Sized values for the chunk.
    pub sized_values: [S; CHUNK_SIZE],
}

impl<S: Sized> PostingChunk<S> {
    /// Get byte size of the compressed ids chunk.
    pub(crate) fn calculate_ids_chunk_size(
        chunks: &[PostingChunk<S>],
        data: &[u8],
        chunk_index: usize,
    ) -> usize {
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            // Last chunk
            data.len() - chunks[chunk_index].offset as usize
        }
    }
}

// Fixed-sized value implementation
// For fixed-size values, we store them directly in the PostingChunk
impl<V: FixedSizedValue + Copy + Default> CompressedPostingList<V> for PostingList<V> {
    type Fixed = V;
    type Var = ();

    fn from_builder(builder: PostingBuilder<V>) -> Self {
        builder.build_generic::<FixedSizeHandler, _>()
    }
}

// Variable-sized value implementation.
// For variable-size values, we store offsets in the PostingChunk that point to
// the actual values stored in var_sized_data.
// Here `chunk.sized_values` are pointing to the start offset of the actual values in `posting.var_sized_values`
impl<V: VarSizedValue + Clone> CompressedPostingList<V> for PostingList<V, u32> {
    type Fixed = u32;
    type Var = V;

    fn from_builder(builder: PostingBuilder<V>) -> Self {
        builder.build_generic::<VarSizeHandler, _>()
    }
}
