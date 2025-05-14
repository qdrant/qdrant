use std::marker::PhantomData;

use common::types::PointOffsetType;

use crate::CHUNK_SIZE;

/// V is the value we are interested to store along with the id.
/// S is the type of value we store within the chunk, should be small like an int. For
/// variable-sized values, this acts as a pointer into var_size_data
pub struct PostingList<V, S = V> {
    pub(crate) id_data: Vec<u8>,
    pub(crate) chunks: Vec<PostingChunk<S>>,
    pub(crate) remainders: Vec<PostingElement<S>>,
    pub(crate) var_size_data: Vec<u8>,
    pub(crate) last_id: Option<PointOffsetType>,
    pub(crate) _phantom: PhantomData<V>,
}

#[derive(Clone, Debug)]
pub struct PostingElement<S> {
    pub(crate) id: PointOffsetType,
    pub(crate) value: S,
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct PostingChunk<S: Sized> {
    /// Initial data point id. Used for decompression.
    pub initial_id: PointOffsetType,

    /// An offset within id_data
    pub offset: u32,

    /// Sized values for the chunk.
    pub sized_values: [S; CHUNK_SIZE],
}

impl<S: Sized> PostingChunk<S> {
    /// Get byte size of the compressed ids chunk.
    pub(crate) fn get_compressed_size(
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
