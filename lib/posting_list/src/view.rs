use std::marker::PhantomData;

use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::value_handler::ValueHandler;
use crate::visitor::PostingVisitor;
use crate::{BitPackerImpl, CHUNK_SIZE, PostingChunk, PostingElement};

/// A non-owning view of [`PostingList`].
#[derive(Debug, Clone)]
pub struct PostingListView<'a, H: ValueHandler> {
    pub(crate) id_data: &'a [u8],
    pub(crate) chunks: &'a [PostingChunk<H::Sized>],
    pub(crate) var_size_data: &'a [u8],
    pub(crate) remainders: &'a [PostingElement<H::Sized>],
    pub(crate) last_id: Option<PointOffsetType>,
    pub(crate) _phantom: PhantomData<H>,
}

impl<'a, H: ValueHandler> PostingListView<'a, H> {
    pub fn visitor(self) -> PostingVisitor<'a, H> {
        PostingVisitor::new(self)
    }

    pub fn components(
        &self,
    ) -> (
        &[u8],
        &[PostingChunk<H::Sized>],
        &[u8],
        &[PostingElement<H::Sized>],
        Option<PointOffsetType>,
    ) {
        let Self {
            id_data,
            chunks,
            var_size_data,
            remainders,
            last_id,
            _phantom,
        } = self;

        (id_data, chunks, var_size_data, remainders, *last_id)
    }

    pub fn from_components(
        id_data: &'a [u8],
        chunks: &'a [PostingChunk<H::Sized>],
        var_size_data: &'a [u8],
        remainders: &'a [PostingElement<H::Sized>],
        last_id: Option<PointOffsetType>,
    ) -> Self {
        Self {
            id_data,
            chunks,
            var_size_data,
            remainders,
            last_id,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn decompress_chunk(
        &self,
        chunk_index: usize,
        decompressed_chunk: &mut [PointOffsetType; CHUNK_SIZE],
    ) {
        let chunk = &self.chunks[chunk_index];
        let compressed_size =
            PostingChunk::get_compressed_size(self.chunks, self.id_data, chunk_index);
        let chunk_bits = compressed_size * u8::BITS as usize / CHUNK_SIZE;
        BitPackerImpl::new().decompress_strictly_sorted(
            chunk.initial_id.checked_sub(1),
            &self.id_data[chunk.offset as usize..chunk.offset as usize + compressed_size],
            decompressed_chunk,
            chunk_bits as u8,
        );
    }
    pub(crate) fn sized_values_unchecked(&self, chunk_idx: usize) -> &[H::Sized] {
        &self.chunks[chunk_idx].sized_values
    }

    pub(crate) fn sized_values(&self, chunk_idx: usize) -> Option<&[H::Sized; CHUNK_SIZE]> {
        self.chunks.get(chunk_idx).map(|chunk| &chunk.sized_values)
    }

    pub(crate) fn is_in_range(&self, id: PointOffsetType) -> bool {
        let Some(last_id) = self.last_id else {
            return false;
        };

        let Some(initial_id) = self
            .chunks
            .first()
            .map(|chunk| chunk.initial_id)
            .or_else(|| self.remainders.first().map(|elem| elem.id))
        else {
            return false;
        };

        id >= initial_id && id <= last_id
    }

    /// Find the chunk that may contain the id.
    /// It doesn't guarantee that the chunk contains the id, but if it is in the posting list, then it must be in the chunk.
    ///
    /// Assumes the id is in the posting list range.
    pub fn find_chunk(&self, id: PointOffsetType, start_chunk: Option<usize>) -> Option<usize> {
        let remainders = self.remainders;
        let chunks = self.chunks;

        // check if id is in the remainders list
        if remainders.first().is_some_and(|elem| id >= elem.id) {
            return None;
        }

        if chunks.is_empty() {
            return None;
        }

        let start_chunk = start_chunk.unwrap_or(0);
        let chunks_slice = &chunks[start_chunk..];
        if chunks_slice.is_empty() {
            return None;
        }

        // No need to check if id is under range of posting list,
        // this function assumes it is within the range
        debug_assert!(id >= chunks_slice[0].initial_id);

        match chunks_slice.binary_search_by(|chunk| chunk.initial_id.cmp(&id)) {
            // id is the initial value of the chunk with index idx
            Ok(idx) => Some(start_chunk + idx),

            // id is not the initial_id of any chunk
            Err(insert_idx) if insert_idx > 0 => {
                // this is the index of the chunk that could contain id
                let idx = insert_idx - 1;

                // id could be within this chunk
                Some(start_chunk + idx)
            }
            Err(_) => None,
        }
    }

    pub(crate) fn search_in_remainders(&self, id: PointOffsetType) -> Option<usize> {
        self.remainders
            .binary_search_by(|elem| elem.id.cmp(&id))
            .ok()
    }

    /// The total number of elements in the posting list.
    pub fn len(&self) -> usize {
        self.chunks.len() * CHUNK_SIZE + self.remainders.len()
    }

    /// Checks if there are no elements in the posting list.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
