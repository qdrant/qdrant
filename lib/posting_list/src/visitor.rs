use std::marker::PhantomData;

use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::{
    BitPackerImpl, CHUNK_SIZE,
    posting_list::{PostingChunk, PostingElement, PostingList},
    value_handler::ValueHandler,
};

/// A non-owning view of [`PostingList`].
#[derive(Debug, Clone)]
pub struct PostingListView<'a, V, S> {
    id_data: &'a [u8],
    chunks: &'a [PostingChunk<S>],
    var_size_data: &'a [u8],
    remainders: &'a [PostingElement<S>],
    last_id: Option<PointOffsetType>,
    _phantom: PhantomData<V>,
}

impl<V: ValueHandler<V, Sized = S>, S: Copy> PostingList<V, S> {
    fn view(&self) -> PostingListView<V, S> {
        let PostingList {
            id_data,
            chunks,
            remainders,
            var_size_data,
            last_id,
            _phantom,
        } = self;

        PostingListView {
            id_data,
            chunks,
            var_size_data,
            remainders,
            last_id: *last_id,
            _phantom: *_phantom,
        }
    }

    fn visitor(&self) -> PostingVisitor<V, S> {
        let view = self.view();
        PostingVisitor::new(view)
    }
}

impl<'a, V, S> PostingListView<'a, V, S> {
    fn decompress_chunk(
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

    fn sized_values(&self, chunk_idx: usize) -> &[S] {
        &self.chunks[chunk_idx].sized_values
    }

    fn is_in_range(&self, id: PointOffsetType) -> bool {
        let Some(last_id) = self.last_id else {
            return false;
        };

        let chunks = self.chunks;
        let remainders = self.remainders;

        let in_chunks_range = !chunks.is_empty() && id >= chunks[0].initial_id && id <= last_id;

        let in_remainder_range =
            || !remainders.is_empty() && id >= remainders[0].id && id <= last_id;

        in_chunks_range || in_remainder_range()
    }
}

pub(crate) struct PostingVisitor<'a, V, S> {
    list: PostingListView<'a, V, S>,

    /// Index of the decompressed chunk.
    /// It is used to shorten the search range of chunk index for the next value.
    decompressed_chunk_idx: Option<usize>,

    /// Lazily decompressed chunk of ids. Never access this directly, prefer [`Self::decompressed_chunk`] function
    decompressed_chunk: [PointOffsetType; CHUNK_SIZE],
}

impl<'a, S: Copy, V: ValueHandler<V, Sized = S>> PostingVisitor<'a, V, S> {
    fn new(view: PostingListView<'a, V, S>) -> Self {
        Self {
            list: view,
            decompressed_chunk_idx: None,
            decompressed_chunk: [0; CHUNK_SIZE],
        }
    }

    fn decompressed_chunk(&mut self, chunk_idx: usize) -> &[PointOffsetType; CHUNK_SIZE] {
        if self.decompressed_chunk_idx != Some(chunk_idx) {
            self.list
                .decompress_chunk(chunk_idx, &mut self.decompressed_chunk);
            self.decompressed_chunk_idx = Some(chunk_idx);
        }

        &self.decompressed_chunk
    }

    pub(crate) fn get_by_offset(&mut self, offset: usize) -> Option<PostingElement<V>> {
        let chunk_idx = offset / CHUNK_SIZE;
        let local_offset = offset % CHUNK_SIZE;

        // get from chunk
        if chunk_idx < self.list.chunks.len() {
            let id = self.decompressed_chunk(chunk_idx)[local_offset];
            let sized_value = self.list.sized_values(chunk_idx)[local_offset];
            let next_sized_value = self
                .list
                .sized_values(chunk_idx)
                .get(local_offset + 1)
                .copied();
            let value = V::get_value(sized_value, next_sized_value, self.list.var_size_data);

            return Some(PostingElement { id, value })
        }

        // else, get from remainder
        self.list.remainders.get(local_offset).map(|e| {
            let id = e.id;
            let next_sized_value = self.list.remainders.get(local_offset + 1).map(|r| r.value);
            let value = V::get_value(e.value, next_sized_value, self.list.var_size_data);

            PostingElement { id, value }
        })
    }
}
