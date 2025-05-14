use std::marker::PhantomData;

use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::posting_builder::PostingBuilder;
use crate::value_handler::{ValueHandler, VarSized};
use crate::{BitPackerImpl, CHUNK_SIZE, CompressedPostingList, FixedSizedValue, VarSizedValue};

/// V is the value we are interested to store along with the id.
/// S is the type of value we store within the chunk, should be small like an int. For
/// variable-sized values, this acts as a pointer into var_size_data
pub struct PostingList<V, S> {
    pub(crate) id_data: Vec<u8>,
    pub(crate) chunks: Vec<PostingChunk<S>>,
    pub(crate) remainders: Vec<PostingElement<S>>,
    pub(crate) var_size_data: Vec<u8>,
    pub(crate) last_id: Option<PointOffsetType>,
    pub(crate) _phantom: PhantomData<V>,
}

#[derive(Clone, Debug)]
pub(crate) struct PostingElement<S> {
    pub(crate) id: PointOffsetType,
    pub(crate) value: S,
}

#[derive(Debug, Clone)]
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
impl<V: FixedSizedValue + Copy + Default> CompressedPostingList<V> for PostingList<V, V> {
    fn from_builder(builder: PostingBuilder<V>) -> Self {
        builder.build_generic::<V>()
    }
}

// Variable-sized value implementation.
// For variable-size values, we store offsets in the PostingChunk that point to
// the actual values stored in var_size_data.
// Here `chunk.sized_values` are pointing to the start offset of the actual values in `posting.var_size_data`
impl<V: VarSizedValue + Clone> CompressedPostingList<V> for PostingList<V, u32> {
    fn from_builder(builder: PostingBuilder<V>) -> Self {
        builder.build_generic::<VarSized<V>>()
    }
}

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

impl<V, S> PostingList<V, S> {
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
}

impl<'a, V, S> PostingListView<'a, V, S> {
    fn decompress_chunk(
        &self,
        chunk_index: usize,
        decompressed_chunk: &mut [PointOffsetType; CHUNK_SIZE],
    ) {
        let chunk = &self.chunks[chunk_index];
        let chunk_size =
            PostingChunk::calculate_ids_chunk_size(self.chunks, self.id_data, chunk_index);
        let chunk_bits = chunk_size * u8::BITS as usize / CHUNK_SIZE;
        BitPackerImpl::new().decompress_strictly_sorted(
            chunk.initial.checked_sub(1),
            &self.id_data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            decompressed_chunk,
            chunk_bits as u8,
        );
    }

    fn sized_values(&self, chunk_idx: usize) -> &[S] {
        &self.chunks[chunk_idx].sized_values
    }
}

struct IteratorPosition {
    /// Offset within the posting list chunks
    idx: usize,
    /// Current point id
    point_id: Option<PointOffsetType>,
}

pub struct PostingListIterator<'a, V, S> {
    list: PostingListView<'a, V, S>,

    /// Determines whether the decommpressed chunk contains valid data for the position
    unpacked: bool,

    /// Lazily decompressed chunk of ids. Never access this directly, prefer [`Self::decompressed_chunk`] function
    decompressed_chunk: [PointOffsetType; CHUNK_SIZE],

    /// Offset inside the posting list along with optional current id.
    pos: IteratorPosition,
}

impl<'a, S: Copy, V: ValueHandler<V, Sized = S>> PostingListIterator<'a, V, S> {
    fn new(list: PostingListView<'a, V, S>) -> Self {
        Self {
            list,
            unpacked: false,
            decompressed_chunk: [0; CHUNK_SIZE],
            pos: IteratorPosition {
                idx: 0,
                point_id: None,
            },
        }
    }

    fn decompressed_chunk(&mut self) -> &[PointOffsetType; CHUNK_SIZE] {
        if !self.unpacked {
            self.list
                .decompress_chunk(self.pos.idx / CHUNK_SIZE, &mut self.decompressed_chunk);
            self.unpacked = true;
        }
        &self.decompressed_chunk
    }

    fn current(&mut self) -> Option<PostingElement<V>> {
        let global_idx = self.pos.idx;
        let chunk_idx = global_idx / CHUNK_SIZE;
        let local_idx = global_idx % CHUNK_SIZE;

        let current = if chunk_idx < self.list.chunks.len() {
            let id = self.decompressed_chunk()[chunk_idx];
            let sized_value = self.list.sized_values(chunk_idx)[local_idx];
            let next_sized_value = self
                .list
                .sized_values(chunk_idx)
                .get(local_idx + 1)
                .copied();
            let value = V::get_value(sized_value, next_sized_value, self.list.var_size_data);

            Some(PostingElement { id, value })
        } else {
            self.list.remainders.get(local_idx).map(|e| {
                let id = e.id;
                let next_sized_value = self.list.remainders.get(local_idx + 1).map(|r| r.value);
                let value = V::get_value(e.value, next_sized_value, self.list.var_size_data);

                PostingElement { id, value }
            })
        };

        // todo: what to do with current IteratorPosition
        // self.pos.point_id = current.map(|e| e.id);
        current
    }
}
