use std::marker::PhantomData;

use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::posting_list::{PostingChunk, PostingElement, PostingList};
use crate::value_handler::ValueHandler;
use crate::{BitPackerImpl, CHUNK_SIZE};

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
            _phantom: PhantomData,
        }
    }

    fn visitor(&self) -> PostingVisitor<V, S> {
        let view = self.view();
        PostingVisitor::new(view)
    }
}

impl<V, S> PostingListView<'_, V, S> {
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
    /// Assumes the id is in the posting list range.
    pub fn find_chunk(&self, id: PointOffsetType, start_chunk: Option<usize>) -> Option<usize> {
        let remainders = self.remainders;
        let chunks = self.chunks;

        if remainders
            .first()
            .map(|elem| id >= elem.id)
            .unwrap_or(false)
        {
            // id is in the remainders list
            return None;
        }

        if chunks.is_empty() {
            return None;
        }

        let start_chunk = start_chunk.unwrap_or(0);
        match chunks[start_chunk..].binary_search_by(|chunk| chunk.initial_id.cmp(&id)) {
            // id is the initial value of the chunk with index idx
            Ok(idx) => Some(start_chunk + idx),
            // chunk idx has larger initial value than id
            // so we need the previous chunk
            Err(idx) if idx > 0 => Some(start_chunk + idx - 1),
            Err(_) => None,
        }
    }

    fn search_in_remainders(&self, id: PointOffsetType) -> Option<usize> {
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

pub(crate) struct PostingVisitor<'a, V, S> {
    pub(crate) list: PostingListView<'a, V, S>,

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

    pub fn contains(&mut self, id: PointOffsetType) -> bool {
        if !self.list.is_in_range(id) {
            return false;
        }

        // Find the chunk that may contain the id and check if the id is in the chunk
        let chunk_index = self.list.find_chunk(id, None);
        if let Some(chunk_index) = chunk_index {
            if self.list.chunks[chunk_index].initial_id == id {
                return true;
            }

            self.decompressed_chunk(chunk_index)
                .binary_search(&id)
                .is_ok()
        } else {
            self.list.search_in_remainders(id).is_some()
        }
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

            return Some(PostingElement { id, value });
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
