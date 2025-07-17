use std::marker::PhantomData;
use std::ops::RangeInclusive;

use bitpacking::BitPacker;
use common::types::PointOffsetType;
use zerocopy::little_endian::U32;

use crate::iterator::PostingIterator;
use crate::posting_list::RemainderPosting;
use crate::value_handler::PostingValue;
use crate::visitor::PostingVisitor;
use crate::{BitPackerImpl, CHUNK_LEN, PostingChunk, PostingList, SizedTypeFor};

/// A non-owning view of [`PostingList`].
pub struct PostingListView<'a, V: PostingValue> {
    pub(crate) id_data: &'a [u8],
    chunks: &'a [PostingChunk<SizedTypeFor<V>>],
    pub(crate) var_size_data: &'a [u8],
    remainders: &'a [RemainderPosting<SizedTypeFor<V>>],
    pub(crate) last_id: Option<PointOffsetType>,
    pub(crate) _phantom: PhantomData<V>,
}

pub struct PostingListComponents<'a, S> {
    pub id_data: &'a [u8],
    pub chunks: &'a [PostingChunk<S>],
    pub var_size_data: &'a [u8],
    pub remainders: &'a [RemainderPosting<S>],
    pub last_id: Option<U32>,
}

impl<'a, V: PostingValue> IntoIterator for PostingListView<'a, V> {
    type Item = <PostingIterator<'a, V> as Iterator>::Item;
    type IntoIter = PostingIterator<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.visitor().into_iter()
    }
}

impl<'a, V: PostingValue> PostingListView<'a, V> {
    pub fn visitor(self) -> PostingVisitor<'a, V> {
        PostingVisitor::new(self)
    }

    // not implemented as ToOwned trait because it requires PostingList's Borrow to return
    // a &PostingListView, which is not possible because it's a non-owning view
    pub fn to_owned(self) -> PostingList<V> {
        PostingList {
            id_data: self.id_data.to_vec(),
            chunks: self.chunks.to_vec(),
            var_size_data: self.var_size_data.to_owned(),
            remainders: self.remainders.to_vec(),
            last_id: self.last_id,
            _phantom: PhantomData,
        }
    }

    pub fn components(&self) -> PostingListComponents<SizedTypeFor<V>> {
        let Self {
            id_data,
            chunks,
            var_size_data,
            remainders,
            last_id,
            _phantom,
        } = self;

        PostingListComponents {
            id_data,
            chunks,
            var_size_data,
            remainders,
            last_id: last_id.map(U32::from),
        }
    }

    pub fn from_components(
        id_data: &'a [u8],
        chunks: &'a [PostingChunk<SizedTypeFor<V>>],
        var_size_data: &'a [u8],
        remainders: &'a [RemainderPosting<SizedTypeFor<V>>],
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
        decompressed_chunk: &mut [PointOffsetType; CHUNK_LEN],
    ) {
        let chunk = &self.chunks[chunk_index];
        let compressed_size =
            PostingChunk::get_compressed_size(self.chunks, self.id_data, chunk_index);
        let chunk_bits = compressed_size * u8::BITS as usize / CHUNK_LEN;

        let start_offset = chunk.offset.get() as usize;
        let end_offset = start_offset + compressed_size;

        BitPackerImpl::new().decompress_sorted(
            chunk.initial_id.get(),
            &self.id_data[start_offset..end_offset],
            decompressed_chunk,
            chunk_bits as u8,
        );
    }

    pub(crate) fn get_chunk_unchecked(&self, chunk_idx: usize) -> &PostingChunk<SizedTypeFor<V>> {
        &self.chunks[chunk_idx]
    }

    pub(crate) fn get_chunk(&self, chunk_idx: usize) -> Option<&PostingChunk<SizedTypeFor<V>>> {
        self.chunks.get(chunk_idx)
    }

    pub(crate) fn chunks_len(&self) -> usize {
        self.chunks.len()
    }

    pub(crate) fn remainders_len(&self) -> usize {
        self.remainders.len()
    }

    pub(crate) fn get_remainder(&self, idx: usize) -> Option<&RemainderPosting<SizedTypeFor<V>>> {
        self.remainders.get(idx)
    }

    pub(crate) fn ids_range(&self, start_chunk: usize) -> Option<RangeInclusive<u32>> {
        // if there is no last id, it means the posting list is empty
        let last_id = self.last_id?;

        let initial_id = self
            .chunks
            .get(start_chunk)
            .map(|chunk| chunk.initial_id.get())
            .or_else(|| self.get_remainder(0).map(|elem| elem.id.get()))?;

        Some(initial_id..=last_id)
    }

    /// Find the chunk that may contain the id.
    /// It doesn't guarantee that the chunk contains the id, but if it is in the posting list, then it must be in the chunk.
    ///
    /// Assumes the id is in the posting list range.
    pub fn find_chunk(&self, id: PointOffsetType, start_chunk: Option<usize>) -> Option<usize> {
        let remainders = self.remainders;
        let chunks = self.chunks;

        // check if id might be in the remainders list
        if remainders.first().is_some_and(|elem| id >= elem.id.get()) {
            return None;
        }

        let start_chunk = start_chunk.unwrap_or(0);
        let chunks_slice = chunks.get(start_chunk..)?;
        if chunks_slice.is_empty() {
            return None;
        }

        // No need to check if id is under range of posting list,
        // this function assumes it is within the range
        debug_assert!(id >= chunks_slice[0].initial_id.get());
        debug_assert!(self.last_id.is_some_and(|last_id| id <= last_id));

        // Fast-path: check if `id` falls into the first chunk's range
        let first = &chunks_slice[0];
        if let Some(second) = chunks_slice.get(1) {
            let id0 = first.initial_id.get();
            let id1 = second.initial_id.get();

            if id0 <= id && id < id1 {
                return Some(start_chunk);
            }
        }

        match chunks_slice.binary_search_by(|chunk| chunk.initial_id.get().cmp(&id)) {
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

    pub(crate) fn search_in_remainders(&self, id: PointOffsetType) -> Result<usize, usize> {
        self.remainders
            .binary_search_by(|elem| elem.id.get().cmp(&id))
    }

    /// The total number of elements in the posting list.
    pub fn len(&self) -> usize {
        self.chunks.len() * CHUNK_LEN + self.remainders.len()
    }

    /// Checks if there are no elements in the posting list.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
