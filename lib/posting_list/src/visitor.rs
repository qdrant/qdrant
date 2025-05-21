use common::types::PointOffsetType;

use crate::CHUNK_LEN;
use crate::iterator::PostingIterator;
use crate::posting_list::PostingElement;
use crate::value_handler::ValueHandler;
use crate::view::PostingListView;

/// A visitor for a posting list which caches the latest decompressed chunk of ids.
pub struct PostingVisitor<'a, H: ValueHandler> {
    pub(crate) list: PostingListView<'a, H>,

    /// Index of the decompressed chunk.
    /// It is used to shorten the search range of chunk index for the next value.
    decompressed_chunk_idx: Option<usize>,

    /// Lazy decompressed chunk of ids. Never access this directly, prefer [`Self::decompressed_chunk`] function
    decompressed_chunk: [PointOffsetType; CHUNK_LEN],
}

impl<'a, H: ValueHandler> PostingVisitor<'a, H> {
    pub(crate) fn new(view: PostingListView<'a, H>) -> Self {
        Self {
            list: view,
            decompressed_chunk_idx: None,
            decompressed_chunk: [0; CHUNK_LEN],
        }
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Returns the decompressed slice of ids for a chunk.
    ///
    /// Assumes the chunk_idx is valid.
    fn decompressed_chunk(&mut self, chunk_idx: usize) -> &[PointOffsetType; CHUNK_LEN] {
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

    pub(crate) fn get_by_offset(&mut self, offset: usize) -> Option<PostingElement<H::Value>> {
        let chunk_idx = offset / CHUNK_LEN;
        let local_offset = offset % CHUNK_LEN;

        // bound check
        if offset >= self.list.len() {
            return None;
        }

        // get from chunk
        if chunk_idx < self.list.chunks.len() {
            let id = self.decompressed_chunk(chunk_idx)[local_offset];
            let chunk_sized_values = self.list.sized_values_unchecked(chunk_idx);
            let sized_value = chunk_sized_values[local_offset];
            let next_sized_value = || {
                chunk_sized_values
                    .get(local_offset + 1)
                    .copied()
                    // or check first of the next chunk
                    .or_else(|| {
                        self.list
                            .sized_values(chunk_idx + 1)
                            .map(|sized_values| sized_values[0])
                    })
                    // or, if it is the last one, check first from remainders
                    .or_else(|| self.list.remainders.first().map(|e| e.value))
            };

            let value = H::get_value(sized_value, next_sized_value, self.list.var_size_data);

            return Some(PostingElement { id, value });
        }

        // else, get from remainder
        self.list.remainders.get(local_offset).map(|e| {
            let id = e.id;
            let next_sized_value = || self.list.remainders.get(local_offset + 1).map(|r| r.value);
            let value = H::get_value(e.value, next_sized_value, self.list.var_size_data);

            PostingElement { id, value }
        })
    }
}

impl<'a, H: ValueHandler> IntoIterator for PostingVisitor<'a, H> {
    type Item = PostingElement<H::Value>;
    type IntoIter = PostingIterator<'a, H>;

    fn into_iter(self) -> Self::IntoIter {
        PostingIterator::new(self)
    }
}
