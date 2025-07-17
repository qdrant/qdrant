use common::types::PointOffsetType;

use crate::iterator::PostingIterator;
use crate::value_handler::{PostingValue, ValueHandler};
use crate::view::PostingListView;
use crate::{CHUNK_LEN, PostingElement};

/// A visitor for a posting list which caches the latest decompressed chunk of ids.
pub struct PostingVisitor<'a, V: PostingValue> {
    pub(crate) list: PostingListView<'a, V>,

    /// Index of the decompressed chunk.
    /// It is used to shorten the search range of chunk index for the next value.
    decompressed_chunk_idx: Option<usize>,

    /// Lazy decompressed chunk of ids. Never access this directly, prefer [`Self::decompressed_chunk`] function
    decompressed_chunk: [PointOffsetType; CHUNK_LEN],
}

impl<'a, V: PostingValue> PostingVisitor<'a, V> {
    pub(crate) fn new(view: PostingListView<'a, V>) -> Self {
        Self {
            list: view,
            decompressed_chunk_idx: None,
            decompressed_chunk: [0; CHUNK_LEN],
        }
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
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

    /// Returns the first offset whose element id is greater or equal to the given id.
    ///
    /// Returns `None` if there is no such element in the posting list
    pub(crate) fn search_greater_or_equal(
        &mut self,
        id: PointOffsetType,
        start_from_offset: Option<usize>,
    ) -> Option<usize> {
        let start_chunk = start_from_offset
            .map(|offset| offset / CHUNK_LEN)
            .unwrap_or(0);

        let ids_range = self.list.ids_range(start_chunk)?;
        // check if the first in the chunk is already greater or equal to the target id
        if ids_range.start() >= &id {
            return Some(start_chunk * CHUNK_LEN);
        }
        // check if the target id is already greater than the last id
        if ids_range.end() < &id {
            return None;
        }

        // Find the chunk that may contain the id and check if the id is in the chunk
        let chunk_index = self.list.find_chunk(id, Some(start_chunk));

        if let Some(chunk_index) = chunk_index {
            let local_offset = match self.decompressed_chunk(chunk_index).binary_search(&id) {
                Ok(found_local_offset) => found_local_offset,
                Err(closest_local_offset) => {
                    // If the target id is bigger than all the values here, and smaller than the first id
                    // in the next chunk or remainders, then that next id is the closest greater id
                    if closest_local_offset >= CHUNK_LEN {
                        let next_offset = (chunk_index + 1) * CHUNK_LEN;

                        let next_offset_exists = next_offset < self.len();

                        return next_offset_exists.then_some(next_offset);
                    }

                    closest_local_offset
                }
            };

            return Some(local_offset + (chunk_index * CHUNK_LEN));
        }

        // Check in remainders
        let remainder_offset = match self.list.search_in_remainders(id) {
            Ok(found_remainder_offset) => found_remainder_offset,
            Err(closest_remainder_offset) => {
                if closest_remainder_offset >= self.list.remainders_len() {
                    // There is no greater or equal id in the posting list
                    return None;
                }

                closest_remainder_offset
            }
        };

        Some(remainder_offset + self.list.chunks_len() * CHUNK_LEN)
    }

    pub fn contains(&mut self, id: PointOffsetType) -> bool {
        if self
            .list
            .ids_range(0)
            .is_none_or(|range| !range.contains(&id))
        {
            return false;
        }

        // Find the chunk that may contain the id and check if the id is in the chunk
        let chunk_index = self.list.find_chunk(id, None);
        if let Some(chunk_idx) = chunk_index {
            if self.list.get_chunk_unchecked(chunk_idx).initial_id == id {
                return true;
            }

            self.decompressed_chunk(chunk_idx)
                .binary_search(&id)
                .is_ok()
        } else {
            self.list.search_in_remainders(id).is_ok()
        }
    }

    pub(crate) fn get_by_offset(&mut self, offset: usize) -> Option<PostingElement<V>> {
        let chunk_idx = offset / CHUNK_LEN;
        let local_offset = offset % CHUNK_LEN;

        // bound check
        if offset >= self.list.len() {
            return None;
        }

        // get from chunk
        if chunk_idx < self.list.chunks_len() {
            let id = self.decompressed_chunk(chunk_idx)[local_offset];
            let chunk_sized_values = self.list.get_chunk_unchecked(chunk_idx).sized_values;
            let sized_value = chunk_sized_values[local_offset];
            let next_sized_value = || {
                chunk_sized_values
                    .get(local_offset + 1)
                    .copied()
                    // or check first of the next chunk
                    .or_else(|| {
                        self.list
                            .get_chunk(chunk_idx + 1)
                            .map(|chunk| chunk.sized_values[0])
                    })
                    // or, if it is the last one, check first from remainders
                    .or_else(|| self.list.get_remainder(0).map(|e| e.value))
            };

            let value =
                V::Handler::get_value(sized_value, next_sized_value, self.list.var_size_data);

            return Some(PostingElement { id, value });
        }

        // else, get from remainder
        self.list.get_remainder(local_offset).map(|e| {
            let id = e.id;
            let next_sized_value = || self.list.get_remainder(local_offset + 1).map(|r| r.value);
            let value = V::Handler::get_value(e.value, next_sized_value, self.list.var_size_data);

            PostingElement {
                id: id.get(),
                value,
            }
        })
    }
}

impl<'a, V: PostingValue> IntoIterator for PostingVisitor<'a, V> {
    type Item = PostingElement<V>;
    type IntoIter = PostingIterator<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        PostingIterator::new(self)
    }
}
