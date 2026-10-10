use std::iter::FusedIterator;

use common::types::PointOffsetType;

use crate::value_handler::{PostingValue, ValueHandler};
use crate::visitor::PostingVisitor;
use crate::{CHUNK_LEN, PostingElement, PostingLen};

pub struct PostingIterator<'a, V: PostingValue> {
    visitor: PostingVisitor<'a, V>,
    current_elem: Option<PostingElement<V>>,
    offset: usize,
}

impl<'a, V: PostingValue> PostingIterator<'a, V> {
    pub fn new(visitor: PostingVisitor<'a, V>) -> Self {
        Self {
            visitor,
            current_elem: None,
            offset: 0,
        }
    }

    /// Advances the iterator until the current element id is greater than or equal to the given id.
    ///
    /// Returns `Some(PostingElement)` on the first element that is greater than or equal to the given id. It can be possible that this id is
    /// the head of the iterator, so it does not need to be advanced.
    ///
    /// `None` means the iterator is exhausted.
    ///
    /// The seek leaves the iterator *on* the element it returns, not past it: a following
    /// [`Iterator::next`] yields that same element again. Callers mixing the two on one iterator
    /// have to skip it themselves.
    pub fn advance_until_greater_or_equal(
        &mut self,
        target_id: PointOffsetType,
    ) -> Option<PostingElement<V>> {
        if let Some(current) = &self.current_elem
            && current.id >= target_id
        {
            return Some(current.clone());
        }

        if self.offset >= self.visitor.len() {
            return None;
        }

        let Some(offset) = self
            .visitor
            .search_greater_or_equal(target_id, Some(self.offset))
        else {
            self.current_elem = None;
            self.offset = self.visitor.len();
            return None;
        };

        debug_assert!(offset >= self.offset);
        let greater_or_equal = self.visitor.get_by_offset(offset);

        self.current_elem = greater_or_equal.clone();
        self.offset = offset;

        greater_or_equal
    }
}

impl<V: PostingValue> Iterator for PostingIterator<'_, V> {
    type Item = PostingElement<V>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_opt = self.visitor.get_by_offset(self.offset).inspect(|_| {
            self.offset += 1;
        });

        self.current_elem = next_opt.clone();

        next_opt
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_len = self.len();
        (remaining_len, Some(remaining_len))
    }

    fn count(self) -> usize {
        self.size_hint().0
    }
}

impl<V: PostingValue> ExactSizeIterator for PostingIterator<'_, V> {
    fn len(&self) -> usize {
        self.visitor.list.len().saturating_sub(self.offset)
    }
}

impl<V: PostingValue> FusedIterator for PostingIterator<'_, V> {}

/// Like [`PostingIterator`], but yields each id with the byte length of its
/// value instead of the value itself. Never reads the variable-size data, so on
/// an mmap'd list the value bytes are never faulted in.
///
/// A term frequency stored as a list of positions is one such length: the
/// number of positions is the number of bytes divided by their width, and the
/// positions themselves are not needed to know how many there are.
pub struct PostingLenIterator<'a, V: PostingValue> {
    visitor: PostingVisitor<'a, V>,
    current: Option<PostingLen>,
    /// The offset of `current`, while there is one.
    current_offset: Option<usize>,
    offset: usize,
}

impl<'a, V: PostingValue> PostingLenIterator<'a, V> {
    pub fn new(visitor: PostingVisitor<'a, V>) -> Self {
        Self {
            visitor,
            current: None,
            current_offset: None,
            offset: 0,
        }
    }

    /// Call `f` for every element from the current one, if any, else from the
    /// next, to the end of the list, in order, and leave the iterator
    /// exhausted.
    ///
    /// The same elements a loop of [`Iterator::next`] would yield, a chunk at
    /// a time: each chunk is decompressed once and its lengths read from
    /// neighbouring offsets, with no per-element offset arithmetic.
    pub fn for_each_remaining(&mut self, mut f: impl FnMut(PostingLen)) {
        let mut offset = self.current_offset.unwrap_or(self.offset);
        let len = self.visitor.len();
        let chunks = self.visitor.list.chunks_len();
        let var_data_len = self.visitor.list.var_size_data.len();
        while offset < len {
            let chunk_idx = offset / CHUNK_LEN;
            if chunk_idx < chunks {
                let ids = *self.visitor.decompressed_chunk(chunk_idx);
                let list = &self.visitor.list;
                let sized_values = &list.get_chunk_unchecked(chunk_idx).sized_values;
                // The value after the chunk's last: the next chunk's first, or
                // the first remainder's.
                let after = list
                    .get_chunk(chunk_idx + 1)
                    .map(|chunk| chunk.sized_values[0])
                    .or_else(|| list.get_remainder(0).map(|e| e.value));
                for local in offset % CHUNK_LEN..CHUNK_LEN {
                    let next = sized_values.get(local + 1).copied().or(after);
                    let value_len =
                        V::Handler::value_len(sized_values[local], || next, var_data_len);
                    f(PostingLen {
                        id: ids[local],
                        value_len,
                    });
                }
                offset = (chunk_idx + 1) * CHUNK_LEN;
            } else {
                let list = &self.visitor.list;
                for local in offset % CHUNK_LEN..list.remainders_len() {
                    let e = list.get_remainder(local).expect("within the remainders");
                    let next = list.get_remainder(local + 1).map(|r| r.value);
                    let value_len = V::Handler::value_len(e.value, || next, var_data_len);
                    f(PostingLen {
                        id: e.id.get(),
                        value_len,
                    });
                }
                offset = len;
            }
        }
        self.offset = len;
        self.current = None;
        self.current_offset = None;
    }

    /// The element most recently yielded by [`Iterator::next`] or
    /// [`Self::advance_until_greater_or_equal`], if any.
    pub fn current(&self) -> Option<PostingLen> {
        self.current
    }

    /// Advances until the current id is greater than or equal to `target_id`,
    /// with the same contract as [`PostingIterator::advance_until_greater_or_equal`].
    pub fn advance_until_greater_or_equal(
        &mut self,
        target_id: PointOffsetType,
    ) -> Option<PostingLen> {
        if let Some(current) = self.current
            && current.id >= target_id
        {
            return Some(current);
        }

        if self.offset >= self.visitor.len() {
            return None;
        }

        let Some(offset) = self
            .visitor
            .search_greater_or_equal(target_id, Some(self.offset))
        else {
            self.current = None;
            self.current_offset = None;
            self.offset = self.visitor.len();
            return None;
        };

        debug_assert!(offset >= self.offset);
        self.current = self.visitor.value_len_by_offset(offset);
        self.current_offset = self.current.map(|_| offset);
        self.offset = offset;
        self.current
    }
}

impl<V: PostingValue> Iterator for PostingLenIterator<'_, V> {
    type Item = PostingLen;

    fn next(&mut self) -> Option<Self::Item> {
        let at = self.offset;
        let next = self.visitor.value_len_by_offset(at).inspect(|_| {
            self.offset += 1;
        });
        self.current = next;
        self.current_offset = next.map(|_| at);
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_len = self.len();
        (remaining_len, Some(remaining_len))
    }
}

impl<V: PostingValue> ExactSizeIterator for PostingLenIterator<'_, V> {
    fn len(&self) -> usize {
        self.visitor.list.len().saturating_sub(self.offset)
    }
}

impl<V: PostingValue> FusedIterator for PostingLenIterator<'_, V> {}
