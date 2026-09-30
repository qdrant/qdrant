use std::iter::FusedIterator;

use common::types::PointOffsetType;

use crate::value_handler::PostingValue;
use crate::visitor::PostingVisitor;
use crate::{PostingElement, PostingLen};

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
    offset: usize,
}

impl<'a, V: PostingValue> PostingLenIterator<'a, V> {
    pub fn new(visitor: PostingVisitor<'a, V>) -> Self {
        Self {
            visitor,
            current: None,
            offset: 0,
        }
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
            self.offset = self.visitor.len();
            return None;
        };

        debug_assert!(offset >= self.offset);
        self.current = self.visitor.value_len_by_offset(offset);
        self.offset = offset;
        self.current
    }
}

impl<V: PostingValue> Iterator for PostingLenIterator<'_, V> {
    type Item = PostingLen;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.visitor.value_len_by_offset(self.offset).inspect(|_| {
            self.offset += 1;
        });
        self.current = next;
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
