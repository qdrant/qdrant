use std::iter::FusedIterator;

use common::types::PointOffsetType;

use crate::PostingElement;
use crate::value_handler::ValueHandler;
use crate::visitor::PostingVisitor;

pub struct PostingIterator<'a, H: ValueHandler> {
    visitor: PostingVisitor<'a, H>,
    current_elem: Option<PostingElement<H::Value>>,
    offset: usize,
}

impl<'a, H: ValueHandler> PostingIterator<'a, H>
where
    H::Value: Clone,
{
    pub fn new(visitor: PostingVisitor<'a, H>) -> Self {
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
    pub fn advance_until_greater_or_equal(
        &mut self,
        target_id: PointOffsetType,
    ) -> Option<PostingElement<H::Value>> {
        if let Some(current) = &self.current_elem {
            if current.id >= target_id {
                return Some(current.clone());
            }
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

impl<H: ValueHandler> Iterator for PostingIterator<'_, H>
where
    H::Value: Clone,
{
    type Item = PostingElement<H::Value>;

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

impl<H: ValueHandler> ExactSizeIterator for PostingIterator<'_, H>
where
    H::Value: Clone,
{
    fn len(&self) -> usize {
        self.visitor.list.len().saturating_sub(self.offset)
    }
}

impl<H: ValueHandler> FusedIterator for PostingIterator<'_, H> where H::Value: Clone {}
