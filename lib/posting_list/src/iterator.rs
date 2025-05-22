use std::iter::FusedIterator;

use common::types::PointOffsetType;

use crate::PostingElement;
use crate::value_handler::ValueHandler;
use crate::visitor::PostingVisitor;

pub struct PostingIterator<'a, H: ValueHandler> {
    visitor: PostingVisitor<'a, H>,
    current_id: Option<PointOffsetType>,
    offset: usize,
}

impl<'a, H: ValueHandler> PostingIterator<'a, H> {
    pub fn new(visitor: PostingVisitor<'a, H>) -> Self {
        Self {
            visitor,
            current_id: None,
            offset: 0,
        }
    }
}

impl<H: ValueHandler> Iterator for PostingIterator<'_, H> {
    type Item = PostingElement<H::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.visitor.get_by_offset(self.offset).inspect(|elem| {
            self.current_id = Some(elem.id);
            self.offset += 1;
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_len = self.len();
        (remaining_len, Some(remaining_len))
    }

    fn count(self) -> usize {
        self.size_hint().0
    }
}

impl<H: ValueHandler> ExactSizeIterator for PostingIterator<'_, H> {
    fn len(&self) -> usize {
        self.visitor.list.len().saturating_sub(self.offset)
    }
}

impl<H: ValueHandler> FusedIterator for PostingIterator<'_, H> {}
