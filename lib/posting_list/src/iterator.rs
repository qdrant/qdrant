use common::types::PointOffsetType;

use crate::PostingElement;
use crate::value_handler::ValueHandler;
use crate::visitor::PostingVisitor;

pub struct PostingIterator<'a, V, S> {
    pub(crate) visitor: PostingVisitor<'a, V, S>,
    current_id: Option<PointOffsetType>,
    idx: usize,
}

impl<'a, V, S> PostingIterator<'a, V, S> {
    pub fn new(visitor: PostingVisitor<'a, V, S>) -> Self {
        Self {
            visitor,
            current_id: None,
            idx: 0,
        }
    }
}

impl<S: Copy, V: ValueHandler<V, Sized = S>> Iterator for PostingIterator<'_, V, S> {
    type Item = PostingElement<V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.visitor.get_by_offset(self.idx).inspect(|elem| {
            self.current_id = Some(elem.id);
            self.idx += 1;
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_len = self.len();
        (remaining_len, Some(remaining_len))
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.size_hint().0
    }
}

impl<S: Copy, V: ValueHandler<V, Sized = S>> ExactSizeIterator for PostingIterator<'_, V, S> {
    fn len(&self) -> usize {
        self.visitor.list.len().saturating_sub(self.idx)
    }
}
