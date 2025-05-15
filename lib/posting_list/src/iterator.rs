use common::types::PointOffsetType;

use crate::PostingElement;
use crate::value_handler::ValueHandler;
use crate::visitor::PostingVisitor;

pub struct PostingIterator<'a, V, H: ValueHandler<V>> {
    pub(crate) visitor: PostingVisitor<'a, V, H>,
    current_id: Option<PointOffsetType>,
    offset: usize,
}

impl<'a, V, H: ValueHandler<V>> PostingIterator<'a, V, H> {
    pub fn new(visitor: PostingVisitor<'a, V, H>) -> Self {
        Self {
            visitor,
            current_id: None,
            offset: 0,
        }
    }
}

impl<V, H: ValueHandler<V>> Iterator for PostingIterator<'_, V, H> {
    type Item = PostingElement<V>;

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

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.size_hint().0
    }
}

impl<V, H: ValueHandler<V>> ExactSizeIterator for PostingIterator<'_, V, H> {
    fn len(&self) -> usize {
        self.visitor.list.len().saturating_sub(self.offset)
    }
}
