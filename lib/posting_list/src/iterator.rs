use crate::{value_handler::ValueHandler, visitor::PostingVisitor, PostingElement};

pub struct PostingIterator<'a, V, S> {
    visitor: PostingVisitor<'a, V, S>,
    idx: usize,
}

impl<'a, V, S> PostingIterator<'a, V, S> {
    pub fn new(visitor: PostingVisitor<'a, V, S>) -> Self {
        Self { visitor, idx: 0 }
    }
}

impl<'a, S: Copy, V: ValueHandler<V, Sized = S>> Iterator for PostingIterator<'a, V, S> {
    type Item = PostingElement<V>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.visitor.get_by_offset(self.idx);
        self.idx += 1;
        next
    }
}
