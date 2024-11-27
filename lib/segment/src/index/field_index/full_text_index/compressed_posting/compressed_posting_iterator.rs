use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_visitor::CompressedPostingVisitor;

pub struct CompressedPostingIterator<'a> {
    visitor: CompressedPostingVisitor<'a>,
    offset: usize,
}

impl<'a> CompressedPostingIterator<'a> {
    pub fn new(visitor: CompressedPostingVisitor<'a>) -> CompressedPostingIterator<'a> {
        CompressedPostingIterator { visitor, offset: 0 }
    }
}

impl Iterator for CompressedPostingIterator<'_> {
    type Item = PointOffsetType;

    fn next(&mut self) -> Option<Self::Item> {
        let val = self.visitor.get_by_offset(self.offset);
        self.offset += 1;
        val
    }
}
