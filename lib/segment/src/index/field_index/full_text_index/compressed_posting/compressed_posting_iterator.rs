use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_chunks_reader::ChunkReader;
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_visitor::CompressedPostingVisitor;

pub struct CompressedPostingIterator<R: ChunkReader> {
    visitor: CompressedPostingVisitor<R>,
    offset: usize,
}

impl<R: ChunkReader> CompressedPostingIterator<R> {
    pub fn new(visitor: CompressedPostingVisitor<R>) -> CompressedPostingIterator<R> {
        CompressedPostingIterator { visitor, offset: 0 }
    }
}

impl<R: ChunkReader> Iterator for CompressedPostingIterator<R> {
    type Item = PointOffsetType;

    fn next(&mut self) -> Option<Self::Item> {
        let val = self.visitor.get_by_offset(self.offset);
        self.offset += 1;
        val
    }
}
