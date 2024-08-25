use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_chunks_reader::ChunkReader;
use crate::index::field_index::full_text_index::compressed_posting::compressed_common::{
    compress_posting, estimate_chunks, BitPackerImpl, CompressedPostingChunk,
};
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_iterator::CompressedPostingIterator;
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_visitor::CompressedPostingVisitor;

#[derive(Clone, Debug, Default)]
pub struct CompressedPostingList {
    last_doc_id: PointOffsetType,
    data: Vec<u8>,
    chunks: Vec<CompressedPostingChunk>,
    // last postings that are not compressed because they are not aligned with the block size
    remainder_postings: Vec<PointOffsetType>,
}

impl CompressedPostingList {
    pub fn new(posting_list: &[PointOffsetType]) -> Self {
        if posting_list.is_empty() {
            return Self::default();
        }
        let (chunks, remainder_postings, data_size) = estimate_chunks(posting_list);

        // compressed data storage
        let mut data = vec![0u8; data_size];

        compress_posting(posting_list, &chunks, &mut data);

        Self {
            last_doc_id: *posting_list.last().unwrap(),
            data,
            chunks,
            remainder_postings,
        }
    }

    pub fn reader(&self) -> ChunkReader {
        ChunkReader::new(
            &self.data,
            &self.chunks,
            &self.remainder_postings,
            self.last_doc_id,
        )
    }

    pub fn contains(&self, val: &PointOffsetType) -> bool {
        self.reader().contains(val)
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * BitPackerImpl::BLOCK_LEN + self.remainder_postings.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        let reader = self.reader();
        let visitor = CompressedPostingVisitor::new(reader);
        CompressedPostingIterator::new(visitor)
    }

    #[cfg(test)]
    pub fn generate_compressed_posting_list_fixture(
        step: PointOffsetType,
    ) -> (
        CompressedPostingList,
        std::collections::HashSet<PointOffsetType>,
    ) {
        let mut set = std::collections::HashSet::new();
        let mut posting_list = vec![];
        for i in 0..999 {
            set.insert(step * i);
            posting_list.push(step * i);
        }
        let compressed_posting_list = CompressedPostingList::new(&posting_list);
        (compressed_posting_list, set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_posting_contains() {
        for step in 0..3 {
            let (compressed_posting_list, set) =
                CompressedPostingList::generate_compressed_posting_list_fixture(step);
            for i in 0..step * 1000 {
                assert_eq!(compressed_posting_list.contains(&i), set.contains(&i));
            }
        }
    }
}
