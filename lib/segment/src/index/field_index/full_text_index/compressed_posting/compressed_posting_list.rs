use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_chunks_reader::{
    ChunkReader, CompressedPostingChunk,
};
use crate::index::field_index::full_text_index::compressed_posting::compressed_common::BitPackerImpl;
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_visitor::{
    CompressedPostingIterator, CompressedPostingVisitor,
};
use crate::index::field_index::full_text_index::posting_list::PostingList;

#[derive(Clone, Debug, Default)]
pub struct CompressedPostingList {
    last_doc_id: PointOffsetType,
    data: Vec<u8>,
    chunks: Vec<CompressedPostingChunk>,
    // last postings that are not compressed because they are not aligned with the block size
    reminder_postings: Vec<PointOffsetType>,
}

impl CompressedPostingList {
    pub fn new(posting_list: PostingList) -> Self {
        let list = posting_list.into_vec();

        if list.is_empty() {
            return Self::default();
        }

        // fill chunks data
        let bitpacker = BitPackerImpl::new();
        let mut chunks = Vec::with_capacity(list.len() / BitPackerImpl::BLOCK_LEN);
        let mut data_size = 0;
        let mut noncompressed_postings = Vec::new();
        for chunk_data in list.chunks(BitPackerImpl::BLOCK_LEN) {
            if chunk_data.len() == BitPackerImpl::BLOCK_LEN {
                let initial = chunk_data[0];
                let chunk_bits: u8 = bitpacker.num_bits_sorted(initial, chunk_data);
                let chunk_size = BitPackerImpl::compressed_block_size(chunk_bits);
                chunks.push(CompressedPostingChunk {
                    initial,
                    offset: data_size as u32,
                });
                data_size += chunk_size;
            } else {
                // last chunk that is not aligned with the block size
                noncompressed_postings.extend_from_slice(chunk_data);
            }
        }

        // compress data
        let mut data = vec![0u8; data_size];
        for (chunk_index, chunk_data) in list.chunks_exact(BitPackerImpl::BLOCK_LEN).enumerate() {
            let chunk = &chunks[chunk_index];
            let chunk_size = ChunkReader::get_chunk_size(&chunks, &data, chunk_index);
            let chunk_bits = (chunk_size * 8) / BitPackerImpl::BLOCK_LEN;
            bitpacker.compress_sorted(
                chunk.initial,
                chunk_data,
                &mut data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                chunk_bits as u8,
            );
        }

        Self {
            last_doc_id: *list.last().unwrap(),
            data,
            chunks,
            reminder_postings: noncompressed_postings,
        }
    }

    pub fn reader(&self) -> ChunkReader {
        ChunkReader::new(
            &self.data,
            &self.chunks,
            &self.reminder_postings,
            self.last_doc_id,
        )
    }

    pub fn contains(&self, val: &PointOffsetType) -> bool {
        self.reader().contains(val)
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * BitPackerImpl::BLOCK_LEN + self.reminder_postings.len()
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
        let mut posting_list = PostingList::default();
        for i in 0..999 {
            set.insert(step * i);
            posting_list.insert(step * i);
        }
        let compressed_posting_list = CompressedPostingList::new(posting_list);
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
