use bitpacking::BitPacker;
use common::counter::conditioned_counter::ConditionedCounter;
use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_common::{
    BitPackerImpl, CompressedPostingChunksIndex, get_chunk_size,
};

pub struct ChunkReader<'a> {
    pub last_doc_id: PointOffsetType,
    chunks: &'a [CompressedPostingChunksIndex],
    data: &'a [u8],
    remainder_postings: &'a [PointOffsetType],
    hw_counter: ConditionedCounter<'a>,
}

impl<'a> ChunkReader<'a> {
    pub fn new(
        last_doc_id: PointOffsetType,
        chunks: &'a [CompressedPostingChunksIndex],
        data: &'a [u8],
        reminder_postings: &'a [PointOffsetType],
        hw_counter: ConditionedCounter<'a>,
    ) -> Self {
        Self {
            data,
            chunks,
            remainder_postings: reminder_postings,
            last_doc_id,
            hw_counter,
        }
    }

    pub fn is_in_postings_range(&self, val: PointOffsetType) -> bool {
        let last_doc_id = self.last_doc_id;
        let chunks = self.chunks;
        let remainder_postings = self.remainder_postings;

        let in_chunks_range = !chunks.is_empty() && val >= chunks[0].initial && val <= last_doc_id;
        let in_noncompressed_range =
            !remainder_postings.is_empty() && val >= remainder_postings[0] && val <= last_doc_id;
        in_chunks_range || in_noncompressed_range
    }

    pub fn contains(&self, val: PointOffsetType) -> bool {
        if !self.is_in_postings_range(val) {
            return false;
        }
        // find the chunk that may contain the value and check if the value is in the chunk
        let chunk_index = self.find_chunk(val, None);
        if let Some(chunk_index) = chunk_index {
            if self.chunks[chunk_index].initial == val {
                return true;
            }

            let mut decompressed = [0u32; BitPackerImpl::BLOCK_LEN];
            self.decompress_chunk(&BitPackerImpl::new(), chunk_index, &mut decompressed);
            decompressed.binary_search(&val).is_ok()
        } else {
            self.search_in_remainder(val)
        }
    }

    pub fn find_chunk(&self, doc_id: PointOffsetType, start_chunk: Option<usize>) -> Option<usize> {
        let remainder_postings = self.remainder_postings;
        let chunks = self.chunks;

        if !remainder_postings.is_empty() && &doc_id >= remainder_postings.first().unwrap() {
            // doc_id is in the noncompressed postings range
            return None;
        }

        if chunks.is_empty() {
            return None;
        }

        let start_chunk = start_chunk.unwrap_or(0);
        match chunks[start_chunk..].binary_search_by(|chunk| chunk.initial.cmp(&doc_id)) {
            // doc_id is the initial value of the chunk with index idx
            Ok(idx) => Some(start_chunk + idx),
            // chunk idx has larger initial value than doc_id
            // so we need the previous chunk
            Err(idx) if idx > 0 => Some(start_chunk + idx - 1),
            Err(_) => None,
        }
    }

    pub fn decompress_chunk(
        &self,
        bitpacker: &BitPackerImpl,
        chunk_index: usize,
        decompressed: &mut [PointOffsetType],
    ) {
        let chunks = self.chunks;
        let data = self.data;

        assert_eq!(decompressed.len(), BitPackerImpl::BLOCK_LEN);
        let chunk = &chunks[chunk_index];
        let chunk_size = get_chunk_size(chunks, data.len(), chunk_index);

        self.hw_counter
            .payload_index_io_read_counter()
            .incr_delta(chunk_size);

        let chunk_bits = (chunk_size * u8::BITS as usize) / BitPackerImpl::BLOCK_LEN;
        bitpacker.decompress_sorted(
            chunk.initial,
            &data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            decompressed,
            chunk_bits as u8,
        );
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * BitPackerImpl::BLOCK_LEN + self.remainder_postings.len()
    }

    pub fn chunks_len(&self) -> usize {
        self.chunks.len()
    }

    pub fn get_chunk_index(&self, offset: usize) -> &CompressedPostingChunksIndex {
        self.hw_counter
            .payload_index_io_read_counter()
            .incr_delta(size_of::<CompressedPostingChunksIndex>());
        &self.chunks[offset]
    }

    pub fn get_remainder_posting(&self, offset: usize) -> Option<PointOffsetType> {
        self.hw_counter
            .payload_index_io_read_counter()
            .incr_delta(size_of::<PointOffsetType>());
        self.remainder_postings.get(offset).copied()
    }

    pub fn search_in_remainder(&self, val: PointOffsetType) -> bool {
        self.hw_counter
            .payload_index_io_read_counter()
            .incr_delta(size_of_val(self.remainder_postings));
        self.remainder_postings.binary_search(&val).is_ok()
    }
}
