use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_common::{
    BitPackerImpl, CompressedPostingChunksIndex,
};

pub struct ChunkReader<'a> {
    data: &'a [u8],
    pub chunks: &'a [CompressedPostingChunksIndex],
    pub reminder_postings: &'a [PointOffsetType],
    last_doc_id: PointOffsetType,
}

impl<'a> ChunkReader<'a> {
    pub fn new(
        data: &'a [u8],
        chunks: &'a [CompressedPostingChunksIndex],
        reminder_postings: &'a [PointOffsetType],
        last_doc_id: PointOffsetType,
    ) -> Self {
        Self {
            data,
            chunks,
            reminder_postings,
            last_doc_id,
        }
    }

    pub fn is_in_postings_range(&self, val: PointOffsetType) -> bool {
        let in_chunks_range =
            !self.chunks.is_empty() && val >= self.chunks[0].initial && val <= self.last_doc_id;
        let in_noncompressed_range = !self.reminder_postings.is_empty()
            && val >= self.reminder_postings[0]
            && val <= self.last_doc_id;
        in_chunks_range || in_noncompressed_range
    }

    pub fn contains(&self, val: &PointOffsetType) -> bool {
        if !self.is_in_postings_range(*val) {
            return false;
        }

        // find the chunk that may contain the value and check if the value is in the chunk
        let chunk_index = self.find_chunk(val, None);
        if let Some(chunk_index) = chunk_index {
            if self.chunks[chunk_index].initial == *val {
                return true;
            }

            let mut decompressed = [0u32; BitPackerImpl::BLOCK_LEN];
            self.decompress_chunk(&BitPackerImpl::new(), chunk_index, &mut decompressed);
            decompressed.binary_search(val).is_ok()
        } else {
            self.reminder_postings.binary_search(val).is_ok()
        }
    }

    pub fn get_chunk_size(
        chunks: &[CompressedPostingChunksIndex],
        data_len_bytes: usize,
        chunk_index: usize,
    ) -> usize {
        assert!(chunk_index < chunks.len());
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            data_len_bytes - chunks[chunk_index].offset as usize
        }
    }

    pub fn find_chunk(
        &self,
        doc_id: &PointOffsetType,
        start_chunk: Option<usize>,
    ) -> Option<usize> {
        if !self.reminder_postings.is_empty() && doc_id >= self.reminder_postings.first().unwrap() {
            // doc_id is in the noncompressed postings range
            return None;
        }

        if self.chunks.is_empty() {
            return None;
        }

        let start_chunk = start_chunk.unwrap_or(0);
        match self.chunks[start_chunk..].binary_search_by(|chunk| chunk.initial.cmp(doc_id)) {
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
        assert_eq!(decompressed.len(), BitPackerImpl::BLOCK_LEN);
        let chunk = &self.chunks[chunk_index];
        let chunk_size = Self::get_chunk_size(self.chunks, self.data.len(), chunk_index);
        let chunk_bits = (chunk_size * 8) / BitPackerImpl::BLOCK_LEN;
        bitpacker.decompress_sorted(
            chunk.initial,
            &self.data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            decompressed,
            chunk_bits as u8,
        );
    }
}
