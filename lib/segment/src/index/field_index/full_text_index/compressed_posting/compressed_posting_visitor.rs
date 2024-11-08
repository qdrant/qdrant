use bitpacking::BitPacker;
use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_chunks_reader::ChunkReader;
use crate::index::field_index::full_text_index::compressed_posting::compressed_common::BitPackerImpl;

/// Help structure to find intersection of compressed postings and set of sorted values.
/// This help structure reuse the decompressed chunk to avoid unnecessary decompression.
pub struct CompressedPostingVisitor<'a> {
    bitpacker: BitPackerImpl,
    chunk_reader: ChunkReader<'a>,

    /// Data for the decompressed chunk.
    decompressed_chunk: [PointOffsetType; BitPackerImpl::BLOCK_LEN],

    /// Index of the decompressed chunk.
    /// It is used to shorten the search range of chunk index for the next value.
    decompressed_chunk_idx: Option<usize>,

    /// start index of the decompressed chunk.
    /// It is used to shorten the search range in decompressed chunk for the next value.
    decompressed_chunk_start_index: usize,

    /// Check if the checked values are in the increasing order.
    #[cfg(test)]
    last_checked: Option<PointOffsetType>,
}

impl<'a> CompressedPostingVisitor<'a> {
    pub fn new(chunk_reader: ChunkReader<'a>) -> CompressedPostingVisitor<'a> {
        CompressedPostingVisitor {
            bitpacker: BitPackerImpl::new(),
            chunk_reader,
            decompressed_chunk: [0; BitPackerImpl::BLOCK_LEN],
            decompressed_chunk_idx: None,
            decompressed_chunk_start_index: 0,
            #[cfg(test)]
            last_checked: None,
        }
    }

    fn decompress_chunk(&mut self, chunk_index: usize) {
        // second, decompress the chunk and check if the value is in the decompressed chunk
        self.chunk_reader.decompress_chunk(
            &self.bitpacker,
            chunk_index,
            &mut self.decompressed_chunk,
        );
        self.decompressed_chunk_idx = Some(chunk_index);
        self.decompressed_chunk_start_index = 0;
    }

    /// Check if the next value is in the compressed posting list.
    /// This function reuses the decompressed chunk to avoid unnecessary decompression.
    /// It is useful when the visitor is used to check the values in the increasing order.
    pub fn contains_next_and_advance(&mut self, val: PointOffsetType) -> bool {
        #[cfg(test)]
        {
            // check if the checked values are in the increasing order
            if let Some(last_checked) = self.last_checked {
                assert!(val > last_checked);
            }
            self.last_checked = Some(val);
        }

        if !self.chunk_reader.is_in_postings_range(val) {
            return false;
        }

        // check if current decompressed chunks range contains the value
        if self.decompressed_chunk_idx.is_some() {
            // check if value is in decompressed chunk range
            // check for max value in the chunk only because we already checked for min value while decompression
            let last_decompressed = &self.decompressed_chunk[BitPackerImpl::BLOCK_LEN - 1];
            match val.cmp(last_decompressed) {
                std::cmp::Ordering::Less => {
                    // value is less than the last decompressed value
                    return self.find_in_decompressed_and_advance(val);
                }
                std::cmp::Ordering::Equal => {
                    // value is equal to the last decompressed value
                    return true;
                }
                std::cmp::Ordering::Greater => {}
            }
        }

        // decompressed chunk is not in the range, so we need to decompress another chunk
        // first, check if there is a chunk that contains the value
        let Some(chunk_index) = self
            .chunk_reader
            .find_chunk(val, self.decompressed_chunk_idx)
        else {
            // value is in the noncompressed postings range
            self.decompressed_chunk_idx = None;
            return self
                .chunk_reader
                .remainder_postings
                .binary_search(&val)
                .is_ok();
        };
        // if the value is the initial value of the chunk, we don't need to decompress the chunk
        if self.chunk_reader.chunks[chunk_index].initial == val {
            return true;
        }

        self.decompress_chunk(chunk_index);

        // check if the value is in the decompressed chunk
        self.find_in_decompressed_and_advance(val)
    }

    fn find_in_decompressed_and_advance(&mut self, val: PointOffsetType) -> bool {
        match self.decompressed_chunk[self.decompressed_chunk_start_index..].binary_search(&val) {
            Ok(idx) => {
                self.decompressed_chunk_start_index = idx;
                true
            }
            Err(idx) => {
                self.decompressed_chunk_start_index = idx;
                false
            }
        }
    }

    pub fn get_by_offset(&mut self, offset: usize) -> Option<PointOffsetType> {
        let chunk_idx = offset / BitPackerImpl::BLOCK_LEN;

        if chunk_idx >= self.chunk_reader.chunks.len() {
            // Reminder postings
            let reminder_idx = offset - self.chunk_reader.chunks.len() * BitPackerImpl::BLOCK_LEN;
            return self
                .chunk_reader
                .remainder_postings
                .get(reminder_idx)
                .copied();
        }

        if self.decompressed_chunk_idx != Some(chunk_idx) {
            self.decompress_chunk(chunk_idx);
        }

        self.decompressed_chunk
            .get(offset % BitPackerImpl::BLOCK_LEN)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_list::CompressedPostingList;

    #[test]
    fn test_compressed_posting_visitor() {
        for build_step in 0..3 {
            let (compressed_posting_list, set) =
                CompressedPostingList::generate_compressed_posting_list_fixture(build_step);

            for search_step in 1..512 {
                let mut visitor = CompressedPostingVisitor::new(compressed_posting_list.reader());
                for i in 0..build_step * 1000 {
                    if i % search_step == 0 {
                        assert_eq!(visitor.contains_next_and_advance(i), set.contains(&i));
                    }
                }
            }
        }
    }

    #[test]
    fn test_access_data_in_posting() {
        let (posting, _all_offsets) =
            CompressedPostingList::generate_compressed_posting_list_fixture(10);

        let reader = posting.reader();

        assert!(reader.contains(0));
        assert!(reader.contains(10));
        assert!(reader.contains(400));
        assert!(reader.contains(8940));
        assert!(reader.contains(8950));
        assert!(reader.contains(8960));
        assert!(reader.contains(8970));
        assert!(reader.contains(8980));
        assert!(reader.contains(8990));
        assert!(reader.contains(9970));
        assert!(reader.contains(9980));

        assert!(!reader.contains(1));
        assert!(!reader.contains(11));
        assert!(!reader.contains(401));
        assert!(!reader.contains(9971));
        assert!(!reader.contains(9981));

        let mut visitor = CompressedPostingVisitor::new(reader);

        for i in 0..posting.len() {
            let val = visitor.get_by_offset(i).expect("Value should be present");
            assert_eq!(val, i as PointOffsetType * 10);
        }
    }
}
