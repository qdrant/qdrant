use bitpacking::BitPacker;
use common::types::PointOffsetType;

type BitPackerImpl = bitpacking::BitPacker4x;

#[derive(Clone, Debug, Default)]
pub struct PostingList {
    list: Vec<PointOffsetType>,
}

impl PostingList {
    pub fn new(idx: PointOffsetType) -> Self {
        Self { list: vec![idx] }
    }

    pub fn insert(&mut self, idx: PointOffsetType) {
        if self.list.is_empty() || idx > *self.list.last().unwrap() {
            self.list.push(idx);
        } else if let Err(insertion_idx) = self.list.binary_search(&idx) {
            // Yes, this is O(n) but:
            // 1. That would give us maximal search performance with minimal memory usage
            // 2. Documents are inserted mostly sequentially, especially in large segments
            // 3. Vector indexing is more expensive anyway
            // 4. For loading, insertion is strictly in increasing order
            self.list.insert(insertion_idx, idx);
        }
    }

    pub fn remove(&mut self, idx: PointOffsetType) {
        if let Ok(removal_idx) = self.list.binary_search(&idx) {
            self.list.remove(removal_idx);
        }
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn contains(&self, val: &PointOffsetType) -> bool {
        self.list.binary_search(val).is_ok()
    }

    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.list.iter().copied()
    }
}

#[derive(Clone, Debug, Default)]
pub struct CompressedPostingList {
    last_doc_id: PointOffsetType,
    data: Vec<u8>,
    chunks: Vec<CompressedPostingChunk>,
    // last postings that are not compressed because they are not aligned with the block size
    noncompressed_postings: Vec<PointOffsetType>,
}

#[derive(Clone, Debug, Default)]
pub struct CompressedPostingChunk {
    initial: PointOffsetType,
    offset: u32,
}

impl CompressedPostingList {
    pub fn new(posting_list: PostingList) -> Self {
        if posting_list.list.is_empty() {
            return Self::default();
        }

        // fill chunks data
        let bitpacker = BitPackerImpl::new();
        let mut chunks = Vec::with_capacity(posting_list.len() / BitPackerImpl::BLOCK_LEN);
        let mut data_size = 0;
        let mut noncompressed_postings = Vec::new();
        for chunk_data in posting_list.list.chunks(BitPackerImpl::BLOCK_LEN) {
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
        for (chunk_index, chunk_data) in posting_list
            .list
            .chunks_exact(BitPackerImpl::BLOCK_LEN)
            .enumerate()
        {
            let chunk = &chunks[chunk_index];
            let chunk_size = Self::get_chunk_size(&chunks, &data, chunk_index);
            let chunk_bits = (chunk_size * 8) / BitPackerImpl::BLOCK_LEN;
            bitpacker.compress_sorted(
                chunk.initial,
                chunk_data,
                &mut data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                chunk_bits as u8,
            );
        }

        Self {
            last_doc_id: *posting_list.list.last().unwrap(),
            data,
            chunks,
            noncompressed_postings,
        }
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
            self.noncompressed_postings.binary_search(val).is_ok()
        }
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * BitPackerImpl::BLOCK_LEN + self.noncompressed_postings.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        let bitpacker = BitPackerImpl::new();
        (0..self.chunks.len())
            .flat_map(move |chunk_index| {
                let mut decompressed = [0u32; BitPackerImpl::BLOCK_LEN];
                self.decompress_chunk(&bitpacker, chunk_index, &mut decompressed);
                decompressed.into_iter()
            })
            .chain(self.noncompressed_postings.iter().copied())
    }

    fn get_chunk_size(chunks: &[CompressedPostingChunk], data: &[u8], chunk_index: usize) -> usize {
        assert!(chunk_index < chunks.len());
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index + 1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            data.len() - chunks[chunk_index].offset as usize
        }
    }

    fn find_chunk(&self, doc_id: &PointOffsetType, start_chunk: Option<usize>) -> Option<usize> {
        if !self.noncompressed_postings.is_empty()
            && doc_id >= self.noncompressed_postings.first().unwrap()
        {
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

    fn is_in_postings_range(&self, val: PointOffsetType) -> bool {
        let in_chunks_range =
            !self.chunks.is_empty() && val >= self.chunks[0].initial && val <= self.last_doc_id;
        let in_noncompressed_range = !self.noncompressed_postings.is_empty()
            && val >= self.noncompressed_postings[0]
            && val <= self.last_doc_id;
        in_chunks_range || in_noncompressed_range
    }

    fn decompress_chunk(
        &self,
        bitpacker: &BitPackerImpl,
        chunk_index: usize,
        decompressed: &mut [PointOffsetType],
    ) {
        assert!(decompressed.len() == BitPackerImpl::BLOCK_LEN);
        let chunk = &self.chunks[chunk_index];
        let chunk_size = Self::get_chunk_size(&self.chunks, &self.data, chunk_index);
        let chunk_bits = (chunk_size * 8) / BitPackerImpl::BLOCK_LEN;
        bitpacker.decompress_sorted(
            chunk.initial,
            &self.data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            decompressed,
            chunk_bits as u8,
        );
    }
}

// Help structure to check if set of sorted values are in the compressed posting list
// This help structure reuse the decompressed chunk to avoid unnecessary decompression
pub struct CompressedPostingVisitor<'a> {
    bitpacker: BitPackerImpl,
    postings: &'a CompressedPostingList,

    // Data for the decompressed chunk.
    decompressed_chunk: [PointOffsetType; BitPackerImpl::BLOCK_LEN],

    // Index of the decompressed chunk.
    // It is used to shorten the search range of chunk index for the next value.
    decompressed_chunk_idx: Option<usize>,

    // start index of the decompressed chunk.
    // It is used to shorten the search range in decompressed chunk for the next value.
    decompressed_chunk_start_index: usize,

    // Check if the checked values are in the increasing order.
    #[cfg(test)]
    last_checked: Option<PointOffsetType>,
}

impl<'a> CompressedPostingVisitor<'a> {
    pub fn new(postings: &'a CompressedPostingList) -> CompressedPostingVisitor<'a> {
        CompressedPostingVisitor {
            bitpacker: BitPackerImpl::new(),
            postings,
            decompressed_chunk: [0; BitPackerImpl::BLOCK_LEN],
            decompressed_chunk_idx: None,
            decompressed_chunk_start_index: 0,
            #[cfg(test)]
            last_checked: None,
        }
    }

    pub fn contains(&mut self, val: &PointOffsetType) -> bool {
        #[cfg(test)]
        {
            // check if the checked values are in the increasing order
            if let Some(last_checked) = self.last_checked {
                assert!(*val > last_checked);
            }
            self.last_checked = Some(*val);
        }

        if !self.postings.is_in_postings_range(*val) {
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
                    return self.find_in_decompressed_chunk(val);
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
        let chunk_index = match self.postings.find_chunk(val, self.decompressed_chunk_idx) {
            Some(idx) => idx,
            None => {
                // value is in the noncompressed postings range
                self.decompressed_chunk_idx = None;
                return self
                    .postings
                    .noncompressed_postings
                    .binary_search(val)
                    .is_ok();
            }
        };
        // if the value is the initial value of the chunk, we don't need to decompress the chunk
        if self.postings.chunks[chunk_index].initial == *val {
            return true;
        }

        // second, decompress the chunk and check if the value is in the decompressed chunk
        self.postings
            .decompress_chunk(&self.bitpacker, chunk_index, &mut self.decompressed_chunk);
        self.decompressed_chunk_idx = Some(chunk_index);
        self.decompressed_chunk_start_index = 0;

        // check if the value is in the decompressed chunk
        self.find_in_decompressed_chunk(val)
    }

    fn find_in_decompressed_chunk(&mut self, val: &PointOffsetType) -> bool {
        match self.decompressed_chunk[self.decompressed_chunk_start_index..].binary_search(val) {
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn generate_compressed_posting_list(
        step: PointOffsetType,
    ) -> (CompressedPostingList, HashSet<PointOffsetType>) {
        let mut set = HashSet::new();
        let mut posting_list = PostingList::default();
        for i in 0..999 {
            set.insert(step * i);
            posting_list.insert(step * i);
        }
        let compressed_posting_list = CompressedPostingList::new(posting_list);
        (compressed_posting_list, set)
    }

    #[test]
    fn test_compressed_posting_contains() {
        for step in 0..3 {
            let (compressed_posting_list, set) = generate_compressed_posting_list(step);
            for i in 0..step * 1000 {
                assert_eq!(compressed_posting_list.contains(&i), set.contains(&i));
            }
        }
    }

    #[test]
    fn test_compressed_posting_visitor() {
        for build_step in 0..3 {
            let (compressed_posting_list, set) = generate_compressed_posting_list(build_step);

            for search_step in 1..512 {
                let mut visitor = CompressedPostingVisitor::new(&compressed_posting_list);
                for i in 0..build_step * 1000 {
                    if i % search_step == 0 {
                        assert_eq!(visitor.contains(&i), set.contains(&i));
                    }
                }
            }
        }
    }
}
