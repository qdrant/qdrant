use bitpacking::BitPacker;
use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::compressed_posting::compressed_chunks_reader::ChunkReader;
use crate::index::field_index::full_text_index::compressed_posting::compressed_common::{
    BitPackerImpl, CompressedPostingChunksIndex, compress_posting,
};
#[cfg(feature = "testing")]
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_iterator::CompressedPostingIterator;
#[cfg(feature = "testing")]
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_visitor::CompressedPostingVisitor;

#[derive(Clone, Debug, Default)]
pub struct CompressedPostingList {
    last_doc_id: PointOffsetType,
    data: Vec<u8>,
    chunks: Vec<CompressedPostingChunksIndex>,
    // last postings that are not compressed because they are not aligned with the block size
    remainder_postings: Vec<PointOffsetType>,
}

impl CompressedPostingList {
    pub fn new(posting_list: &[PointOffsetType]) -> Self {
        if posting_list.is_empty() {
            return Self::default();
        }
        let (chunks, remainder_postings, data) = compress_posting(posting_list);

        Self {
            last_doc_id: *posting_list.last().unwrap(),
            data,
            chunks,
            remainder_postings,
        }
    }

    pub fn reader<'a>(&'a self, hw_counter: &'a HardwareCounterCell) -> ChunkReader<'a> {
        ChunkReader::new(
            self.last_doc_id,
            &self.chunks,
            &self.data,
            &self.remainder_postings,
            ConditionedCounter::never(hw_counter),
        )
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * BitPackerImpl::BLOCK_LEN + self.remainder_postings.len()
    }

    #[allow(dead_code)]
    #[cfg(feature = "testing")]
    pub fn iter<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        let reader = self.reader(hw_counter);
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

    pub(crate) fn internal_structs(
        &self,
    ) -> (&[u8], &[CompressedPostingChunksIndex], &[PointOffsetType]) {
        (&self.data, &self.chunks, &self.remainder_postings)
    }

    pub(crate) fn last_doc_id(&self) -> PointOffsetType {
        self.last_doc_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_posting_contains() {
        let hw_counter = HardwareCounterCell::new();
        for step in 0..3 {
            let (compressed_posting_list, set) =
                CompressedPostingList::generate_compressed_posting_list_fixture(step);
            for i in 0..step * 1000 {
                assert_eq!(
                    compressed_posting_list.reader(&hw_counter).contains(i),
                    set.contains(&i)
                );
            }
        }
    }
}
