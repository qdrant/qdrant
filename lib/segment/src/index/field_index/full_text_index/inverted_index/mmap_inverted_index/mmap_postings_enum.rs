#[cfg(test)]
use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::inverted_index::TokenId;
use crate::index::field_index::full_text_index::inverted_index::mmap_inverted_index::mmap_postings::MmapPostings;
use super::super::positions::Positions;

pub enum MmapPostingsEnum {
    Ids(MmapPostings<()>),
    WithPositions(MmapPostings<Positions>),
}

impl MmapPostingsEnum {
    pub fn populate(&self) {
        match self {
            MmapPostingsEnum::Ids(postings) => postings.populate(),
            MmapPostingsEnum::WithPositions(postings) => postings.populate(),
        }
    }

    pub fn posting_len(&self, token_id: TokenId) -> Option<usize> {
        match self {
            MmapPostingsEnum::Ids(postings) => postings.get(token_id).map(|view| view.len()),
            MmapPostingsEnum::WithPositions(postings) => {
                postings.get(token_id).map(|view| view.len())
            }
        }
    }

    #[cfg(test)]
    pub fn iter_ids<'a>(
        &'a self,
        token_id: TokenId,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match self {
            MmapPostingsEnum::Ids(postings) => postings.get(token_id).map(|view| {
                Box::new(view.into_iter().map(|elem| elem.id))
                    as Box<dyn Iterator<Item = PointOffsetType>>
            }),
            MmapPostingsEnum::WithPositions(postings) => postings.get(token_id).map(|view| {
                Box::new(view.into_iter().map(|elem| elem.id))
                    as Box<dyn Iterator<Item = PointOffsetType>>
            }),
        }
    }
}
