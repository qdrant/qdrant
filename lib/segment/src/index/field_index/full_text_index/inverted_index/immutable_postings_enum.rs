#[cfg(test)]
use common::types::PointOffsetType;
use posting_list::PostingList;

use super::positions::Positions;
use crate::index::field_index::full_text_index::inverted_index::TokenId;

#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
pub enum ImmutablePostings {
    Ids(Vec<PostingList<()>>),
    WithPositions(Vec<PostingList<Positions>>),
}

impl ImmutablePostings {
    pub fn len(&self) -> usize {
        match self {
            ImmutablePostings::Ids(lists) => lists.len(),
            ImmutablePostings::WithPositions(lists) => lists.len(),
        }
    }

    pub fn posting_len(&self, token: TokenId) -> Option<usize> {
        match self {
            ImmutablePostings::Ids(postings) => {
                postings.get(token as usize).map(|posting| posting.len())
            }
            ImmutablePostings::WithPositions(postings) => {
                postings.get(token as usize).map(|posting| posting.len())
            }
        }
    }

    /// Approximate RAM usage in bytes.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            ImmutablePostings::Ids(lists) => {
                lists.capacity() * std::mem::size_of::<PostingList<()>>()
                    + lists.iter().map(|p| p.heap_bytes()).sum::<usize>()
            }
            ImmutablePostings::WithPositions(lists) => {
                lists.capacity() * std::mem::size_of::<PostingList<Positions>>()
                    + lists.iter().map(|p| p.heap_bytes()).sum::<usize>()
            }
        }
    }

    #[cfg(test)]
    pub fn iter_ids(
        &self,
        token_id: TokenId,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        match self {
            ImmutablePostings::Ids(postings) => postings.get(token_id as usize).map(|posting| {
                Box::new(posting.iter().map(|elem| elem.id))
                    as Box<dyn Iterator<Item = PointOffsetType>>
            }),
            ImmutablePostings::WithPositions(postings) => {
                postings.get(token_id as usize).map(|posting| {
                    Box::new(posting.iter().map(|elem| elem.id))
                        as Box<dyn Iterator<Item = PointOffsetType>>
                })
            }
        }
    }
}
