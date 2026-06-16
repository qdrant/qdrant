#[cfg(test)]
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::super::positions::Positions;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::inverted_index::TokenId;
use crate::index::field_index::full_text_index::inverted_index::on_disk_inverted_index::on_disk_postings::OnDiskPostings;

pub enum OnDiskPostingsEnum<S: UniversalRead> {
    Ids(OnDiskPostings<(), S>),
    WithPositions(OnDiskPostings<Positions, S>),
}

impl<S: UniversalRead> OnDiskPostingsEnum<S> {
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            OnDiskPostingsEnum::Ids(postings) => postings.populate(),
            OnDiskPostingsEnum::WithPositions(postings) => postings.populate(),
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            OnDiskPostingsEnum::Ids(postings) => postings.clear_cache(),
            OnDiskPostingsEnum::WithPositions(postings) => postings.clear_cache(),
        }
    }

    pub fn posting_len(&self, token_id: TokenId) -> OperationResult<Option<usize>> {
        match self {
            OnDiskPostingsEnum::Ids(postings) => postings.posting_len(token_id),
            OnDiskPostingsEnum::WithPositions(postings) => postings.posting_len(token_id),
        }
    }

    #[cfg(test)]
    pub fn iter_ids<'a>(
        &'a self,
        token_id: TokenId,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        // Collect ids upfront so the borrowed `RawPostingList` bytes don't have
        // to outlive this call. Acceptable because UniversalPostings is on disk.
        let ids: Vec<PointOffsetType> = match self {
            OnDiskPostingsEnum::Ids(postings) => {
                let raw = postings.get(token_id).unwrap()?;
                let view = raw.as_view::<()>().unwrap();
                view.into_iter().map(|elem| elem.id).collect()
            }
            OnDiskPostingsEnum::WithPositions(postings) => {
                let raw = postings.get(token_id).unwrap()?;
                let view = raw.as_view::<Positions>().unwrap();
                view.into_iter().map(|elem| elem.id).collect()
            }
        };
        Some(Box::new(ids.into_iter()))
    }
}
