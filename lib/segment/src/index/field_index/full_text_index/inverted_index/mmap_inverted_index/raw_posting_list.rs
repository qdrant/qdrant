use std::borrow::Cow;

use common::types::PointOffsetType;
use posting_list::{PostingChunk, PostingListView, RemainderPosting, SizedTypeFor};
use zerocopy::FromBytes;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::inverted_index::mmap_inverted_index::types::{
    PostingListHeader, ZerocopyPostingValue,
};

/// Raw byte representation of posting list, which can be converted into [`PostingListView`]
pub struct RawPostingList<'a> {
    bytes: Cow<'a, [u8]>,
    header: PostingListHeader,
}

impl<'a> RawPostingList<'a> {
    pub fn new(bytes: Cow<'a, [u8]>, header: PostingListHeader) -> RawPostingList<'a> {
        RawPostingList { bytes, header }
    }
}

impl RawPostingList<'_> {
    /// Decode the raw bytes into a [`PostingListView`]. The view borrows from
    /// `self` (not from the original `'a`), which allows it to be produced from
    /// contexts that hold `RawPostingList` by value (e.g., inside a `self_cell`
    /// builder where the outer `'a` is not reachable).
    pub fn as_view<V: ZerocopyPostingValue>(&self) -> OperationResult<PostingListView<'_, V>> {
        let (last_doc_id, bytes) = PointOffsetType::read_from_prefix(self.bytes.as_ref())?;

        let (chunks, bytes) = <[PostingChunk<SizedTypeFor<V>>]>::ref_from_prefix_with_elems(
            bytes,
            self.header.chunks_count as usize,
        )?;

        let (id_data, bytes) = bytes.split_at(self.header.ids_data_bytes_count as usize);

        let (var_size_data, bytes) = bytes.split_at(self.header.var_size_data_bytes_count as usize);

        // skip padding
        let bytes = &bytes[self.header.alignment_bytes_count as usize..];

        let (remainder_postings, _) =
            <[RemainderPosting<SizedTypeFor<V>>]>::ref_from_prefix_with_elems(
                bytes,
                self.header.remainder_count as usize,
            )?;

        let posting_list_view = PostingListView::from_components(
            id_data,
            chunks,
            var_size_data,
            remainder_postings,
            Some(last_doc_id),
        );

        Ok(posting_list_view)
    }
}
