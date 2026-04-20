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

impl<'a> RawPostingList<'a> {
    pub fn as_view<V: ZerocopyPostingValue>(&'a self) -> OperationResult<PostingListView<'a, V>> {
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
