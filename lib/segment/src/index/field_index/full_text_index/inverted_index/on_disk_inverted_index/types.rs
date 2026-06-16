use common::types::PointOffsetType;
use posting_list::{
    CHUNK_LEN, PostingChunk, PostingValue, RemainderPosting, SizedTypeFor, ValueHandler,
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::index::field_index::full_text_index::inverted_index::positions::Positions;

pub const ALIGNMENT: usize = 4;

/// A [`PostingValue`] whose sized payload can be zerocopy-read from an mmap.
pub(in crate::index::field_index::full_text_index) trait ZerocopyPostingValue:
    PostingValue<
    Handler: ValueHandler<Sized: FromBytes + IntoBytes + Immutable + KnownLayout + Unaligned>,
>
{
}

impl ZerocopyPostingValue for () {}

impl ZerocopyPostingValue for Positions {}

#[derive(Debug, Default, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub(in crate::index::field_index::full_text_index) struct PostingsHeader {
    /// Number of posting lists. One posting list per term
    pub posting_count: usize,
    pub _reserved: [u8; 32],
}

/// This data structure should contain all the necessary information to
/// construct `PostingListView<V>` from the mmap file.
#[derive(Debug, Default, Clone, FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
pub(in crate::index::field_index::full_text_index) struct PostingListHeader {
    /// Offset in bytes from the start of the mmap file
    /// where the posting list data starts
    pub offset: u64,
    /// Amount of chunks in compressed posting list
    pub chunks_count: u32,
    /// Length in bytes for the compressed postings data
    pub ids_data_bytes_count: u32,
    /// Length in bytes for the alignment bytes
    pub alignment_bytes_count: u8,
    /// Number of `RemainderPosting` elements (tail that doesn't fill a chunk)
    pub remainder_count: u8,

    pub _reserved: [u8; 2],

    /// Length in bytes for the var-sized data. Add-on for phrase matching, otherwise 0
    pub var_size_data_bytes_count: u32,
}

impl PostingListHeader {
    pub fn posting_size<V: PostingValue>(&self) -> usize {
        self.ids_data_bytes_count as usize
            + self.var_size_data_bytes_count as usize
            + self.alignment_bytes_count as usize
            + self.remainder_count as usize * size_of::<RemainderPosting<SizedTypeFor<V>>>()
            + self.chunks_count as usize * size_of::<PostingChunk<SizedTypeFor<V>>>()
            + size_of::<PointOffsetType>() // last_doc_id
    }

    /// Number of elements in the posting list. Matches `PostingListView::len`.
    pub fn posting_len(&self) -> usize {
        self.chunks_count as usize * CHUNK_LEN + self.remainder_count as usize
    }
}
