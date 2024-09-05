use std::path::PathBuf;
use std::sync::Arc;

use common::types::PointOffsetType;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, FromZeroes};

use crate::index::field_index::full_text_index::compressed_posting::compressed_common::CompressedPostingChunksIndex;
use crate::index::field_index::full_text_index::inverted_index::TokenId;

pub struct CompressedMmapPostingListView<'a> {
    last_doc_id: &'a PointOffsetType,
    chunks: &'a [CompressedPostingChunksIndex],
    data: &'a [u8],
    /// 0-3 extra bytes to align the data
    _alignment: &'a [u8],
    remainder_postings: &'a [PointOffsetType],
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PostingsHeader {
    /// Number of posting lists. One posting list per term
    pub posting_count: usize,
    _reserved: [u8; 32],
}

/// This data structure should contain all the necessary information to
/// construct `CompressedMmapPostingList` from the mmap file.
#[derive(Debug, Default, Clone, FromBytes, FromZeroes)]
#[repr(C)]
struct PostingListHeader {
    /// Offset in bytes from the start of the mmap file
    /// where the posting list data starts
    offset: u64,
    /// Length in bytes for chunks index
    chunks_len: u32,
    /// Length in bytes for the compressed postings data
    data_len: u32,
    /// Length in bytes for the alignment bytes
    alignment_len: u8,
    /// Length in bytes for the remainder postings
    remainder_len: u8,
    _reserved: [u8; 2],
}

/// MmapPostings Structure on disk:
///
///
/// `| PostingsHeader | [ PostingListHeader, PostingListHeader, ... ] | [ CompressedMmapPostingList, CompressedMmapPostingList, ... ] |`
pub struct MmapPostings {
    path: PathBuf,
    mmap: Arc<Mmap>,
    header: PostingsHeader,
}

impl MmapPostings {
    fn get_header(&self, token_id: TokenId) -> Option<&PostingListHeader> {
        if self.header.posting_count <= token_id as usize {
            return None;
        }

        let header_offset =
            size_of::<PostingsHeader>() + token_id as usize * size_of::<PostingListHeader>();
        let header_bytes = self
            .mmap
            .get(header_offset..header_offset + size_of::<PostingListHeader>())?;

        PostingListHeader::ref_from(header_bytes)
    }

    fn read_compressed_mmap_posting_list_view(
        &self,
        header: &PostingListHeader,
    ) -> Option<CompressedMmapPostingListView<'_>> {
        let offset = header.offset as usize;
        let chunks_len = header.chunks_len as usize;
        let data_len = header.data_len as usize;
        let alignment_len = header.alignment_len as usize;
        let remainder_len = header.remainder_len as usize;

        let last_doc_id_bytes = size_of::<PointOffsetType>();
        let chunks_size_bytes = chunks_len * size_of::<CompressedPostingChunksIndex>();
        let data_size_bytes = data_len;
        let alignment_size_bytes = alignment_len;
        let remainder_size_bytes = remainder_len * size_of::<PointOffsetType>();

        let last_doc_id_offset = offset;
        let chunks_offset = last_doc_id_offset + last_doc_id_bytes;
        let data_offset = chunks_offset + chunks_size_bytes;
        let alignment_offset = data_offset + data_size_bytes;
        let remainder_offset = alignment_offset + alignment_size_bytes;

        let last_doc_id_mem = self.mmap.get(last_doc_id_offset..chunks_offset)?;
        let last_doc_id = u32::ref_from(last_doc_id_mem)?;

        let chunks_mem = self.mmap.get(chunks_offset..data_offset)?;
        let chunks = CompressedPostingChunksIndex::slice_from(chunks_mem)?;

        let data = self.mmap.get(data_offset..alignment_offset)?;
        let alignment = self.mmap.get(alignment_offset..remainder_offset)?;

        let remainder_mem = self
            .mmap
            .get(remainder_offset..remainder_offset + remainder_size_bytes)?;
        let remainder_postings = u32::slice_from(remainder_mem)?;

        Some(CompressedMmapPostingListView {
            last_doc_id,
            chunks,
            data,
            _alignment: alignment,
            remainder_postings,
        })
    }

    pub fn get(&self, token_id: TokenId) -> Option<CompressedMmapPostingListView<'_>> {
        let header = self.get_header(token_id)?;
        self.read_compressed_mmap_posting_list_view(header)
    }
}
