use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use common::types::PointOffsetType;
use memmap2::Mmap;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::compressed_posting::compressed_common::CompressedPostingChunksIndex;
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_list::CompressedPostingList;
use crate::index::field_index::full_text_index::inverted_index::TokenId;

const ALIGNMENT: usize = 4;

pub struct CompressedMmapPostingListView<'a> {
    last_doc_id: &'a PointOffsetType,
    chunks_index: &'a [CompressedPostingChunksIndex],
    data: &'a [u8],
    /// 0-3 extra bytes to align the data
    _alignment: &'a [u8],
    remainder_postings: &'a [PointOffsetType],
}

#[derive(Debug, Default, Clone, AsBytes, FromBytes, FromZeroes)]
#[repr(C)]
struct PostingsHeader {
    /// Number of posting lists. One posting list per term
    pub posting_count: usize,
    _reserved: [u8; 32],
}

/// This data structure should contain all the necessary information to
/// construct `CompressedMmapPostingList` from the mmap file.
#[derive(Debug, Default, Clone, AsBytes, FromBytes, FromZeroes)]
#[repr(C)]
struct PostingListHeader {
    /// Offset in bytes from the start of the mmap file
    /// where the posting list data starts
    offset: u64,
    /// Amount of chunks in compressed posting list
    chunks_count: u32,
    /// Length in bytes for the compressed postings data
    data_bytes_count: u32,
    /// Length in bytes for the alignment bytes
    alignment_bytes_count: u8,
    /// Length in bytes for the remainder postings
    remainder_count: u8,
    _reserved: [u8; 6],
}

impl PostingListHeader {
    /// Size of the posting list this header represents
    fn posting_size(&self) -> usize {
        self.data_bytes_count as usize
            + self.alignment_bytes_count as usize
            + self.remainder_count as usize * size_of::<PointOffsetType>()
            + self.chunks_count as usize * size_of::<CompressedPostingChunksIndex>()
            + size_of::<PointOffsetType>() // last_doc_id
    }
}

/// MmapPostings Structure on disk:
///
///
/// `| PostingsHeader |
/// [ PostingListHeader, PostingListHeader, ... ] |
/// [ CompressedMmapPostingList, CompressedMmapPostingList, ... ] |`
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
        let chunks_len = header.chunks_count as usize;
        let data_len = header.data_bytes_count as usize;
        let alignment_len = header.alignment_bytes_count as usize;
        let remainder_len = header.remainder_count as usize;

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
            chunks_index: chunks,
            data,
            _alignment: alignment,
            remainder_postings,
        })
    }

    pub fn get(&self, token_id: TokenId) -> Option<CompressedMmapPostingListView<'_>> {
        let header = self.get_header(token_id)?;
        self.read_compressed_mmap_posting_list_view(header)
    }

    /// Given a vector of compressed posting lists, this function writes them to the `path` file.
    /// The format of the file is compatible with the `MmapPostings` structure.
    pub fn create_from(
        path: PathBuf,
        compressed_postings: &[CompressedPostingList],
    ) -> OperationResult<()> {
        // Create a new empty file, where we will write the compressed posting lists and the header
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let postings_header = PostingsHeader {
            posting_count: compressed_postings.len(),
            _reserved: [0; 32],
        };

        // Write the header to the file
        file.write_all(postings_header.as_bytes())?;

        let postings_lists_headers_size =
            compressed_postings.len() * size_of::<PostingListHeader>();
        let mut posting_offset = size_of::<PostingsHeader>() + postings_lists_headers_size;

        for compressed_posting in compressed_postings {
            if let Some(posting) = compressed_posting {
                let (data, chunks, remainder_postings) = posting.internal_structs();

                let data_len = data.len();
                let alignment_len = ALIGNMENT - data_len % ALIGNMENT;

                let posting_list_header = PostingListHeader {
                    offset: posting_offset as u64,
                    chunks_count: chunks.len() as u32,
                    data_bytes_count: data.len() as u32,
                    alignment_bytes_count: alignment_len as u8,
                    remainder_count: remainder_postings.len() as u8,
                    _reserved: [0; 6],
                };

                // Write the posting list header to the file
                file.write_all(posting_list_header.as_bytes())?;

                posting_offset += posting_list_header.posting_size();
            } else {
                // TODO(luis): Is this the best thing we can do if a posting is None?
                let posting_list_header = PostingListHeader {
                    offset: posting_offset as u64,
                    chunks_count: 0,
                    data_bytes_count: 0,
                    alignment_bytes_count: 0,
                    remainder_count: 0,
                    _reserved: [0; 6],
                };
                file.write_all(posting_list_header.as_bytes())?;
            }
        }

        for compressed_posting in compressed_postings {
            let Some(posting) = compressed_posting else {
                // TODO(luis): Is this the best thing we can do if a posting is None?
                continue;
            };

            let (data, chunks, remainder_postings) = posting.internal_structs();

            let last_doc_id = posting.last_doc_id();

            file.write_all(last_doc_id.as_bytes())?;

            for chunk in chunks {
                file.write_all(chunk.as_bytes())?;
            }

            file.write_all(data)?;

            // Example:
            // For data size = 5, alignment_len = 3 as (5 + 3 = 8)
            // alignment_len = 4 - 5 % 4 = 3
            let alignment_len = ALIGNMENT - data.len() % ALIGNMENT;

            if alignment_len > 0 {
                let alignment = vec![0; alignment_len];
                file.write_all(alignment.as_slice())?;
            }

            for posting in remainder_postings {
                file.write_all(posting.as_bytes())?;
            }
        }

        Ok(())
    }
}
