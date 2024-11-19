use std::io;
use std::io::Write;
use std::path::PathBuf;

use common::types::PointOffsetType;
use common::zeros::WriteZerosExt;
use memmap2::Mmap;
use memory::madvise::{Advice, AdviceSetting};
use memory::mmap_ops::open_read_mmap;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::index::field_index::full_text_index::compressed_posting::compressed_chunks_reader::ChunkReader;
use crate::index::field_index::full_text_index::compressed_posting::compressed_common::CompressedPostingChunksIndex;
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_list::CompressedPostingList;
use crate::index::field_index::full_text_index::inverted_index::TokenId;

const ALIGNMENT: usize = 4;

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
    mmap: Mmap,
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

    /// Create ChunkReader from the given header
    ///
    /// Assume the following layout:
    ///
    /// * `last_doc_id: &'a PointOffsetType,`
    /// * `chunks_index: &'a [CompressedPostingChunksIndex],`
    /// * `data: &'a [u8],`
    /// * `_alignment: &'a [u8], // 0-3 extra bytes to align the data`
    /// * `remainder_postings: &'a [PointOffsetType],`
    /// ```
    fn get_reader(&self, header: &PostingListHeader) -> Option<ChunkReader<'_>> {
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
        let last_doc_id = u32::read_from(last_doc_id_mem)?;

        let chunks_mem = self.mmap.get(chunks_offset..data_offset)?;
        let chunks = CompressedPostingChunksIndex::slice_from(chunks_mem)?;

        let data = self.mmap.get(data_offset..alignment_offset)?;

        let remainder_mem = self
            .mmap
            .get(remainder_offset..remainder_offset + remainder_size_bytes)?;
        let remainder_postings = u32::slice_from(remainder_mem)?;

        Some(ChunkReader::new(
            last_doc_id,
            chunks,
            data,
            remainder_postings,
        ))
    }

    pub fn get(&self, token_id: TokenId) -> Option<ChunkReader<'_>> {
        let header = self.get_header(token_id)?;
        self.get_reader(header)
    }

    /// Given a vector of compressed posting lists, this function writes them to the `path` file.
    /// The format of the file is compatible with the `MmapPostings` structure.
    pub fn create(path: PathBuf, compressed_postings: &[CompressedPostingList]) -> io::Result<()> {
        // Create a new empty file, where we will write the compressed posting lists and the header
        let file = tempfile::Builder::new()
            .prefix(path.file_name().ok_or(io::ErrorKind::InvalidInput)?)
            .tempfile_in(path.parent().ok_or(io::ErrorKind::InvalidInput)?)?;
        let mut bufw = io::BufWriter::new(&file);

        let postings_header = PostingsHeader {
            posting_count: compressed_postings.len(),
            _reserved: [0; 32],
        };

        // Write the header to the buffer
        bufw.write_all(postings_header.as_bytes())?;

        let postings_lists_headers_size =
            compressed_postings.len() * size_of::<PostingListHeader>();
        let mut posting_offset = size_of::<PostingsHeader>() + postings_lists_headers_size;

        for compressed_posting in compressed_postings {
            let (data, chunks, remainder_postings) = compressed_posting.internal_structs();

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

            // Write the posting list header to the buffer
            bufw.write_all(posting_list_header.as_bytes())?;

            posting_offset += posting_list_header.posting_size();
        }

        for compressed_posting in compressed_postings {
            let (data, chunks, remainder_postings) = compressed_posting.internal_structs();

            let last_doc_id = compressed_posting.last_doc_id();

            bufw.write_all(last_doc_id.as_bytes())?;

            for chunk in chunks {
                bufw.write_all(chunk.as_bytes())?;
            }

            bufw.write_all(data)?;

            // Example:
            // For data size = 5, alignment = 3 as (5 + 3 = 8)
            // alignment = 4 - 5 % 4 = 3
            bufw.write_zeros(ALIGNMENT - data.len() % ALIGNMENT)?;

            for posting in remainder_postings {
                bufw.write_all(posting.as_bytes())?;
            }
        }

        // Dropping will flush the buffer to the file
        drop(bufw);

        file.persist(path)?;

        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>, populate: bool) -> io::Result<Self> {
        let path = path.into();
        let mmap = open_read_mmap(&path, AdviceSetting::Advice(Advice::Normal), populate)?;

        let header_bytes = mmap.get(0..size_of::<PostingsHeader>()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid header in {}", path.display()),
            )
        })?;

        let header = PostingsHeader::read_from(header_bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid header deserialization in {}", path.display()),
            )
        })?;

        Ok(Self { path, mmap, header })
    }
}
