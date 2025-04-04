use std::io;
use std::io::Write;
use std::path::PathBuf;

use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::zeros::WriteZerosExt;
use memmap2::Mmap;
use memory::madvise::{Advice, AdviceSetting, Madviseable};
use memory::mmap_ops::open_read_mmap;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::index::field_index::full_text_index::compressed_posting::compressed_chunks_reader::ChunkReader;
use crate::index::field_index::full_text_index::compressed_posting::compressed_common::CompressedPostingChunksIndex;
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_list::CompressedPostingList;
use crate::index::field_index::full_text_index::inverted_index::TokenId;

const ALIGNMENT: usize = 4;

#[derive(Debug, Default, Clone, FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
struct PostingsHeader {
    /// Number of posting lists. One posting list per term
    pub posting_count: usize,
    _reserved: [u8; 32],
}

/// This data structure should contain all the necessary information to
/// construct `CompressedMmapPostingList` from the mmap file.
#[derive(Debug, Default, Clone, FromBytes, Immutable, IntoBytes, KnownLayout)]
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
    _path: PathBuf,
    mmap: Mmap,
    header: PostingsHeader,
    on_disk: bool,
}

impl MmapPostings {
    fn get_header(&self, token_id: TokenId) -> Option<&PostingListHeader> {
        if self.header.posting_count <= token_id as usize {
            return None;
        }

        let header_offset =
            size_of::<PostingsHeader>() + token_id as usize * size_of::<PostingListHeader>();

        PostingListHeader::ref_from_prefix(self.mmap.get(header_offset..)?)
            .ok()
            .map(|(header, _)| header)
    }

    /// Create ChunkReader from the given header
    ///
    /// Assume the following layout:
    ///
    /// ```ignore
    /// last_doc_id: &'a PointOffsetType,
    /// chunks_index: &'a [CompressedPostingChunksIndex],
    /// data: &'a [u8],
    /// _alignment: &'a [u8], // 0-3 extra bytes to align the data
    /// remainder_postings: &'a [PointOffsetType],
    /// ```
    fn get_reader<'a>(
        &'a self,
        header: &PostingListHeader,
        hw_counter: ConditionedCounter<'a>,
    ) -> Option<ChunkReader<'a>> {
        let counter = hw_counter.payload_index_io_read_counter();

        let bytes = self.mmap.get(header.offset as usize..)?;
        counter.incr_delta(size_of::<PointOffsetType>());

        let (last_doc_id, bytes) = PointOffsetType::read_from_prefix(bytes).ok()?;

        counter.incr_delta(size_of::<CompressedPostingChunksIndex>());
        let (chunks, bytes) = <[CompressedPostingChunksIndex]>::ref_from_prefix_with_elems(
            bytes,
            header.chunks_count as usize,
        )
        .ok()?;
        let (data, bytes) = bytes.split_at(header.data_bytes_count as usize);
        let bytes = bytes.get(header.alignment_bytes_count as usize..)?;

        let (remainder_postings, _) =
            <[u32]>::ref_from_prefix_with_elems(bytes, header.remainder_count as usize).ok()?;

        Some(ChunkReader::new(
            last_doc_id,
            chunks,
            data,
            remainder_postings,
            hw_counter,
        ))
    }

    pub fn get<'a>(
        &'a self,
        token_id: TokenId,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<ChunkReader<'a>> {
        let hw_counter = ConditionedCounter::new(self.on_disk, hw_counter);

        hw_counter
            .payload_index_io_read_counter()
            .incr_delta(size_of::<PostingListHeader>());

        let header = self.get_header(token_id)?;

        self.get_reader(header, hw_counter)
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
        file.as_file().sync_all()?;
        file.persist(path)?;

        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>, populate: bool) -> io::Result<Self> {
        let path = path.into();
        let mmap = open_read_mmap(&path, AdviceSetting::Advice(Advice::Normal), populate)?;

        let (header, _) = PostingsHeader::read_from_prefix(&mmap).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid header deserialization in {}", path.display()),
            )
        })?;

        Ok(Self {
            _path: path,
            mmap,
            header,
            on_disk: !populate,
        })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) {
        self.mmap.populate();
    }
}
