use std::fmt::Debug;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::path::PathBuf;

use common::types::PointOffsetType;
use common::zeros::WriteZerosExt;
use memmap2::Mmap;
use memory::madvise::{Advice, AdviceSetting, Madviseable};
use memory::mmap_ops::open_read_mmap;
use posting_list::{
    PostingChunk, PostingList, PostingListComponents, PostingListView, PostingValue,
    RemainderPosting, SizedTypeFor, ValueHandler,
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::index::field_index::full_text_index::inverted_index::TokenId;
use crate::index::field_index::full_text_index::inverted_index::positions::Positions;

const ALIGNMENT: usize = 4;

/// Trait marker to enrich [`posting_list::PostingValue`] for handling mmap files with the posting list.
pub(in crate::index::field_index::full_text_index) trait MmapPostingValue:
    PostingValue<
    Handler: ValueHandler<Sized: FromBytes + Immutable + IntoBytes + KnownLayout + Unaligned>
                 + Clone
                 + Debug,
>
{
}

impl MmapPostingValue for () {}

impl MmapPostingValue for Positions {}

#[derive(Debug, Default, Clone, FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
struct PostingsHeader {
    /// Number of posting lists. One posting list per term
    pub posting_count: usize,
    _reserved: [u8; 32],
}

/// This data structure should contain all the necessary information to
/// construct `PostingListView<V>` from the mmap file.
#[derive(Debug, Default, Clone, FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
pub(in crate::index::field_index::full_text_index) struct PostingListHeader {
    /// Offset in bytes from the start of the mmap file
    /// where the posting list data starts
    offset: u64,
    /// Amount of chunks in compressed posting list
    chunks_count: u32,
    /// Length in bytes for the compressed postings data
    ids_data_bytes_count: u32,
    /// Length in bytes for the alignment bytes
    alignment_bytes_count: u8,
    /// Length in bytes for the remainder postings
    remainder_count: u8,

    _reserved: [u8; 2],

    /// Length in bytes for the var-sized data. Add-on for phrase matching, otherwise 0
    var_size_data_bytes_count: u32,
}

impl PostingListHeader {
    fn posting_size<V: PostingValue>(&self) -> usize {
        self.ids_data_bytes_count as usize
            + self.var_size_data_bytes_count as usize
            + self.alignment_bytes_count as usize
            + self.remainder_count as usize * size_of::<RemainderPosting<SizedTypeFor<V>>>()
            + self.chunks_count as usize * size_of::<PostingChunk<SizedTypeFor<V>>>()
            + size_of::<PointOffsetType>() // last_doc_id
    }
}

/// MmapPostings Structure on disk:
///
///
/// `| PostingsHeader |
/// [ PostingListHeader, PostingListHeader, ... ] |
/// [ CompressedMmapPostingList, CompressedMmapPostingList, ... ] |`
pub struct MmapPostings<V: MmapPostingValue> {
    _path: PathBuf,
    mmap: Mmap,
    header: PostingsHeader,
    _value_type: PhantomData<V>,
}

impl<V: MmapPostingValue> MmapPostings<V> {
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

    /// Create PostingListView<V> from the given header
    ///
    /// Assume the following layout:
    ///
    /// ```ignore
    /// last_doc_id: &'a PointOffsetType,
    /// chunks_index: &'a [PostingChunk<()>],
    /// data: &'a [u8],
    /// var_size_data: &'a [u8], // might be empty in case of only ids
    /// _alignment: &'a [u8], // 0-3 extra bytes to align the data
    /// remainder_postings: &'a [PointOffsetType],
    /// ```
    fn get_view<'a>(&'a self, header: &'a PostingListHeader) -> Option<PostingListView<'a, V>> {
        let bytes = self.mmap.get(header.offset as usize..)?;

        let (last_doc_id, bytes) = PointOffsetType::read_from_prefix(bytes).ok()?;

        let (chunks, bytes) = <[PostingChunk<SizedTypeFor<V>>]>::ref_from_prefix_with_elems(
            bytes,
            header.chunks_count as usize,
        )
        .ok()?;

        let (id_data, bytes) = bytes.split_at(header.ids_data_bytes_count as usize);

        let (var_size_data, bytes) = bytes.split_at(header.var_size_data_bytes_count as usize);

        // skip padding
        let bytes = bytes.get(header.alignment_bytes_count as usize..)?;

        let (remainder_postings, _) =
            <[RemainderPosting<SizedTypeFor<V>>]>::ref_from_prefix_with_elems(
                bytes,
                header.remainder_count as usize,
            )
            .ok()?;

        Some(PostingListView::from_components(
            id_data,
            chunks,
            var_size_data,
            remainder_postings,
            Some(last_doc_id),
        ))
    }

    pub fn get<'a>(&'a self, token_id: TokenId) -> Option<PostingListView<'a, V>> {
        let header = self.get_header(token_id)?;
        self.get_view(header)
    }

    /// Given a vector of compressed posting lists, this function writes them to the `path` file.
    /// The format of the file is compatible with the `MmapPostings` structure.
    pub fn create(path: PathBuf, compressed_postings: &[PostingList<V>]) -> io::Result<()> {
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
            let view = compressed_posting.view();
            let PostingListComponents {
                id_data,
                chunks,
                var_size_data,
                remainders,
                last_id: _, // not used for the header
            } = view.components();

            let id_data_len = id_data.len();
            let var_size_data_len = var_size_data.len();
            let data_len = id_data_len + var_size_data_len;
            let alignment_len = data_len.next_multiple_of(ALIGNMENT) - data_len;

            let posting_list_header = PostingListHeader {
                offset: posting_offset as u64,
                chunks_count: chunks.len() as u32,
                ids_data_bytes_count: id_data_len as u32,
                var_size_data_bytes_count: var_size_data_len as u32,
                alignment_bytes_count: alignment_len as u8,
                remainder_count: remainders.len() as u8,
                _reserved: [0; 2],
            };

            // Write the posting list header to the buffer
            bufw.write_all(posting_list_header.as_bytes())?;

            posting_offset += posting_list_header.posting_size::<V>();
        }

        for compressed_posting in compressed_postings {
            let view = compressed_posting.view();
            let PostingListComponents {
                id_data,
                chunks,
                var_size_data, // not used with just ids postings
                remainders,
                last_id,
            } = view.components();

            bufw.write_all(
                last_id
                    .expect("posting must have at least one element")
                    .as_bytes(),
            )?;

            for chunk in chunks {
                bufw.write_all(chunk.as_bytes())?;
            }

            // write all unaligned data together
            bufw.write_all(id_data)?;

            // write var_size_data if it exists
            if !var_size_data.is_empty() {
                bufw.write_all(var_size_data)?;
            }

            // write alignment padding
            // Example:
            // For data size = 5, alignment = 3 as (5 + 3 = 8)
            // alignment = 8 - 5 = 3
            let data_len = id_data.len() + var_size_data.len();
            bufw.write_zeros(data_len.next_multiple_of(ALIGNMENT) - data_len)?;

            for element in remainders {
                bufw.write_all(element.as_bytes())?;
            }
        }

        // Explicitly flush write buffer so we can catch IO errors
        bufw.flush()?;
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
            _value_type: PhantomData,
        })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) {
        self.mmap.populate();
    }

    /// Iterate over posting lists, returning a view for each
    pub fn iter_postings<'a>(&'a self) -> impl Iterator<Item = PostingListView<'a, V>> {
        (0..self.header.posting_count as u32)
            // we are iterating over existing posting lists, all of them should return `Some`
            .filter_map(|posting_idx| self.get(posting_idx))
    }
}
