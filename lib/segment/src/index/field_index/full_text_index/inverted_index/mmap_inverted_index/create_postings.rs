use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

use common::zeros::WriteZerosExt;
use fs_err::File;
use posting_list::{PostingList, PostingListComponents};
use zerocopy::IntoBytes;

use crate::index::field_index::full_text_index::inverted_index::mmap_inverted_index::types::{
    ALIGNMENT, PostingListHeader, PostingsHeader, ZerocopyPostingValue,
};

/// Write a vector of compressed posting lists to `path` in the format expected
/// by [`UniversalPostings`](super::uio_postings::UniversalPostings).
///
/// On-disk layout:
///
/// ```text
/// offset 0
/// ┌────────────────────────────────────────────────┐
/// │ PostingsHeader                                 │  size_of::<PostingsHeader>()
/// │   posting_count: usize    (one per term)       │  = 8 + 32 = 40 bytes
/// │   _reserved: [u8; 32]                          │
/// ├────────────────────────────────────────────────┤
/// │ PostingListHeader[0]   ── token_id 0           │  each
/// │ PostingListHeader[1]   ── token_id 1           │  size_of::<PostingListHeader>()
/// │ ...                                            │  = 24 bytes
/// │ PostingListHeader[N-1] ── token_id N-1         │
/// ├────────────────────────────────────────────────┤
/// │ CompressedPostingList[0]                       │  variable
/// │ CompressedPostingList[1]                       │  size; located via
/// │ ...                                            │  header.offset
/// │ CompressedPostingList[N-1]                     │
/// └────────────────────────────────────────────────┘
/// ```
///
/// One `CompressedPostingList`:
///
/// ```text
/// header.offset ─►
/// ┌──────────────────────────────────────────────┐
/// │ last_doc_id : PointOffsetType (u32)          │  4 B
/// ├──────────────────────────────────────────────┤
/// │ chunks: [PostingChunk<Sized<V>>;             │  chunks_count *
/// │          chunks_count]                       │  size_of::<PostingChunk>
/// │   each chunk = (initial_id, sized_value,     │
/// │                 offset into id_data)         │
/// ├──────────────────────────────────────────────┤
/// │ id_data: [u8; ids_data_bytes_count]          │  bit-packed deltas for
/// │                                              │  CHUNK_LEN ids per chunk
/// ├──────────────────────────────────────────────┤
/// │ var_size_data: [u8; var_size_data_bytes]     │  optional (Positions);
/// │                                              │  empty for V = ()
/// ├──────────────────────────────────────────────┤
/// │ alignment: [0u8; alignment_bytes_count]      │  pad so next region is
/// │                                              │  4-byte aligned
/// ├──────────────────────────────────────────────┤
/// │ remainders:[RemainderPosting<Sized<V>>;      │  tail < CHUNK_LEN ids
/// │             remainder_count]                 │  stored uncompressed
/// └──────────────────────────────────────────────┘
/// ```
///
/// The alignment slot exists because `id_data` + `var_size_data` are byte
/// streams of arbitrary length, but `RemainderPosting` is `#[repr(C)]` and
/// read via zerocopy, so its start needs 4-byte alignment.
///
/// Two flavors of `V`:
/// - `V = ()` — ids-only postings; `var_size_data` is empty and
///   `SizedTypeFor<V>` is zero-sized.
/// - `V = Positions` — phrase index; per-doc token positions live in
///   `var_size_data`, with each `PostingChunk` / `RemainderPosting` carrying
///   a `Sized` handle (offset/len) into that blob.
pub fn create_postings_file<V: ZerocopyPostingValue>(
    path: PathBuf,
    compressed_postings: &[PostingList<V>],
) -> io::Result<()> {
    // Create a new empty file, where we will write the compressed posting lists and the header
    let (file, temp_path) = tempfile::Builder::new()
        .prefix(path.file_name().ok_or(io::ErrorKind::InvalidInput)?)
        .tempfile_in(path.parent().ok_or(io::ErrorKind::InvalidInput)?)?
        .into_parts();
    let file = File::from_parts::<&Path>(file, temp_path.as_ref());
    let mut bufw = io::BufWriter::new(&file);

    let postings_header = PostingsHeader {
        posting_count: compressed_postings.len(),
        _reserved: [0; 32],
    };

    bufw.write_all(bytemuck::bytes_of(&postings_header))?;

    let postings_lists_headers_size = compressed_postings.len() * size_of::<PostingListHeader>();
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

        bufw.write_all(posting_list_header.as_bytes())?;

        posting_offset += posting_list_header.posting_size::<V>();
    }

    for compressed_posting in compressed_postings {
        let view = compressed_posting.view();
        let PostingListComponents {
            id_data,
            chunks,
            var_size_data,
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

        bufw.write_all(id_data)?;

        if !var_size_data.is_empty() {
            bufw.write_all(var_size_data)?;
        }

        // alignment padding so the next `RemainderPosting` is 4-byte aligned
        let data_len = id_data.len() + var_size_data.len();
        bufw.write_zeros(data_len.next_multiple_of(ALIGNMENT) - data_len)?;

        for element in remainders {
            bufw.write_all(element.as_bytes())?;
        }
    }

    bufw.flush()?;
    drop(bufw);

    file.sync_all()?;
    drop(file);
    temp_path.persist(path)?;

    Ok(())
}
